# ADCAP → Impulse POC — Project Context & Data Model (Source of Truth)

**Last updated:** 2026-10-02
**Distilled from:** six meeting notes (2026-08-28 → 2026-09-25) in `../meeting notes/`.
**Read this first.** This doc **+ [`DATA_AND_BUILD_FINDINGS.md`](./DATA_AND_BUILD_FINDINGS.md)** (verified read-only from GM's workspace) are the source of truth; where they and `ADCAP_POC_PROPOSAL.md` / `GM_TO_DATABRICKS_REQUIREMENTS.md` disagree, the context/findings docs win (§10 "Supersedes").

**A working Impulse build already exists** in `design_development_test.silver_plus_adcap_impulse_poc` (bronze→Silver Plus→gold on the 10-file set; Phase 1 effectively done). Details, modeling choices, and the 3 remaining gaps: `DATA_AND_BUILD_FINDINGS.md` §4.

---

## 1. What this is
GM's VSSM **Calibration** team (Engine & Transmission Controls, pre-production development) wants to move their time-series calibration analytics off on-prem/local tooling (ADCAP + SPOT + INCA) into Databricks, using the **Impulse** framework. ~300 calibrators; data from instrumented development vehicles. Target: a POC that proves the approach, scaling to a pilot by year end. **Overnight latency** is acceptable (upload during the day → results next morning); not real-time.

## 2. The decided architecture — Option 1: narrow "Silver Plus" from Bronze
After evaluating two options, the team **decided**:

- **Build a new narrow, EAV-style "Silver Plus" model directly from Bronze** via a minimal ETL. Leave Bronze ingestion **and** the legacy wide Silver table **untouched** (the wide table is an unused one-off; no known consumers).
- **Custom query solver (Stellantis-style) is held in RESERVE** — the fallback/counter-argument if GM pushes back on restructuring, and possibly run in parallel for a performance comparison. It is **not** the opening approach.
  > ⚠️ This reverses the earlier proposal, which framed the custom solver as "preferred." The decision is now **ETL into the native narrow model**, custom solver in reserve.

**Why Option 1 won:** wide tables (5,000+ cols, mostly null, poorly clustered) lose Spark dynamic file pruning/data skipping; Impulse + **Genie agents work well on the native narrow model and poorly against thousands of columns** (the deciding point); schema drift makes columns proliferate (Thomas cited Cummins hitting 40,000 columns); deleting a column means rewriting the whole table.

**Key enabler:** on a final check, **GM's bronze is already close to long format** — `channel name, timestamp, value` with **values & timestamps stored as arrays** — so the ETL is essentially an **explode** + derive the metric/tag tables. Thomas Bonford estimated **~2 days** to build the silver layer and get Impulse running.

## 3. Data hierarchy & bronze format (what we build from)

**Nesting: Program → Test (= Container) → Channel → individual timestamped samples.**

- **Program** — the **broadest** level: a **vehicle + engine combination** ("truck and engine") — the overall engineering project / vehicle-engine line the calibration work belongs to. A program contains **many tests**. Example program names: **"ADCAP"** and **"ADAS Drive."** *(Terminology note: "ADCAP" is overloaded — it is also the name of GM's legacy Time Series Analyzer tool referenced throughout these docs.)*
- **Test = Container** — **one MF4 file** (identified by file name); one discrete run of the engine/vehicle on a test bench or in the field (e.g. one dyno session or one drive cycle). This is what **`container_id`** refers to and what Impulse queries events/stats against. A single test records **5,000–10,000+ channels across ~20 calibration domains**, all sharing the same time range.
- **Channel** — one sensor/signal within a test (e.g. coolant temp, RPM, engine load, valve-timing), its own `(timestamp, value)` time series. The narrow/EAV grain is `container_id + channel_id + timestamp + value`. (Brien's example: one channel in one test = **503 samples**.)
- Bronze stores **timestamps and values as embedded arrays per channel/row** → must be **exploded** to one-row-per-sample before the silver transform.
- The **`group` field = sample-rate raster — CONFIRMED from workspace data** (`DATA_AND_BUILD_FINDINGS.md` §2): per-group rates cluster on clean values (1–1000 Hz), all channels in a group share one time axis. **Multiple groups can share a nominal rate**, and the **same channel does appear in >1 group** ⇒ **channel identity = `(channelName, group)`** (as the existing build does).
- **Values are not all numeric:** some signals are **enum/text with a numeric code**, and GM wants the **actual string preserved** (no lossy double conversion). **Recommended pattern (per the Impulse EU team — see [`IMPULSE_SCALING_AND_DESIGN.md`](./IMPULSE_SCALING_AND_DESIGN.md) §6):** not a single string column, but **multiple typed value columns on `channels`** — `raw_value` (int), `scaled_value` (double), `string_value` (string) — plus a **`value_map`** selector saying which column holds each datapoint (`value_map` also on `channel_metrics` to set a channel's value type). **Verified in data** (`DATA_AND_BUILD_FINDINGS.md` §2): **93% of channels are numeric** (number-as-string → double), **7% are enum** (string label + `enumMap`→code), **0 free-form text**. The existing build currently types `value` as `double` and **drops enum labels** — decision open (§11), though likely a non-issue for the boost/EGR POC tests (numeric signals).
- Files: compressed MF4, ~hundreds of MB to multiple GB; durations 10–30 s (test-track maneuvers) up to 4 h (road trips); never merged — each session is its own file.
- A **"common experiment"** core signal set is in every file (valuable for fleet-level analysis even when the recording calibrator doesn't care about those signals).

## 4. The Impulse silver model to build (Impulse's input contract)
Impulse needs three core tables + optional supporting tables (all derivable from the long bronze):

- **`channels`** (fact / EAV) — `container_id, channel_id, timestamp, value`. One row per measurement → very long/narrow. (See §3 on `value` typing as string.)
- **`container_metrics`** — one row per container (file-level metadata: project/engine, #channels, start/end timestamps; can carry software/build versions).
- **`channel_metrics`** — one row per `(container, channel)` summary stats (min/max/count/duration). **Purpose = query performance:** lets Impulse skip containers that can't satisfy a query (dynamic pruning) — especially valuable for sparse boolean/flag signals (e.g. skip files where max < threshold).
- **`container_tags` / `channel_tags`** — EAV key-value lookups describing what a container/channel is (dimension tables).
- **`file_status`** — needed for **incremental processing** (detect files added since the last Impulse report run) and filtering.
- **Optional:** unit conversion (per-channel, e.g. F→C) and channel aliasing (merge differently-named columns for the same sensor, e.g. `valve_1`/`valve_2`) — maps to the SPOT **Aliases** sheet. Finalize once we see real value against GM data.

**Standardized timestamp field:** **resolved** — bronze `timeStamps` are **epoch seconds (double)**, shared across all channels in a group (`DATA_AND_BUILD_FINDINGS.md` §2). The existing build stores them as `bigint` `tstart`/`tend` (RLE).

## 5. Gold layer & where derived metrics live
- **Gold = Impulse's output** (aggregated fact/dimension tables; Impulse's configurable prefix defaults to `gold`).
- **Placement rule:** metrics extracted from raw values with minimal modification → **silver**; truly aggregated/derived analytical results → **gold** via Impulse's query layer (interval-based queries, e.g. "RPM in high-load range"). Final placement of specific GM metrics (e.g. **cold-start** flag — engine from ambient/cold temp, emissions-relevant; diagnostic-ran flags) worked through with Thomas Bonford.

## 6. Governance — GM medallion naming (Glenn)
- **Bronze** = read-only unmodified source (locked). **Staging** = in-process transform tables. **Silver** = cleaned consumable output. **Gold** = aggregates on silver.
- What we build (container/channel tags + metrics) is **silver**; Impulse output is **gold**. The model doesn't conflict with GM standards — just apply the correct schema/naming when deployed to GM's shared environment. Standards live on GM Confluence/"Geek Docs"; access via ServiceNow (Steve helping).
- **Decision-ownership:** per John Leach guidance, individual teams shouldn't build their own silver independently — central **CDP/platform ("Carrie's team")** should review/own. Looped in for visibility; invited to recurring Friday syncs. Final sign-off authority still open.

## 7. POC scope (decided)
- **Program: `DK68`** — the **one** vehicle program currently loaded in Databricks (**~6–8 more programs to be onboarded** later, likely MY27-forward).
- **Client-selected 5 files** = **`container_id 6–10`** in the existing build (`DATA_AND_BUILD_FINDINGS.md` §3): odo 39552/39575/39591/39610/39643 (vehicle KDUWT1217), each 5,441 channels × 48 groups. ⚠️ all share one software build (`E42Aa262718250gb_quasi`) — no build-to-build trend within this set unless more builds are added.
- **Take indicative containers with ALL their channels** — not a channel subset — plus the **known-stable tests** that run against them (the GM **boost + EGR** test suite = 7 tests; `DATA_AND_BUILD_FINDINGS.md` §5).
- Build in a **dedicated test catalog/schema** (not production). Test workspace has read access to prod; clone catalog-to-catalog for write. **Impulse runs on serverless — no special networking.**
- Rationale: same code, less data; prove the approach before a full historical ETL. More programs (~6–8 additional) onboard later, likely MY27-forward rather than backfilling all history.

## 8. Division of labor & people
- **Daria + Brian** own **Bronze → Silver** data engineering (adapting Brian's prototype against a real subset; building the array-explode).
- **Thomas Bonford** (Impulse creator, European FDE; GM ID approved) owns **Silver → Gold**: translating GM's existing **Python test scripts** into Impulse query/aggregation methodology. ~1–2 days, side-project basis, paid, no hard guarantees.
- GM: **Tom** (domain lead), **Alex**; data engineering **Rohit, Maduri/Majuri, Bala, Sheila** (stretched thin, supportive, pulled in strategically); **Glenn** (medallion standards), **Steve** (access). Databricks: **Aaron** (leadership), **Eric** (earlier liaison).
- Precedent: Brian built an equivalent at **Cummins**; a synthetic prototype (wide 2,511 cols × ~839k rows → ~232–432M narrow rows) → silver → gold already demoed.

## 9. Data volume
- **~61 TB = the compressed MF4 binary size** in Azure Blob; the **actual uncompressed size is much larger.** The **Databricks bronze table ≈ 41.5 TB** represents **one program** (currently the only one loaded). With **~6–8 more programs** to onboard, the full footprint will be well above 61 TB.

## 10. Supersedes / reconcile
Reconciliation status:
- `ADCAP_POC_PROPOSAL.md` — ✅ **reconciled to Option 1 and merged with the former `ADCAP_POC_REQUIREMENTS.md`** (now retired) on 2026-10-01.
- `GM_TO_DATABRICKS_REQUIREMENTS.md` — ⚠️ **still to reconcile:** flip the "preferred = bronze-direct" framing to **Option 1** (narrow Silver Plus from bronze; custom solver reserve); change `channels.value` from a single `double` to the **multi-column `raw_value`/`scaled_value`/`string_value` + `value_map`** pattern (see [`IMPULSE_SCALING_AND_DESIGN.md`](./IMPULSE_SCALING_AND_DESIGN.md) §6); add `file_status`.

## 11. Open questions
- Final sign-off authority for the Bronze → Silver Plus change (Tom vs. central CDP).
- Will data engineering permit **reading from bronze** for the POC (the main political dependency)?
- **Enum value typing** — the existing build types `value` as `double` and **drops enum labels**; decide whether to preserve them (`DATA_AND_BUILD_FINDINGS.md` §4/§6). Likely non-blocking for the boost/EGR POC tests (numeric signals).
- **Port the real work** — replace the demo gold (generic events/hists/calc-channels) with the **7 boost/EGR tests + SPOT Template-Plots rows**, scoped to `container_id 6–10`.
- **SPOT 2D-hist mean-of-Z** extension (`ADCAP_POC_PROPOSAL.md` §4.4); **program-level grouping** as a container tag; final **silver-vs-gold placement** of derived flags.
- **Make the GM tests runnable** — obtain the `tests/common/` package (`dtc_validation.py`, the common `*_validation` modules).
- Rollout plan for the ~6–8 additional programs; on-prem→Azure storage migration timeline.
- *Resolved by findings:* timestamp = **epoch-seconds doubles**; value kinds (**93% numeric / 7% enum / 0 text**); metadata lives in **`file_status`**; **group = sample-rate**; multi-group same-signal is **real** (handled via `(channel, group)` identity); volume (41.5 TB external Parquet vs. 13.7 GB 10-file Delta subset; 61 TB = compressed binary).

## 12. Adjacent opportunities (out of POC scope)
Co-simulation (software root-cause); EV team (Crispin, John Mark); engine test bench/dyno (AVL — involved in Impulse's origin); a "virtual" 1D modeling team; longer-term anomaly-pattern mining. Brian also sees broader GM time-series potential (plant/manufacturing, GM Financial, Motorsports ~10 ms Class A).
