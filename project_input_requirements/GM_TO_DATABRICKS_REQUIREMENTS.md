# GM → Databricks — What GM Must Provide to Start the Impulse POC

**Status:** Draft for review
**Last updated:** 2026-09-03
**Companion docs:** [`ADCAP_POC_REQUIREMENTS.md`](./ADCAP_POC_REQUIREMENTS.md) · [`ADCAP_POC_PROPOSAL.md`](./ADCAP_POC_PROPOSAL.md)
**Purpose:** the concrete access, data, and format GM needs to hand the Databricks team so the POC can begin — organized to **comply with the Impulse input framework** (its silver-layer input contract).

> Impulse does **not** ingest data. MF4 → bronze ingestion stays with GM data engineering. What Impulse needs is a **silver layer** in the shape below (or a mappable layout). Producing that silver layer — from the already-ingested bronze — is the first joint task.

---

## 1. Access & Permissions (blocking — needed day 1)

### 1.1 GitHub
- [ ] Add Daria as a **contributor** to the private test-scripts repo **`GM-SDV/etc-time-series-tests`** (Tom to share access/links). This is the source of the Python "tests" we reproduce as Impulse events.

### 1.2 Databricks workspace & Unity Catalog
- [ ] **Workspace access** for the Databricks team on the target workspace.
- [ ] **Unity Catalog grants** on the POC catalog/schema(s):
  - `USE CATALOG` + `USE SCHEMA` on the schema holding the ingested bronze/silver data.
  - `SELECT` on the bronze/silver tables for the POC program.
  - `CREATE TABLE` (or `ALL PRIVILEGES`) on a **gold** schema Impulse can write its star schema into (or a dedicated `poc_gold` schema).
  - `CREATE SCHEMA` if we need to stand up a working schema for silver-shaping.
- [ ] Identify a **catalog/schema owner** on the GM side who can grant the above directly (faster than a help-desk request). *Daria to Slack Tom the exact permission list.*

### 1.3 Compute
- [ ] **Serverless Environment Version 2+** available (or a DBR ML cluster) — Impulse requires **Python 3.12, PySpark 4.0, Delta Lake 4.0**. (Env Version 1 ships Python 3.10 and Impulse fails to import.)
- [ ] Ability to `%pip install databricks-impulse[local-dev]` **or** clone this repo into a Databricks Git folder.

### 1.4 Data location
- [ ] The **Unity Catalog location** (`catalog.schema`) of the already-ingested bronze data for the POC program, and read access to it.
- [ ] A **Volume** or path for any intermediate/CSV loads if needed.

---

## 2. Scope & Program Selection

- [ ] **Pick one vehicle program** to start (candidate: a program already fully landed in bronze — e.g. an `LS6` / `T1XX_HDPU_DK68`-class project). Confirm which project is already fully in bronze.
- [ ] Provide the **project/program identifier** as it appears in the data (used for `project_id` scoping / container filtering).
- [ ] Confirm expected **volume** for the POC slice (recordings, channels, rough size).

---

## 3. The Impulse Silver Input Contract (the "table format")

Impulse's `DefaultSolver` reads **only three required tables** plus optional add-ons:

- **`channels`** — the **large** time-series sample data (the only big table).
- **`container_metrics`** — small metadata table, one row per container (a container = one test drive / bench run).
- **`channel_metrics`** — small metadata table, one row per channel (a channel = a sensor signal, e.g. Engine Speed).

The two `*_metrics` tables are tiny metadata; effectively all volume lives in `channels`. GM (with Databricks) produces these from bronze. Column **types and key columns below are fixed by the engine's internal names**; other metric columns are flexible. Where GM's existing columns differ in name, we remap them via `solver_config.column_name_mapping` rather than renaming source data (see §5). **Note:** we may not have to build these tables at all — see §5 on connecting Impulse directly to GM's bronze via a custom solver.

> **`container_id` type:** the reference schema uses `long`, but per the [Impulse ingestion docs](https://databrickslabs.github.io/impulse/docs/data_model/ingestion/) the engine accepts `long`, `int`, **or `string`** — as long as the type is **consistent across every table** (it is the PK on `container_metrics` and the FK everywhere else). `(container_id, channel_id)` identifies a channel and `channel_id` is **local to its container**.

### 3.1 `container_metrics` — **required** (one row per recording / trip)

| Column | Type | Required | Notes |
|---|---|---|---|
| `container_id` | **long** | **Yes** | **Primary key.** Stable id per recording; foreign key everywhere else. |
| `start_dt` | timestamp | Yes* | Recording start. |
| `stop_dt` | timestamp | Yes* | Recording end. |
| `duration_ms` | int | Yes* | Recording duration (ms). |
| `num_channels` | int | Yes* | Channel count. |

\* Beyond `container_id`, the exact metric columns are **not hard-fixed** — but the reference schema above is what the standard pipeline and gold `measurement_dimension` expect. Add any container-level columns you want surfaced (e.g. `vehicle_key`). **A `timestamp`/last-modified column is needed if we enable incremental processing** (see §6).

### 3.2 `channel_metrics` — **required** (one row per `(container_id, channel_id)`)

| Column | Type | Required | Notes |
|---|---|---|---|
| `container_id` | **long** | **Yes** | FK to `container_metrics`. |
| `channel_id` | **int** | **Yes** | Channel id, **local to its container**. |
| `value_type` | string | Yes | e.g. `"double"`. |
| `sample_count` | int | | |
| `nan_ratio` | float | | |
| `begin_s`, `end_s` | float | | Channel time span (seconds). |
| `duration_ms` | int | | |
| `original_sample_count` | int | | Pre-encoding sample count (preserves original timing info). |
| `original_sr` | float | | Original sample rate (Hz) — **important for ECUs at different loop rates**. |
| `min`, `max`, `mean`, `std` | float | | Per-channel stats. |
| `pz1`, `pz10`, `pz90`, `pz99` | float | | Percentiles. |

> **Channel selection:** if `channel_tags` (§3.5) is **not** provided, channels are selected by **columns on `channel_metrics`** (wide model) — so a `channel_name` column here is what `query.channel(channel_name="VeEPSI_n_LoresI")` matches against. See §3.5 for the tag-based alternative (recommended for ADCAP's signal naming).

### 3.3 `channels` — **required** (the time-series samples)

Two supported formats — pick one and set `query_engine.data_type` to match:

**RLE (recommended — one row per stable interval; far smaller/faster):**

| Column | Type | Required | Notes |
|---|---|---|---|
| `container_id` | **long** | **Yes** | |
| `channel_id` | **int** | **Yes** | |
| `tstart` | **long** | **Yes** | Interval start (integer time base). |
| `tend` | **long** | **Yes** | Interval end. |
| `value` | double | Yes | Constant value over `[tstart, tend)`. |

**RAW (one row per sample):**

| Column | Type | Required | Notes |
|---|---|---|---|
| `container_id` | **long** | **Yes** | |
| `channel_id` | **int** | **Yes** | |
| `timestamp` | **long** | **Yes** | Per-sample timestamp (integer time base). |
| `value` | double | Yes | |
| `is_plausible` | boolean | No | Optional; enables `drop_implausible_data` (RAW only). |

> **RAW encoder:** in RAW mode the engine converts point samples to intervals via `query_engine.raw_encoder` — `"RLE"` (default, collapses equal-valued runs) or `"INTERVAL"` (keeps every sample, only derives `tend` and drops exact duplicates). Choose `"INTERVAL"` when exact per-sample timing must be preserved for (co-)simulation.
> **MF4 invalidation bits → `is_plausible`:** the Impulse ingestion docs call out that MDF4/ASAM per-sample **invalidation bits** must be honored — drop or mark invalid samples **before** RLE encoding, or carry them into the `is_plausible` column (RAW) so `drop_implausible_data=True` can exclude them. GM/data-engineering should confirm how invalidation bits are represented in the current bronze so we preserve this.

> **Time base:** `tstart`/`tend`/`timestamp` are integer time. Use **one consistent base across all tables**; **epoch microseconds** is recommended (it matches the POI schema). This is where **cross-ECU time alignment** must be correct — signals at different loop rates must share the same time base so sequencing ("did X happen one loop before Y") is faithful. Extra bookkeeping columns on `channels` are ignored by the engine, so they're safe to keep.
> `channels` is by far the largest table — plan to `OPTIMIZE` / Z-order on `(container_id, channel_id)`.

### 3.4 `container_tags` — **optional, recommended** (EAV, for filtering by build/trip/project)

| Column | Type | Notes |
|---|---|---|
| `container_id` | **long** | FK. |
| `key` | string | e.g. `software_build`, `vehicle`, `trip`, `project`, `device_class`. |
| `value` | string | |

> Needed to use `tag_filters` and to inject container-level metadata into UDFs. **This is how the ADCAP "correlate pass/fail to software build" capability is preserved** — put software/build revision, vehicle, trip, project, and device class here (or as `container_metrics` columns).

### 3.5 `channel_tags` — **optional, recommended for ADCAP** (EAV channel selection)

| Column | Type | Notes |
|---|---|---|
| `container_id` | **long** | FK. |
| `channel_id` | **int** | FK. |
| `key` | string | e.g. `channel_name`. |
| `value` | string | e.g. `VeEPSI_n_LoresI`. |

> With this table, `query.channel(channel_name="VeEPSI_n_LoresI")` resolves against `channel_tags` where `key='channel_name'`. Given ADCAP selects signals by **INCA signal name**, providing `channel_tags` with a `channel_name` key is the clean path (and lets the same schema carry arbitrary signal sets across projects).

### 3.6 `poi_channels` — optional (Points-in-Time / discrete-event signals)

`(container_id long, channel_id int, timestamp long [epoch µs], value_double double, value_string string)`. Provide only if we select POI channels.

### 3.7 `channel_mapping` + `unit_conversion` — optional (aliases & units)

Logical→physical channel alias table (+ per-unit conversion factors). **This maps to the SPOT `Aliases` sheet** (master signal → ordered alias fallbacks) — provide if the POC signals rely on alias resolution or unit conversion.

---

## 4. GM Business Logic to Hand Over (so we reproduce the right results)

From the current tooling, GM should provide:

- [ ] **Python test definitions** (from `GM-SDV/etc-time-series-tests`) for the 3–5 tests we reproduce in the POC — including each test's **Concern** and **Failure** thresholds and the signals used. *(e.g. `test_p0171_system_too_lean_bank1`, `test_p134b_nox_catalyst_efficiency_during_regen`, `test_p0128_ect_below_thermostat`.)*
- [ ] **SPOT input config** (`SPOT_INPUT_dashboard_LS6_v1.xlsx` or equivalent), specifically:
  - **Custom Signals** — the algebraic derived-signal definitions (e.g. `Coolant_dT = VeEECR_T_EngArbitrated − VeEECR_T_EngInletCoolant`).
  - **Filters** — global data-inclusion filters (Equal/Min/Max per signal).
  - **Anomaly Detection** — named boolean detection logic.
  - **Channel List** — the exact signals to process.
  - **Aliases** — master → alias fallbacks.
- [ ] **Signal dictionary / units** for the POC signals (name, unit, expected range) — so histogram bins, unit conversions, and plausibility match SPOT/ADCAP.
- [ ] **Expected outputs for parity validation** — for a known set of files/builds, the ADCAP tool's KPI values and pass/fail/violation counts, so we can confirm Impulse matches.
- [ ] **Target SPOT reports** to reproduce (e.g. the 2D-hist axes: RPM × `VeMAFR_m_AirPerCylCurEst_Trpd`, bin edges, and the aggregated stat per cell).

---

## 5. How GM's Data Connects to Impulse — Bronze-direct vs. Silver (decide with data engineering)

Ingestion is owned by data engineering and MF4 is **already in Databricks**. So the first analytics-layer decision is *how Impulse reads that data*. The Impulse [ingestion / "Adapting to existing data layouts"](https://databrickslabs.github.io/impulse/docs/data_model/ingestion/#adapting-to-existing-data-layouts) guidance gives the rule: *"SolverConfig for naming differences, custom solver for structural differences, ETL into the standard shape for everything else."*

### 5.1 Preferred direction — connect Impulse **directly to GM's narrow bronze** (evaluate first)

From the screenshots, GM appears to **pivot bronze → a wide silver table**. Impulse can instead read GM's existing layout via a **custom solver**, so **the pivot may be unnecessary**. This is the recommended option to evaluate first, and it has real advantages:

- **No pivot step** — saves the compute/storage cost of building and maintaining a wide silver table.
- **Full channel coverage** — Impulse sees **all** channels in bronze, not just the subset that made it into a pivoted table (pivoted/wide tables almost always carry a limited signal set).
- Impulse's `channels` model is intentionally **narrow** (`container_id, channel_id, tstart/tend/timestamp, value`), which is the natural shape of un-pivoted bronze samples.

**Precedent & support:** Databricks did exactly this at **Stellantis** — a **custom `QuerySolver`** over the customer's existing layout — and the Impulse team has offered to **help build the equivalent solver for GM**. A **second native single-input-table solver** (one table only) is also on the Impulse roadmap, which may make bronze-direct even simpler.

### 5.2 Fallback paths (if bronze can't be read directly)

- **Reshape — ETL into the standard shape.** One-shot ETL `SELECT` from bronze → the §3 silver tables; wide metadata **unpivoted into `(container_id, key, value)`** tags. Cleanest when a custom solver isn't warranted.
- **Adapt via `SolverConfig`.** Keep GM's layout; declare per-table `column_name_mapping` (physical → internal names `container_id`, `channel_id`, `tstart`, `tend`, `value`, `key`), plus optional per-table `filters` and a top-level `project_id`. Naming differences only — the silver relationships must still hold.
- **Custom solver.** For structurally different layouts (no EAV, composite keys, JSON-encoded values). Subclass `QuerySolver` and implement `filter_container_tags/…_metrics/…_channel_tags/…_channel_metrics` and `solve()`. *(This is also the mechanism used in 5.1 to read bronze directly.)*

### 5.3 What GM needs to provide to make this call (data discovery)

- [ ] **Example data of the existing bronze layer** (schema + a small sample) — column names, types, keys, and how signals are represented (narrow samples vs. something else).
- [ ] **Example data of the pivoted silver/wide table**, if they keep one — and specifically **how many columns** it has.
- [ ] **What additional metadata is available** and where it lives: **ECU software/build versions, track/route info, fleet metadata, vehicle metadata**, trip info, project. (These become container tags / metrics and drive the build-vs-pass/fail correlation.)
- [ ] **Is connecting Impulse directly to bronze an option?** (governance/access on the bronze tables, and whether data engineering is comfortable with read access.)

---

## 6. Optional but Worth Confirming Early

- [ ] **Incremental processing** — if we enable it, `container_metrics` needs a last-modified column (default `timestamp`) and gold uses `_created_at`. Confirm bronze/silver carries a reliable update timestamp per container.
- [ ] **Metadata/labeling gaps** — some labeling is manual today (~184K files unlabeled in ADCAP Explorer; RPO backfill). Decide whether labels needed for the POC are solved upstream in ingestion or in the silver-shaping step **before** data is used.
- [ ] **Sink target** — confirm the gold `catalog.schema` + a `table_prefix` for the POC report (tables are written as `{prefix}_{entity}`).

---

## 7. Summary Checklist (hand to GM)

**Access**
- [ ] GitHub contributor on `GM-SDV/etc-time-series-tests`
- [ ] Databricks workspace + UC grants (SELECT on bronze/silver; CREATE on gold schema)
- [ ] Named catalog/schema owner to grant access
- [ ] Serverless Env V2+ / DBR ML available

**Data**
- [ ] POC program chosen + confirmed in bronze; project identifier provided
- [ ] UC location of bronze data + read access
- [ ] **Example data + schema of the bronze layer** (narrow) — for the connection-path decision
- [ ] **Example data of the pivoted/wide silver table (if any) + its column count**
- [ ] **Additional metadata inventory**: ECU software/build versions, track/route, fleet, vehicle, trip, project
- [ ] Decision: **connect Impulse directly to bronze** (custom solver, Stellantis-style — Impulse team to help) vs. reshape/adapt

**Impulse input** (only if we don't read bronze directly — §5.2)
- [ ] `container_metrics`, `channel_metrics` (tiny metadata) + `channels` (large; RLE or RAW) in the §3 shape
- [ ] `container_tags` (build/vehicle/trip/project) and `channel_tags` (`channel_name`)
- [ ] Consistent integer time base (epoch µs) with cross-ECU alignment verified
- [ ] `channel_mapping`/`unit_conversion` if aliases/units are in play

**Business logic**
- [ ] Selected Python test definitions (thresholds + signals)
- [ ] SPOT Custom Signals / Filters / Anomaly Detection / Channel List / Aliases
- [ ] Signal dictionary + units + expected ranges
- [ ] Expected KPI/pass-fail outputs for parity validation
- [ ] Target SPOT reports (axes, bins, aggregates) to reproduce

---

### Internal column-name reference (for `solver_config` remapping)

Fixed engine-internal names to map onto: `container_id`, `channel_id`, `tstart`, `tend`, `value` (RLE) / `timestamp`, `value`, `is_plausible` (RAW), `key`, `value` (tag tables). Container timestamp columns in the reference schema are `start_dt` / `stop_dt` / `duration_ms` / `num_channels`. Any `container_metrics` column can be surfaced to gold via `measurement_dimensions`.
