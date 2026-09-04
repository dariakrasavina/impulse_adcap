# ADCAP → Databricks Impulse POC — Requirements & Expectations

**Status:** Draft for review
**Last updated:** 2026-09-03
**Owner (Databricks technical POC):** Daria (Delivery Solutions Architect)
**Primary GM contacts:** Tom (Thomas) Godward, Rafat Hattar (VSSM Calibration)
**Referenced (not on core team):** Bala's team (GM data engineering — owns MF4 → bronze ingestion), Eric (Databricks SA), Aaron (Databricks AE)

> Source material: GM VSSM Calibration scoping session notes (Aug 28, 2026), ADCAP tool screenshots, and the `SPOT_INPUT_dashboard_LS6_v1.xlsx` configuration workbook. This document defines *what the ADCAP team needs* and *what the POC is expected to build* — not the implementation design.

---

## 1. Background

GM's VSSM Calibration team analyzes vehicle time-series calibration data using **ADCAP ("Ad Cap") Time Series Analyzer** — a GM-built, in-house framework (not a vendor tool). It supports ~300 calibrators GM-wide, most of whom do not write Python.

Today the workflow spans **three parallel tools**, all fed from raw **MF4** binary files:

| # | Tool | Role | Pain |
|---|------|------|------|
| 1 | **ETAS INCA / MDA** (manual) | Engineer eyeballs raw signal traces and manually root-causes issues | Fully manual, per-file, not scalable |
| 2 | **SPOT reports** | Scatter / 1D & 2D histograms / filtering / multi-file aggregation, driven by a spreadsheet config | Static HTML output, re-run per dataset |
| 3 | **ADCAP Time Series Analyzer** (web) | Runs a GitHub repo of Python "tests" (KPIs) against files; tracks pass/fail/violations by software build | Reprocesses raw MF4 every time; results live in a local DB "on an island" |

**The core problem:** every new test case or investigation re-loads and re-processes raw MF4 into a throwaway data frame. There is **no persistent, queryable database of already-computed results**, and results cannot be cross-referenced with the rest of GM's data ecosystem.

---

## 2. The Need (why GM wants to move)

Ranked roughly by the emphasis given in scoping:

1. **Eliminate repeated reprocessing (top pain point).** Raw MF4 is re-parsed for every test/investigation, used once, then discarded. GM needs computed signals/KPIs persisted and queryable on demand.
2. **Centralization.** ADCAP results are siloed and can't be joined to other GM data (e.g. correlating a data issue against manufacturing data is impossible today).
3. **Compute & scale.** Query and slice large historical datasets on demand, not limited by a single tool/session.
4. **Tool consolidation.** Collapse the 3 tools/methods (including laptop-local Python scripts) into one centralized system.
5. **Accessibility for non-Python users.** Make analysis usable by the ~300 calibrators who don't know Python.
6. **Faster ad hoc investigation.** When an issue surfaces in vehicle testing, a ready queryable store removes the write-a-script-and-reprocess bottleneck.

---

## 3. Goals & Scope

### 3.1 In scope (POC)
- Apply the **standard Impulse pipeline first** (bronze → Impulse-processed silver/gold) to a **focused set of 1–few vehicle programs/projects** to establish baseline outputs.
- Then **tune with GM-specific business rules** (derived signals, filters, KPI/test logic) based on feedback.
- Demonstrate that computed signals and KPIs land in **persistent Unity Catalog tables** that are queryable without reprocessing raw MF4.
- Preserve GM's ability to **run its own Python** inside Databricks (scripts re-pointed from raw MF4 to UC tables — no lost functionality).

### 3.2 Explicitly out of scope (POC)
- Migrating all ~60 TB / all projects at once.
- Replicating the entire ADCAP web tool UI.
- Owning the MF4 → bronze **ingestion** pipeline (that stays with Bala's data-engineering team).
- Building the natural-language "Genie" calibrator agent (a *later* aspiration; requires codified context first — see §7).

### 3.3 Success criteria
- One vehicle program flows end-to-end: bronze MF4 → Impulse silver/gold → persisted KPI/derived-signal tables in UC.
- A representative subset of GM tests/KPIs (see §6) reproduced against those tables, with results **matching** the current ADCAP tool.
- Demonstrable "macroscope → microscope" workflow: query aggregate KPIs, then drill into a specific trip/time window/signal — **faster or easier than today** (the agreed bar: if it isn't, there's no reason to move).
- A GM Python script re-pointed to a UC table produces the same result as against raw MF4.

---

## 4. Data Requirements

- **Source format:** MF4 binary files only. No other formats in use for ADCAP.
- **Volume:** ~60 TB total, already compressed. (ADCAP Explorer dashboard observed: ~185K recordings, ~61 TB, ~932 vehicles, ~91 projects.)
- **Landing:** Bronze layer is owned by GM data engineering (Bala's team). One project is already fully in bronze; others are not. Additional projects will be moved as the POC scope firms up — deliberately not "push it all and have it sit idle."
- **Domain model:**
  - **Project** = a currently active GM production vehicle program (e.g. `T1XX_HDPU_DK68`, `Y2xC_LS6_M1L`; architectures T1xx-2 / Y2XX; MY2027 / MY2028; Engine / Transmission sectors). ~6 major programs currently releasing.
  - **Trip** = a vehicle test run; calibrators upload trip data against a project.
  - **Device / device class** (e.g. ECM), **software/build revision** — must be tracked and joinable to results.

### 4.1 Time alignment (hard requirement)
- Multiple ECUs record signals at **different loop rates**. Sequencing matters ("did X happen one loop before Y").
- **Time alignment across signal groupings must be preserved on ingestion.**
- Downsampling is acceptable in some cases, but **exact timing must be preserved** where data will feed back into simulation / co-simulation with the ECU software.

### 4.2 Metadata & labeling
- Parse as much metadata as possible out of the MF4 file itself during bronze ingestion (software build, vehicle, project, trip, RPO/hardware-revision codes).
- Some labeling is still **manual** today (observed: "help label a file" and RPO backfill in ADCAP Explorer, with ~184K files unlabeled). Decide whether this is solved in Databricks or upstream in ingestion **before** data lands in bronze.

---

## 5. Functional Requirements (what to build)

Mapped to the three tools the POC must eventually subsume:

### 5.1 Persistent computed-signal & KPI store
- Compute derived signals and KPIs **once** and persist them to queryable UC tables (silver/gold).
- Track lineage: result ↔ file ↔ project ↔ device ↔ **software build** (so pass/fail trends correlate to specific builds — this is a current, valued ADCAP capability).
- Support incremental reprocessing when a test definition changes (today this triggers a full re-run).

### 5.2 Test / KPI computation (ADCAP "Tests")
- Reproduce the GitHub-repo test model. Tests currently live in **`GM-SDV/etc-time-series-tests`** (Python), resolved by branch/sha, discovered per project.
- Each test encodes a **Concern** threshold and a **Failure** threshold over one or more signals and resolves to a boolean/violation. Examples observed:
  - `test_p0171_system_too_lean_bank1` — P0171 System Too Lean Bank 1.
  - `test_p134b_nox_catalyst_efficiency_during_regen` — Concern `VeSCRD_r_EffDiffRGN_SCR2 < 0.45`; Failure `OeSCRR_r_EffDiff_EWMA_RGN_SCR2 < 0`.
  - `test_p0128_ect_below_thermostat` — Concern/Failure on `NsETHD_h_EOR_WarmUpDiagData.a_r_PassRatioFOM[…]`.
- Test modules observed: `test_boost/ combmode/ coolant/ dpf/ egr/ fuel_pressure/ fueling/ idle_quality/ maf/ o2_nox_sensor/ oil_catalyst/ scr/ spec_range/ torque_response/ torque_validation.py`.
- Emit the KPIs the team relies on: **violation distributions, pass rates, fail rates, FOM (figure of merit)**, run counts, distinct files, durations — sliceable by project / module / test / device class / software revision.

### 5.3 Aggregate reporting (SPOT-equivalent)
- Reproduce SPOT report primitives against persisted tables: **scatter, 1D histogram, 2D histogram (heatmap), and filtering**, with **multi-file / multi-trip aggregation**.
- Support binning over engine operating axes (e.g. RPM × `VeMAFR_m_AirPerCylCurEst_Trpd`) with per-cell aggregate stats (avg / min / max / std / frequency), as seen in the SPOT 2D-hist plots.

### 5.4 "Macroscope → microscope" drill-down
- High-level KPI view first, then drill into a specific trip → time window → raw signal traces (the INCA/MDA manual step). This drill-down must be at least as fast/easy as current tooling.

### 5.5 GM business rules from `SPOT_INPUT` (to be codified as transforms)
The SPOT input workbook is effectively GM's business logic in a spreadsheet DSL and should be translated into silver/gold transforms:
- **Custom Signals** — algebraic derived signals, e.g. `Coolant_dT = VeEECR_T_EngArbitrated − VeEECR_T_EngInletCoolant`, `MinAir_Violation`, `MAP_Pred_*_Err`, per-cylinder `CA50` averages. Expressed today as `SDF['signal']` expressions (Python/NumPy semantics).
- **Filters** — global data-inclusion filters (Equal-to / Min / Max on a signal, e.g. keep coolant temp between bounds).
- **Anomaly Detection** — named boolean detection logic per signal, with associated filters.
- **Channel List** — additional INCA channels to process (e.g. knock octane scalers per cylinder/zone).
- **Aliases** — signal name aliasing (master signal → ordered alias fallbacks).
- **MSG (Missing Signal Generator)** — ML model that synthesizes a missing channel from training inputs→output (nice-to-have / later).

### 5.6 Preserve GM Python
- GM must retain the ability to run existing Python inside Databricks once tables are referenced in Unity Catalog — scripts re-point from a raw MF4 file to the correct UC table/source. No functionality lost.

---

## 6. Representative KPIs / Tests to reproduce in the POC

Pick a tractable subset for validation (final list TBD with GM):

- Fueling / lean: `test_p0171_system_too_lean_bank1`
- SCR / NOx catalyst: `test_p134b_nox_catalyst_efficiency_during_regen`
- Coolant / thermostat: `test_p0128_ect_below_thermostat`
- SPOT aggregates: EGR Valve Area, Avg Octane Scaler, IAT/ECT spark offset (2D hist + scatter over RPM × AirPerCyl)

Validation target: KPI values and pass/fail/violation counts **match** the current ADCAP tool for the same files/software builds.

---

## 7. Longer-term (post-POC, noted for direction — not committed)

- Replace many bespoke Python test scripts with a natural-language **Genie**-style agent a calibrator can query directly. Requires first **codifying baseline context**: the data, its signals, and the desired calculations.
- Distinction to hold onto: **Genie = analytics agent** (ad hoc, non-persisted natural-language querying), **not** a data-engineering agent — it will not, on its own, create/maintain persistent KPI tables.
- ML / co-simulation use cases (feed preserved-timing data back into ECU software simulation).

---

## 8. Open Questions / Decisions Needed

| # | Question | Notes |
|---|----------|-------|
| 1 | **Table ownership & schema evolution** | Which tables should the calibration/analytics team own & manage directly (to add signals/KPIs on the fly, potentially daily during investigations) vs. remain owned by data engineering? Data engineering has historically wanted signal lists specified up front; GM needs to adjust on the fly. **Unresolved — may warrant its own dedicated session.** |
| 2 | **Ad hoc "dig into a thread" — Databricks vs. local?** | Is thread-style deep-dive genuinely faster in Databricks notebooks than local tools? To be **validated during the POC, not assumed** (the agreed bar: no speed/ease win = no reason to move). |
| 3 | **Manual labeling** | Solve in Databricks or upstream in ingestion, before bronze? |
| 4 | **Which/how many projects into bronze** | Beyond the one already fully present — confirm once POC scope is finalized. |
| 5 | **Scope creep → MVP** | If scope grows past a POC, shift the conversation to MVP/rollout planning (flagged by Eric). |

---

## 9. Access & Logistics (prerequisites to start)

- **GitHub:** Add Daria as a contributor to the private test-scripts repo (`GM-SDV/etc-time-series-tests`). Tom to share access/links.
- **Databricks:** Daria needs broader catalog/schema/workspace access beyond prior dev-level access. Either a help-desk/access request, or direct grant from a GM catalog-owner. Daria to Slack Tom the exact permissions needed.
- **Communication:** Eric to set up a Slack group thread for day-to-day coordination.

## 10. Cadence & Roles

- **Weekly check-ins** (minimum), proposed **Friday mornings ET**; Slack for interim updates. Ad hoc deep-dive sessions as needed (e.g. schema evolution).
- **Daria** = primary technical POC for GM; may work directly with Bala's team on the data. Can pull in Databricks' European SA/PS team (original Impulse authors) for deeper dives.
- **Target:** complete a POC/pilot by **end of 2026**, with the framework in place to scale to the broader calibrator base afterward.

---

## 11. Decisions already reached

1. Proceed with a focused POC scoped to one / a small number of programs — not a full migration or full ADCAP re-build.
2. Apply the standard Impulse pipeline first, then tune with GM business rules from feedback.
3. Weekly Friday-morning (ET) check-ins + Slack interim updates.
4. Daria is the primary technical point of contact.
