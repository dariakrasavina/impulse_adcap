# ADCAP POC — Verified Data & Build Findings

**Last updated:** 2026-10-02
**Source:** read-only inspection of GM's workspace `adb-2053852316949185` — catalogs `design_development_prod.bronze_6060998_adcap` (bronze) and `design_development_test.silver_plus_adcap_impulse_poc` (the existing Impulse build) — plus the GM test scripts in `../tests/`. These are **observed facts**, and supersede the inferred notes in `PROJECT_CONTEXT.md` where they overlap.

---

## 1. Bronze source tables (verified schemas)

### `adcap_veh_eng_data` — channel data (EXTERNAL **Parquet**, **~41.5 TB** `totalSize`)
One row per **(fileName, group, channelName)**, with the time series as **parallel arrays**:

| Column | Type |
|---|---|
| `fileName` | string (→ container) |
| `group` | string (= sample-rate raster, see §2) |
| `channelName` | string |
| `timeStamps` | **array\<double>** (epoch seconds) |
| `utcOffsets` | array\<string> |
| `values` | **array\<string>** |
| `enumMap` | **map\<string,bigint>** (enum label → code) |
| `my`, `date`, `vehicle_number` | string |

- **`adcap_veh_eng_data_delta`** — MANAGED Delta, **54,529 rows (~13.7 GB)**, the **10-file working subset** (same shape; `fileName`→`file_name`, +`output_file`). This is the practical POC input, not the 41.5 TB external table.

### `adcap_file_status` — per-file metadata (Delta, 36 cols, **~77,453 files** indexed; `status='BRONZE_COMPLETE'`)
Container-level metadata source. Notable columns: `file_name`, `file_size`, `recording_duration`, `pre/post_trigger_time`, `vehicle_number`, `trip_names` (array), `experiment`, `program_description`, `devices`, `database`, `workspace`; build/cal codes `engine_rp/sv/wp`, `transmission_rp/sv/wp`, `rp`, `wp`; `project_name`, `model_year`, `AD_PROJECT_NAME`, `AD_MODEL_YEAR`; pipeline `status`, `_SUCCESS`, `error`, paths.
- **`engine_sv`** (e.g. `E42Aa262718250gb_quasi`) is the clean **software-build** key (131 distinct across files) for pass/fail-by-build.
- ⚠️ `project_name` / `model_year` columns are **mostly blank** — program (`DK68`) and MY (`MY27`) are embedded in the build strings (`transmission_rp = T93A_…MY27_DK68_…`) and `experiment`, so they must be **derived**, not read from the named columns.

## 2. Confirmed data facts (from queries)

- **Hierarchy:** Program → **Test = Container = one MF4 file** → Channel → timestamped samples.
- **`group` = sample-rate raster — CONFIRMED.** Per-group approx rate lands on clean values: **1, 4, 10, 20, 40, 66, 80, 100, 159, 198, 317, 1000 Hz**; all channels in a group share one time axis. **Multiple groups can share a nominal rate**, and **the same channel can appear in >1 group** (`VeECTI_T_EngineCoolant`, `VeEPSR_n_Engine`, …) ⇒ **channel identity must be `(channelName, group)`**, not channelName alone.
- **Timestamps = epoch seconds (double)**, shared per group.
- **Value typing (verified on the 10-file subset):** `values` is `array<string>`; by channel —
  - **5,063 / 5,441 channels (93%) are numeric** (number stored as a string, casts cleanly to double),
  - **378 / 5,441 channels (7%) are enum/state** (value is a **string label** e.g. `CeEGRC_e_PstnClosedLoopDsbld`, with `enumMap` → integer code; up to 382 entries),
  - **0 free-form/other text.** `enumMap` (not value parsing) is the reliable per-channel flag.
- **Shape per file:** **5,441 channels × 48 groups** (vehicle KDUWT1217 subset).

## 3. POC scope — the 5 client-selected files

Client selected these 5 files; **all are `BRONZE_COMPLETE`, loaded, and already modeled** as `container_id` **6–10** in the existing build:

| odo | descriptor | channels | groups | samples | duration | software build | container_id |
|---|---|---|---|---|---|---|---|
| 39552 | MPG | 5,441 | 48 | 620.7M | 34:49 | E42Aa262718250gb_quasi | 6 |
| 39575 | Linden | 5,441 | 48 | 645.9M | 35:39 | E42Aa262718250gb_quasi | 7 |
| 39591 | CostcoGB | 5,441 | 48 | 581.7M | 32:08 | E42Aa262718250gb_quasi | 8 |
| 39610 | Fenton | 5,441 | 48 | 498.6M | 27:23 | E42Aa262718250gb_quasi | 9 |
| 39643 | MPG_Regen | 5,441 | 48 | 1.34B | 1:13:45 | E42Aa262718250gb_quasi | 10 |

- POC analytics can **scope to `container_id IN (6,7,8,9,10)`** immediately.
- ⚠️ **All 5 share one software build** (`E42Aa262718250gb_quasi`) ⇒ can't demonstrate build-to-build pass/fail trends within this set; add files spanning ≥2 `engine_sv` builds if that's needed for the pitch.

## 4. Existing build — `design_development_test.silver_plus_adcap_impulse_poc`

A coworker (Thomas Bonford, Impulse creator) already ran the **full pipeline** on the 10-file set.

**Silver (input):**
| Table | Rows | Shape / notes |
|---|---|---|
| `channels` | **901,526,883** | **RLE**: `container_id, channel_id, tstart, tend, value (double), t_last_measured` |
| `channel_metrics` | 54,529 | `… channel_name, channel_group, sample_count, start/end_timestamp, has_enum (bool)` — **channel identity includes `channel_group`** |
| `container_metrics` | 10 | enriched from `file_status`: `file_name, model_year, date, vehicle_number, channel_count, group_count, total_sample_count, start/end_timestamp, recording_duration_sec, status, engine_rp/sv/wp, transmission_rp/sv, experiment, program_description` |
| `channel_descriptions` | 105 | `channel_name, channel_group, description` (for agents/Genie) |

**Gold (Impulse output, `adcap_` prefix):** `event_instance_fact` **723**, `histogram2d_fact` **15,600**, `histogram_fact` **1,400**, `stats_aggregator_fact` **7,866**, `calculated_channel_fact` **3,179,720**, + matching dimensions + `measurement_dimension`.

**Modeling choices:** RLE channels; **channel identity = (channel_name, channel_group)**; metadata as columns (no `*_tags` tables); `channel_descriptions` lookup.

**Three gaps vs. GM's needs:**
1. **Enum string labels are dropped.** `channels.value` is `double`; for an enum channel (`VeSHPC_e_DPM_GearTarget`, `has_enum=true`) it stores the **code** (9.0, 1.0, …), not the label — GM wants the string preserved. The `enumMap` isn't carried into silver. **Decision needed with Thomas** (e.g. value=double code + a compact per-channel enum lookup table, or the MB `string_value`/`value_map` pattern). *(Note: likely a non-issue for the boost/EGR POC tests — their signals are numeric/boolean, §5.)*
2. **Gold is demo analytics, not GM's work.** Events are generic (`engine_running`, `idle`, `high_rpm`, `highway`, `cold_engine`, `hard_braking`); 2D-hists are `rpm_vs_speed`/`accel_vs_brake`/`coolant_vs_speed`; calc-channels are `Coolant−Ambient delta`, `TC slip` — **not** the GM Python tests or SPOT custom signals/plots.
3. **SPOT 2D-hist gap still open.** The built 2D-hists are `histogram_duration` (occupancy), **not** the SPOT RPM×Air **mean/min/max-of-Z** binned statistic (see `ADCAP_POC_PROPOSAL.md` §4.4).

**Net:** Phase 1 (data modeling, bronze→Silver Plus→gold) is **done and validated at scale**; the remaining work is the **GM-specific** half — real tests + SPOT plots + enum/value decision.

## 5. GM test scripts (in `../tests/`) — inventory & Impulse mapping

**Structure (two layers):**
- **`test_<domain>_validation.py`** = the **DK68-specific ADCAP test entry points** (what runs); thin, declare `SIGNALS_OF_INTEREST`, delegate to the common module.
- **`<domain>_validation.py`** (really `tests.common.<domain>_validation`) = the **shared, program-agnostic implementation** they call (takes `program_label`, config).
- **Discovery convention** (per their `AGENTS.md`, via `inspect.getmembers`): `test_*` → a pass/fail **test**, `fom_*` → a **figure-of-merit**, leading `_` → hidden helper.
- **MF4/`asammdf`-native** today (`mdf.get(signal, raw=…)`, `MdfException`) — reading MF4 directly.
- ⚠️ **Not runnable as shared** — imports expect a `tests/common/` package (`dtc_validation.py`, `common/*_validation.py`) that isn't in the shared folder (only flat copies of the DK68 files + common modules).

**7 active tests + FOMs** (2 domains):

| Test | Gate / condition (Concern/Failure) | Signals | Impulse equivalent |
|---|---|---|---|
| `test_p0299_turbo_underboost` | while `VeAICD_b_BstDevPos_DiagEnbl=1`: `VeAICD_p_BoostCntrlDev > VeAICD_p_BstDevPosThrsh` | boost dev / thresh / enable | `BasicEvent` (gate & compare) |
| `test_p0234_turbo_overboost` | while `…BstDevNeg_DiagEnbl=1`: `BoostCntrlDev < BstDevNegThrsh` | ″ (neg) | `BasicEvent` |
| `test_boostcontrol` | `|VeAICR_p_BoostReq − VeAICR_p_BstFdbck| < 5 kPa` when CL active & request stable | boost req/fdbck, CL flag | `BasicEvent` + `StatsAggregator` |
| `test_p0401_egr_flow_insufficient` | while `VeAICD_b_InsufEGR_DiagEnbl=1`: `InsufEGR_ResThrsh > EGR_SysFlowDiagRes` | EGR res/thresh/enable | `BasicEvent` |
| `test_p0402_egr_flow_excessive` | while `…ExcsvEGR_DiagEnbl=1`: `ExcsvEGR_ResThrsh < EGR_SysFlowDiagRes` | ″ (excessive) | `BasicEvent` |
| `test_p140c_egr_slow_response_decreasing` | `VeAICD_Pct_DecrEGR_RespErrAvg != 0 & > concern(ambient)`; concern = 16% @96 kPa → 13% @85 kPa (interp) | resp err, `VeAAPI_p_AmbAir` | `CalculatedChannel` (ambient-interp threshold) + `BasicEvent` |
| `test_egrvalvecontrol` | EGR valve control / residual error | valve pstn, residual | `BasicEvent` + `StatsAggregator` |

**FOMs** (trendable metrics → `StatsAggregator` / `PointValueAggregator`): `fom_p0299` (max `OeAICR_p_BstDevPosM6_Value`, concern>-20/fail>0), `fom_p0234` (min `OeAICR_p_BstDevNegM6_Value`, concern<10/fail<0), `fom_maxvgt` (RMSE VGT pstn vs measured while CL), `fom_p0401/p0402_egr_window_mean_margin`, `fom_p140c`, `fom_residualerror`. Commented-out/stubbed: `test_p049b/p049c_egr_b_flow_*`.

**Enum relevance:** the boost/EGR test signals are numeric (`_p_` pressure, `_m_` mass) and boolean enable gates (`_b_`, stored as 0/1 numeric) — so the enum-label-preservation gap (§4.1) is **likely out of scope for these 7 tests**; confirm none of their signals are in the 378 enum set.

## 6. Open decisions (updated)
- **Enum value typing** — preserve enum labels (per GM) vs. the current `double`-code build. Likely not blocking for the boost/EGR POC tests; needed for enum-valued analyses later.
- **Port the real work** — replace the demo gold with the 7 tests (§5) + SPOT Template-Plots rows, scoped to `container_id 6–10`.
- **SPOT 2D-hist mean-of-Z** — implement the binned-statistic extension (`ADCAP_POC_PROPOSAL.md` §4.4).
- **Single-build scope** — add files spanning ≥2 `engine_sv` builds if build-trend demonstration is wanted.
- **Make the tests runnable** — obtain the `tests/common/` package (incl. `dtc_validation.py`).
