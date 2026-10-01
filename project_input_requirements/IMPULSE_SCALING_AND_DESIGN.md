# Impulse at Scale — Design Guidance from the Impulse Engineering Team (EU)

**Last updated:** 2026-10-01
**Source:** Q&A with the European team that built Impulse (the framework we're pitching to GM). Reusable reference for scaling, multi-table layout, custom solvers, streaming, agents, and value typing.
**Companion:** `PROJECT_CONTEXT.md` (GM POC source of truth). GM-specific implications are called out as **→ GM** below.

---

## 1. Scaling & maximum `channels` table size

**There is no hard maximum, but the working recommendation is: keep a single `channels` table in the low 100s of TB — two-digit TB per table is even better. It depends on the customer and use case.**

Key principle: **do not persist ALL data in one `channels` table.** If something goes wrong, you'd have to recover petabytes. Partition into multiple tables (see §2) and size each for recoverability.

Real-world sizing from existing deployments:

| Customer | Partitioning of `channels` | Per-table size |
|---|---|---|
| **Mercedes-Benz** | **one table per vehicle** | 3–10 TB each |
| **Stellantis** | **one table per month** (a month = an entire fleet) | ~**500 TB** for a 5-month table (much bigger per table) |
| **AVL** | **one table per project** | up to **tens of TB** (smaller datasets) |

**→ GM:** one program ≈ **41.5 TB** today; with ~6–8 programs the total lands in the **low 100s of TB** — comfortably in range. Natural partitioning = **one `channels` table per program** (mirrors MB's per-vehicle / AVL's per-project pattern), each well-sized for recovery. Avoid a single monolithic table.

## 2. Working with multiple `channels` tables (and "joining" them)

Impulse combines multiple `channels` tables by **union**, not by join — there are two supported ways:

- **A) Impulse takes an array of `channels` tables as input and unions them on the fly.** (Mercedes-Benz)
- **B) Union the tables in a Unity Catalog view and point Impulse at the view.** (Stellantis)

So "joining multiple channels tables" is really a **union** (same schema, more rows) handled either in a UC view definition or on the fly by Impulse — TSAL queries and agents are unaffected by how many physical tables sit underneath.

**→ GM:** per-program tables unioned via a **UC view** (option B) is the simplest path and keeps TSAL/report code identical regardless of how many programs are onboarded.

## 3. Very large / non-EAV datasets — custom solver

If a customer already has the data in Delta/Iceberg **analytics-ready (silver) form** but **not** in Impulse's EAV model, the recommendation is **not** to force a reshape — instead **evaluate a custom solver** so Impulse connects to the existing model.

- Done for **Stellantis** (their own narrow-but-different model + different metadata tables); writing the solver took **a few days**.
- The process for adding custom solvers is being improved: **PR databrickslabs/impulse#109** (https://github.com/databrickslabs/impulse/pull/109).
- Context for scale: GM Cruise-class data can be **600–700 PB**; at that scale a reshape ETL is impractical and a custom solver over the existing analytics-ready model is the recommended approach.

**→ GM:** this is exactly the **reserve option** in `PROJECT_CONTEXT.md` §2. GM's POC data is small enough for the native EAV model (Option 1), but the custom-solver path is proven and getting easier (PR #109) if GM later declines to restructure or scales far beyond the POC.

## 4. Streaming / combining streaming + batch

- **No support today** for Spark Structured Streaming workloads.
- A **design exists** for how it could work; it's **deprioritized** because there was no concrete use case yet.
- Expected to matter more for **manufacturing** use cases (more streaming there than in automotive testing).

**→ GM:** fine for the calibration POC — the target is an **overnight batch cadence**, not real-time, so the batch-only model matches the need.

## 5. Genie / AI agents on the Impulse data model

Agents work **well** on this data model — the approach is to **let the agent write Impulse (TSAL) code**, not query raw tables:

- It's **easy for an agent to formulate TSAL queries**; Impulse then does the heavy lifting (channel synchronization, interpolation, channel mapping, unit conversion).
- Agents work well **as long as the metadata tables (metrics & tags) carry good descriptions** — the agent relies on those to understand the data.
- Mercedes-Benz is building **AI-assisted report creation** on top of Impulse; there is also an **Impulse app that builds an analysis from natural language**.

**→ GM:** supports the "macroscope → Genie" aspiration. Precondition = **well-described tags/metrics metadata**. The agent authors TSAL (consistent with the Impulse `skills/`), rather than being pointed at thousands of wide columns — a key reason the narrow model (not the wide table) was chosen.

## 6. Value typing for heterogeneous channel data types (MF4)

Channel values are **not uniformly doubles** — in MF4/automotive testing a value can be string, double, integer, enum/coded, etc. This is common and expected.

**Mercedes-Benz's recommended pattern — multiple value columns on `channels`:**

| Column | Type | Holds |
|---|---|---|
| `raw_value` | integer | raw/coded value |
| `scaled_value` | double | scaled numeric value |
| `string_value` | string | string/enum text |
| `value_map` | — | **per-datapoint selector**: which value column the actual datapoint should be read from |

- The **`value_map`** column tells Impulse, for each datapoint, which column is authoritative.
- `value_map` is **also present on the `channel_metrics` table**, where it defines the **value type (i.e. which value column) for an entire channel**.

**→ GM:** this **resolves** the earlier open question in `PROJECT_CONTEXT.md` (§3/§11) about typing `value` as a single string. The recommended approach is **not** a single string column but **multiple typed value columns + a `value_map` selector** (per-datapoint on `channels`, per-channel on `channel_metrics`) — preserving enum/string values losslessly while keeping numerics efficient. Confirm the concrete column set with Thomas Bonford against GM's real bronze.
