# Data Observability & Pipeline Monitoring

**SYNTHETIC DATA. Portfolio skill demo, NOT work experience.** No real
pipelines, customers, or revenue. SQLite stands in for a warehouse /
observability store; the monitors are plain SQL + Python, not a claim of
production Monte Carlo / Datadog / dbt Cloud experience.

## Business question

A pipeline can fail quietly: rows go missing, a source adds a column, data
arrives hours late — and nobody notices until a dashboard is wrong or a
stakeholder asks why revenue moved. How do you watch a warehouse
continuously so the *first* sign of trouble is an alert, not a complaint?

This is the exact shape of work in current Data Engineer / Data Analyst
postings: ingestion + monitoring/observability (DRW, Data Engineer),
monitoring and trending on reporting pipelines (Aledade, Data Analyst),
and data validation / reconciliation / root-cause analysis (Danone, Data
Analyst, Nov 6 2026 pool).

## Approach

1. **Run history** (`src/generate.py`, seeded — re-runs are identical):
   3 pipelines (orders, customers, payments), one run per day for 60 days
   (2026-09-08–2026-11-06), 180 runs total. Each run records source rows,
   rows loaded, duration, freshness lag, null rate, duplicate rate, and
   the schema in force. Five incidents are injected on known dates so the
   monitors have real signal to catch.
2. **Monitors** (`src/monitor.py` + `sql/monitors.sql`):
   - **Freshness SLA** — lag vs a 120-minute SLA per pipeline.
   - **Volume anomaly** — trailing 14-day mean/std via SQL window
     functions; |z| ≥ 3 fires.
   - **Null rate / duplicate rate** — threshold checks (5% / 2%).
   - **Reconciliation** — rows loaded vs rows in source (±2%).
   - **Schema drift** — column set vs the prior day's run.
   Every alert carries severity, the metric value, and the threshold it
   breached, so triage starts from evidence, not a gut feeling.
3. **Outputs**: `results/alerts.csv` (one row per alert),
   `results/incident_report.md` (triage report with a root-cause note per
   incident), `results/summary.json`, and one volume + freshness chart per
   pipeline in `results/charts/`.
4. **Tests** (`tests/test_monitor.py`, 7 tests; `sql/checks.sql`, 7 data
   tests): every injected incident must be detected, no alert may fire on
   a clean day, loaded rows must equal source rows on clean days, and all
   SQL checks must pass with 0 violations.

## Results (real numbers from actual runs, 2026-11-06)

Verified byte-identical on a from-scratch re-run
(`rm -rf data results observability.db && bash run_demo.sh` —
`summary.json`, `alerts.csv`, and `runs.csv` all match).

### Coverage

- **180 runs monitored** (3 pipelines × 60 days), 0 runs missed.
- **6 alerts fired** — 3 critical, 3 warning — across 5 incident days.
- **5/5 injected incidents detected**, **0 false positives** on the
  175 clean runs.

### The five incidents, as the monitor saw them

| Date | Pipeline | What happened | How it was caught |
|---|---|---|---|
| 2026-10-11 | payments | Schema drift: `payment_method` column added | schema_drift (warning) |
| 2026-10-19 | orders | Volume collapse: upstream partial outage, 612 rows loaded vs 1,976 in source | volume_anomaly z = −7.10 + reconciliation 69.0% gap (both critical) |
| 2026-10-25 | customers | Null spike: email null rate 18.0% | null_rate (warning) |
| 2026-10-30 | payments | Freshness breach: 320 min lag vs 120 min SLA | freshness_sla (critical) |
| 2026-11-04 | orders | Duplicate spike: 6.5% duplicate rate | duplicate_rate (warning) |

### Data tests

- 7/7 SQL checks pass with 0 violations (no duplicate runs, no negative
  metrics, loaded never exceeds source, exactly 60 runs per pipeline,
  all 5 incidents present, no empty schemas, freshness recorded
  everywhere).
- 7/7 Python tests pass, including the zero-false-positive assertion.

## How to run

```bash
bash run_demo.sh        # generate -> monitor -> tests -> SQL checks
```

Requires Python 3.10+ with `pandas` and `matplotlib`
(`pip install -r requirements.txt`); `sqlite3` CLI for the checks step.

## What this does not claim

The run history is synthetic and the monitors run on demand, not on a
live scheduler — this is a portfolio demo of observability *logic*
(baselines, thresholds, reconciliation, drift detection), not a claim of
operating a production observability platform.
