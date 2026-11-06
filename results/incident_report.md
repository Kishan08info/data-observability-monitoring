# Incident triage report (synthetic data)

Monitored 180 runs across 3 pipelines, 2026-09-08 to 2026-11-06. 6 alerts fired (3 critical, 3 warning).

## Alerts

- **2026-10-11 payments** [warning] `schema_drift` — schema changed: added ['payment_method'], removed — (threshold: schema unchanged vs prior day)
- **2026-10-19 orders** [critical] `reconciliation` — loaded 612 vs source 1976 (69.0% gap) (threshold: |loaded - source| <= 2.0% of source)
- **2026-10-19 orders** [critical] `volume_anomaly` — loaded 612 rows vs trailing 14-day mean 1897 (std 181); z=-7.10 (threshold: |z| >= 3.0 (14d mean 1897))
- **2026-10-25 customers** [warning] `null_rate` — null rate 18.0% exceeds 5.0% (threshold: null rate <= 5.0%)
- **2026-10-30 payments** [critical] `freshness_sla` — freshness lag 320.0 min exceeds 120 min SLA (threshold: lag <= 120 min)
- **2026-11-04 orders** [warning] `duplicate_rate` — duplicate rate 6.5% exceeds 2.0% (threshold: dup rate <= 2.0%)

## Root-cause notes (from the injected incident log)

- 2026-10-11 payments: schema drift: payment_method added — detected by: schema_drift
- 2026-10-19 orders: volume collapse: upstream partial outage — detected by: reconciliation, volume_anomaly
- 2026-10-25 customers: null spike: email nulls — detected by: null_rate
- 2026-10-30 payments: freshness breach: upstream late — detected by: freshness_sla
- 2026-11-04 orders: duplicate spike: retry storm — detected by: duplicate_rate
