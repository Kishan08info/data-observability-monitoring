#!/usr/bin/env python3
"""Generate 60 days of synthetic daily pipeline-run history.

Three pipelines (orders, customers, payments) run once a day, 2026-09-08 to
2026-11-06. Each run records what an observability platform would see:
rows in the source, rows loaded, duration, freshness lag, null rate,
duplicate rate, and the schema (column list) in force that day.

Five incidents are injected on known dates so the monitor has real signal
to catch (and the tests can assert they are caught):
  2026-10-11  payments schema drift: new column `payment_method` appears
  2026-10-19  orders volume collapse: upstream partial outage, ~31% of rows
  2026-10-25  customers null spike: email null rate jumps to ~18%
  2026-10-30  payments freshness breach: 320 min lag vs 120 min SLA
  2026-11-04  orders duplicate spike: duplicate rate jumps to ~6.5%

Seeded (random.seed(42)) — re-runs are identical.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

START = date(2026, 9, 8)
DAYS = 60  # through 2026-11-06

BASE = {
    "orders":    {"rows": 2000, "duration": 14, "lag": 35, "null": 0.8, "dup": 0.2,
                  "cols": "order_id,customer_id,amount,status,created_at"},
    "customers": {"rows": 150,  "duration": 6,  "lag": 25, "null": 1.2, "dup": 0.1,
                  "cols": "customer_id,email,plan,region,updated_at"},
    "payments":  {"rows": 1900, "duration": 11, "lag": 30, "null": 0.5, "dup": 0.3,
                  "cols": "payment_id,order_id,amount,method,paid_at"},
}

# (pipeline, date) -> overrides applied on top of the base pattern
INCIDENTS = {
    ("payments", date(2026, 10, 11)): {
        "cols": "payment_id,order_id,amount,method,paid_at,payment_method",
        "note": "schema drift: payment_method added"},
    ("orders", date(2026, 10, 19)): {
        "rows_frac": 0.31, "note": "volume collapse: upstream partial outage"},
    ("customers", date(2026, 10, 25)): {
        "null": 18.0, "note": "null spike: email nulls"},
    ("payments", date(2026, 10, 30)): {
        "lag": 320, "duration": 95, "note": "freshness breach: upstream late"},
    ("orders", date(2026, 11, 4)): {
        "dup": 6.5, "note": "duplicate spike: retry storm"},
}


def main() -> None:
    random.seed(42)
    DATA.mkdir(parents=True, exist_ok=True)
    rows = []
    # Schema evolves: a column added on an incident day stays added
    # (a real schema change persists; a one-day flip would be two drifts).
    current_cols = {p: b["cols"] for p, b in BASE.items()}
    for i in range(DAYS):
        day = START + timedelta(days=i)
        # weekly seasonality: weekends run ~15% lighter
        seasonal = 0.85 if day.weekday() >= 5 else 1.0
        for pipe, base in BASE.items():
            inc = INCIDENTS.get((pipe, day), {})
            if "cols" in inc:
                current_cols[pipe] = inc["cols"]
            source_rows = int(base["rows"] * seasonal * random.uniform(0.92, 1.08))
            rows_frac = inc.get("rows_frac", 1.0)
            loaded = int(source_rows * rows_frac)
            rows.append({
                "run_date": day.isoformat(),
                "pipeline": pipe,
                "status": "success" if rows_frac > 0.5 else "partial",
                "source_rows": source_rows,
                "rows_loaded": loaded,
                "duration_min": round(
                    inc["duration"] if "duration" in inc
                    else base["duration"] * random.uniform(0.85, 1.2), 1),
                "freshness_lag_min": round(
                    base["lag"] * random.uniform(0.8, 1.25)
                    if "lag" not in inc else inc["lag"], 1),
                "null_rate_pct": round(
                    base["null"] * random.uniform(0.7, 1.4)
                    if "null" not in inc else inc["null"], 2),
                "duplicate_rate_pct": round(
                    base["dup"] * random.uniform(0.6, 1.5)
                    if "dup" not in inc else inc["dup"], 2),
                "schema_columns": current_cols[pipe],
                "incident_note": inc.get("note", ""),
            })
    with open(DATA / "runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} runs to data/runs.csv "
          f"({START} to {START + timedelta(days=DAYS - 1)})")


if __name__ == "__main__":
    main()
