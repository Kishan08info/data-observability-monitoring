#!/usr/bin/env python3
"""Data-observability monitor: freshness, volume, quality, schema, reconciliation.

Loads data/runs.csv into SQLite (observability.db), runs the monitors in
sql/monitors.sql (SQL window functions for rolling volume baselines), then
applies threshold checks in Python. Writes:
  results/alerts.csv      — one row per fired alert
  results/summary.json    — run counts, alert counts, incident coverage
  results/incident_report.md — human-readable triage report
  results/charts/*.png    — volume + freshness trends with alerts marked
"""
import csv
import json
import sqlite3
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RESULTS = ROOT / "results"
SQL = ROOT / "sql"

# Monitor thresholds — the "SLA / policy" layer, kept in one place.
FRESHNESS_SLA_MIN = {"orders": 120, "customers": 120, "payments": 120}
NULL_RATE_MAX_PCT = 5.0
DUPLICATE_RATE_MAX_PCT = 2.0
VOLUME_Z_THRESHOLD = 3.0       # |z| vs trailing 14-day mean/std
VOLUME_WINDOW_DAYS = 14
RECON_TOLERANCE_PCT = 2.0      # |loaded - source| / source


def load_db(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS runs")
    conn.execute("""CREATE TABLE runs (
        run_date TEXT, pipeline TEXT, status TEXT,
        source_rows INTEGER, rows_loaded INTEGER,
        duration_min REAL, freshness_lag_min REAL,
        null_rate_pct REAL, duplicate_rate_pct REAL,
        schema_columns TEXT, incident_note TEXT)""")
    with open(DATA / "runs.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    conn.executemany(
        "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [(r["run_date"], r["pipeline"], r["status"], int(r["source_rows"]),
          int(r["rows_loaded"]), float(r["duration_min"]),
          float(r["freshness_lag_min"]), float(r["null_rate_pct"]),
          float(r["duplicate_rate_pct"]), r["schema_columns"],
          r["incident_note"]) for r in rows])
    conn.commit()
    return len(rows)


def sql_monitors(conn: sqlite3.Connection) -> list[dict]:
    """Rolling volume baseline via SQL window functions (sql/monitors.sql)."""
    sql = (SQL / "monitors.sql").read_text()
    cur = conn.execute(sql)
    return [dict(zip([d[0] for d in cur.description], row)) for row in cur]


def check_volume_anomalies(baseline: list[dict]) -> list[dict]:
    alerts = []
    for r in baseline:
        if r["n_prior"] < VOLUME_WINDOW_DAYS or r["prior_std"] is None:
            continue
        z = (r["rows_loaded"] - r["prior_mean"]) / r["prior_std"]
        if abs(z) >= VOLUME_Z_THRESHOLD:
            alerts.append({
                "run_date": r["run_date"], "pipeline": r["pipeline"],
                "check": "volume_anomaly", "severity": "critical",
                "metric_value": round(z, 2),
                "threshold": f"|z| >= {VOLUME_Z_THRESHOLD} "
                             f"(14d mean {r['prior_mean']:.0f})",
                "detail": f"loaded {r['rows_loaded']} rows vs trailing "
                          f"14-day mean {r['prior_mean']:.0f} "
                          f"(std {r['prior_std']:.0f}); z={z:.2f}",
            })
    return alerts


def check_thresholds(conn: sqlite3.Connection) -> list[dict]:
    alerts = []
    for r in conn.execute("SELECT * FROM runs ORDER BY run_date, pipeline"):
        (run_date, pipe, _status, source, loaded, _dur, lag,
         null_rate, dup_rate, schema, _note) = r
        sla = FRESHNESS_SLA_MIN[pipe]
        if lag > sla:
            alerts.append({"run_date": run_date, "pipeline": pipe,
                           "check": "freshness_sla", "severity": "critical",
                           "metric_value": lag,
                           "threshold": f"lag <= {sla} min",
                           "detail": f"freshness lag {lag} min exceeds "
                                     f"{sla} min SLA"})
        if null_rate > NULL_RATE_MAX_PCT:
            alerts.append({"run_date": run_date, "pipeline": pipe,
                           "check": "null_rate", "severity": "warning",
                           "metric_value": null_rate,
                           "threshold": f"null rate <= {NULL_RATE_MAX_PCT}%",
                           "detail": f"null rate {null_rate}% exceeds "
                                     f"{NULL_RATE_MAX_PCT}%"})
        if dup_rate > DUPLICATE_RATE_MAX_PCT:
            alerts.append({"run_date": run_date, "pipeline": pipe,
                           "check": "duplicate_rate", "severity": "warning",
                           "metric_value": dup_rate,
                           "threshold": f"dup rate <= {DUPLICATE_RATE_MAX_PCT}%",
                           "detail": f"duplicate rate {dup_rate}% exceeds "
                                     f"{DUPLICATE_RATE_MAX_PCT}%"})
        gap = abs(loaded - source) / source * 100
        if gap > RECON_TOLERANCE_PCT:
            alerts.append({"run_date": run_date, "pipeline": pipe,
                           "check": "reconciliation", "severity": "critical",
                           "metric_value": round(gap, 2),
                           "threshold": f"|loaded - source| <= "
                                        f"{RECON_TOLERANCE_PCT}% of source",
                           "detail": f"loaded {loaded} vs source {source} "
                                     f"({gap:.1f}% gap)"})
    return alerts


def check_schema_drift(conn: sqlite3.Connection) -> list[dict]:
    alerts = []
    prev = {}
    for run_date, pipe, schema in conn.execute(
            "SELECT run_date, pipeline, schema_columns FROM runs "
            "ORDER BY run_date, pipeline"):
        if pipe in prev and prev[pipe] != schema:
            added = sorted(set(schema.split(",")) - set(prev[pipe].split(",")))
            removed = sorted(set(prev[pipe].split(",")) - set(schema.split(",")))
            alerts.append({"run_date": run_date, "pipeline": pipe,
                           "check": "schema_drift", "severity": "warning",
                           "metric_value": len(added) + len(removed),
                           "threshold": "schema unchanged vs prior day",
                           "detail": f"schema changed: added {added or '—'}, "
                                     f"removed {removed or '—'}"})
        prev[pipe] = schema
    return alerts


def write_charts(conn: sqlite3.Connection, alerts: list[dict]) -> None:
    (RESULTS / "charts").mkdir(parents=True, exist_ok=True)
    alert_days = {(a["pipeline"], a["run_date"]) for a in alerts}
    for pipe in ("orders", "customers", "payments"):
        rows = conn.execute(
            "SELECT run_date, rows_loaded, freshness_lag_min FROM runs "
            "WHERE pipeline=? ORDER BY run_date", (pipe,)).fetchall()
        dates = [r[0] for r in rows]
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
        fig.suptitle(f"{pipe} — daily volume & freshness (60 days, synthetic)")
        ax1.plot(dates, [r[1] for r in rows], marker="o", ms=3)
        ax1.set_ylabel("rows loaded")
        ax1.set_title("Rows loaded per run (red = alert day)")
        for i, d in enumerate(dates):
            if (pipe, d) in alert_days:
                ax1.scatter([d], [rows[i][1]], color="red", zorder=5, s=40)
        ax2.plot(dates, [r[2] for r in rows], marker="o", ms=3, color="tab:orange")
        ax2.axhline(FRESHNESS_SLA_MIN[pipe], color="red", ls="--",
                    label=f"SLA {FRESHNESS_SLA_MIN[pipe]} min")
        ax2.set_ylabel("freshness lag (min)")
        ax2.set_title("Freshness lag per run")
        ax2.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        fig.savefig(RESULTS / "charts" / f"{pipe}.png", dpi=100)
        plt.close(fig)


def main() -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ROOT / "observability.db")
    n_runs = load_db(conn)
    baseline = sql_monitors(conn)
    alerts = (check_volume_anomalies(baseline)
              + check_thresholds(conn)
              + check_schema_drift(conn))
    alerts.sort(key=lambda a: (a["run_date"], a["pipeline"], a["check"]))

    with open(RESULTS / "alerts.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["run_date", "pipeline", "check",
                                          "severity", "metric_value",
                                          "threshold", "detail"])
        w.writeheader()
        w.writerows(alerts)

    known = conn.execute(
        "SELECT COUNT(*) FROM runs WHERE incident_note != ''").fetchone()[0]
    alert_days = {(a["run_date"], a["pipeline"]) for a in alerts}
    incident_days = {(r[0], r[1]) for r in conn.execute(
        "SELECT run_date, pipeline FROM runs WHERE incident_note != ''")}
    summary = {
        "runs_monitored": n_runs,
        "pipelines": sorted({r[0] for r in conn.execute(
            "SELECT DISTINCT pipeline FROM runs")}),
        "date_range": [conn.execute("SELECT MIN(run_date) FROM runs").fetchone()[0],
                       conn.execute("SELECT MAX(run_date) FROM runs").fetchone()[0]],
        "alerts_total": len(alerts),
        "alerts_by_check": {c: sum(1 for a in alerts if a["check"] == c)
                            for c in sorted({a["check"] for a in alerts})},
        "alerts_by_severity": {s: sum(1 for a in alerts if a["severity"] == s)
                               for s in ("critical", "warning")},
        "injected_incidents": known,
        "injected_incidents_with_alert": len(incident_days & alert_days),
        "clean_days_with_alerts": len(alert_days - incident_days),
        "clean_days_total": n_runs - known,
    }
    with open(RESULTS / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    lines = ["# Incident triage report (synthetic data)\n",
             f"Monitored {n_runs} runs across {len(summary['pipelines'])} "
             f"pipelines, {summary['date_range'][0]} to "
             f"{summary['date_range'][1]}. {len(alerts)} alerts fired "
             f"({summary['alerts_by_severity']['critical']} critical, "
             f"{summary['alerts_by_severity']['warning']} warning).\n",
             "## Alerts\n"]
    for a in alerts:
        lines.append(f"- **{a['run_date']} {a['pipeline']}** [{a['severity']}] "
                     f"`{a['check']}` — {a['detail']} "
                     f"(threshold: {a['threshold']})")
    lines.append("\n## Root-cause notes (from the injected incident log)\n")
    for run_date, pipe, note in conn.execute(
            "SELECT run_date, pipeline, incident_note FROM runs "
            "WHERE incident_note != '' ORDER BY run_date"):
        fired = [a["check"] for a in alerts
                 if a["run_date"] == run_date and a["pipeline"] == pipe]
        lines.append(f"- {run_date} {pipe}: {note} — "
                     f"detected by: {', '.join(fired) or 'NOT DETECTED'}")
    (RESULTS / "incident_report.md").write_text("\n".join(lines) + "\n")

    write_charts(conn, alerts)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
