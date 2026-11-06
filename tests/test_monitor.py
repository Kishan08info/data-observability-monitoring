#!/usr/bin/env python3
"""Tests for the observability monitor (stdlib unittest, no extra deps).

Run from the project root:  python3 -m unittest discover -s tests -v
"""
import csv
import json
import sqlite3
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
import monitor  # noqa: E402


class TestMonitor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run([sys.executable, str(ROOT / "src" / "generate.py")],
                       check=True, cwd=ROOT, capture_output=True)
        subprocess.run([sys.executable, str(ROOT / "src" / "monitor.py")],
                       check=True, cwd=ROOT, capture_output=True)
        cls.conn = sqlite3.connect(ROOT / "observability.db")
        with open(ROOT / "results" / "alerts.csv") as f:
            cls.alerts = list(csv.DictReader(f))
        cls.summary = json.loads(
            (ROOT / "results" / "summary.json").read_text())

    def test_all_injected_incidents_detected(self):
        """Every injected incident day must fire at least one alert."""
        incident_days = {(r[0], r[1]) for r in self.conn.execute(
            "SELECT run_date, pipeline FROM runs WHERE incident_note != ''")}
        alert_days = {(a["run_date"], a["pipeline"]) for a in self.alerts}
        self.assertEqual(incident_days - alert_days, set(),
                         "undetected incidents")

    def test_volume_collapse_is_volume_anomaly(self):
        hits = [a for a in self.alerts
                if a["run_date"] == "2026-10-19"
                and a["pipeline"] == "orders"
                and a["check"] == "volume_anomaly"]
        self.assertEqual(len(hits), 1)
        self.assertLess(float(hits[0]["metric_value"]), -3.0)

    def test_freshness_breach_is_critical(self):
        hits = [a for a in self.alerts
                if a["run_date"] == "2026-10-30"
                and a["check"] == "freshness_sla"]
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["severity"], "critical")
        self.assertEqual(float(hits[0]["metric_value"]), 320.0)

    def test_schema_drift_detected(self):
        hits = [a for a in self.alerts
                if a["run_date"] == "2026-10-11"
                and a["check"] == "schema_drift"]
        self.assertEqual(len(hits), 1)
        self.assertIn("payment_method", hits[0]["detail"])

    def test_no_alerts_on_clean_days(self):
        """Alerts fire only on the 5 injected incident days."""
        self.assertEqual(self.summary["clean_days_with_alerts"], 0)

    def test_reconciliation_matches_source(self):
        """On clean days loaded == source (no silent row loss)."""
        bad = self.conn.execute(
            "SELECT COUNT(*) FROM runs WHERE incident_note = '' "
            "AND rows_loaded != source_rows").fetchone()[0]
        self.assertEqual(bad, 0)

    def test_sql_checks_all_pass(self):
        out = subprocess.run(
            ["sqlite3", str(ROOT / "observability.db")],
            input=(ROOT / "sql" / "checks.sql").read_text(),
            capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(out.returncode, 0, out.stderr)
        violations = [int(line.rsplit("|", 1)[1])
                      for line in out.stdout.strip().splitlines()]
        self.assertEqual(len(violations), 7)
        self.assertEqual(sum(violations), 0, out.stdout)


if __name__ == "__main__":
    unittest.main()
