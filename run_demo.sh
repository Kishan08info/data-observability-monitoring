#!/bin/bash
# End-to-end demo: generate 60 days of synthetic pipeline-run history ->
# run the observability monitors -> write alerts, incident report, charts.
# Not a production scheduler — just a shell sequence for the demo.
set -e
cd "$(dirname "$0")"
python3 src/generate.py
python3 src/monitor.py
python3 -m unittest discover -s tests -v
sqlite3 observability.db < sql/checks.sql
echo "Demo complete. Alerts in results/alerts.csv, report in results/incident_report.md"
