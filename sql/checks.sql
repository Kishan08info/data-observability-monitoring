-- Data tests for the observability monitor. Every check must return
-- 0 violations. Run: sqlite3 observability.db < sql/checks.sql
-- 1. One run per pipeline per day (no duplicate runs recorded).
SELECT 'duplicate_runs' AS check_name, COUNT(*) AS violations
FROM (SELECT run_date, pipeline, COUNT(*) c FROM runs
      GROUP BY run_date, pipeline HAVING c > 1);
-- 2. No negative metrics anywhere.
SELECT 'negative_metrics' AS check_name, COUNT(*) AS violations
FROM runs
WHERE source_rows < 0 OR rows_loaded < 0 OR duration_min < 0
   OR freshness_lag_min < 0 OR null_rate_pct < 0 OR duplicate_rate_pct < 0;
-- 3. Rows loaded never exceeds rows in source (loads are subsets).
SELECT 'loaded_exceeds_source' AS check_name, COUNT(*) AS violations
FROM runs WHERE rows_loaded > source_rows;
-- 4. Every pipeline has exactly 60 runs (one per day, 2026-09-08–2026-11-06).
SELECT 'run_count_per_pipeline' AS check_name, COUNT(*) AS violations
FROM (SELECT pipeline, COUNT(*) c FROM runs GROUP BY pipeline HAVING c != 60);
-- 5. All five injected incidents are present in the data.
SELECT 'injected_incidents_present' AS check_name,
       (5 - COUNT(*)) AS violations
FROM runs WHERE incident_note != '';
-- 6. Schema is non-empty for every run.
SELECT 'empty_schema' AS check_name, COUNT(*) AS violations
FROM runs WHERE schema_columns IS NULL OR TRIM(schema_columns) = '';
-- 7. Freshness lag is recorded for every run.
SELECT 'missing_freshness' AS check_name, COUNT(*) AS violations
FROM runs WHERE freshness_lag_min IS NULL;
