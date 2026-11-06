-- Rolling volume baseline per pipeline, via window functions.
-- For each run, the mean and sample stddev of rows_loaded over the
-- trailing 14 days (excluding today). The Python monitor turns a
-- |z| >= 3 deviation from this baseline into a volume_anomaly alert.
-- SQLite has no STDDEV aggregate, so it is computed from the windowed
-- sums: sqrt((sum(x^2) - n*mean^2) / (n - 1)).
WITH windowed AS (
    SELECT
        run_date,
        pipeline,
        rows_loaded,
        COUNT(*) OVER w AS n_prior,
        AVG(rows_loaded) OVER w AS prior_mean,
        SUM(rows_loaded * rows_loaded) OVER w AS prior_sum_sq,
        SUM(rows_loaded) OVER w AS prior_sum
    FROM runs
    WINDOW w AS (
        PARTITION BY pipeline
        ORDER BY run_date
        ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
    )
)
SELECT
    run_date,
    pipeline,
    rows_loaded,
    n_prior,
    prior_mean,
    CASE
        WHEN n_prior > 1 THEN
            SQRT(MAX(0.0, prior_sum_sq - prior_sum * prior_sum / n_prior)
                 / (n_prior - 1))
        ELSE NULL
    END AS prior_std
FROM windowed
ORDER BY run_date, pipeline;
