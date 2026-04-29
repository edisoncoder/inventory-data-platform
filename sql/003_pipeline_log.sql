CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT,
    rows_processed INTEGER,
    error_message TEXT
);