CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.load_history (
    id           SERIAL PRIMARY KEY,
    table_name   TEXT        NOT NULL,
    partition_dt TEXT,
    file_name    TEXT,
    checksum     TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    status       TEXT        NOT NULL DEFAULT 'running',
    rows_loaded  INTEGER,
    error_msg    TEXT
);
