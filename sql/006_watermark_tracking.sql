CREATE SCHEMA IF NOT EXISTS control;

-- DROP TABLE IF EXISTS control.watermarks;

CREATE TABLE IF NOT EXISTS control.watermarks (
    watermark_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    last_processed_id BIGINT DEFAULT 0,
    last_processed_timestamp TIMESTAMP DEFAULT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active',
    CONSTRAINT chk_status CHECK (status IN ('active', 'paused', 'archived'))
);

-- Initialize watermark for supply_chain_data table
INSERT INTO control.watermarks (table_name, last_processed_id, status)
VALUES ('supply_chain_data', 0, 'active')
ON CONFLICT (table_name) DO NOTHING;

-- Create index for efficient lookups by table_name
CREATE INDEX IF NOT EXISTS idx_watermarks_table_name 
ON control.watermarks(table_name);

-- Create audit trigger to track updates (optional, for future compliance)
CREATE OR REPLACE FUNCTION control.update_watermark_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_watermark_updated_at
BEFORE UPDATE ON control.watermarks
FOR EACH ROW
EXECUTE FUNCTION control.update_watermark_timestamp();

-- Grant permissions (adjust user as needed)
-- GRANT SELECT, INSERT, UPDATE ON control.watermarks TO app_user;
-- GRANT USAGE ON SCHEMA control TO app_user;