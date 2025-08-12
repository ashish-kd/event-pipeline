-- Event Pipeline Database Schema
-- Minimal schema per checklist specification

-- signals table per specification
CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY,
    user_id TEXT,
    source TEXT,
    type TEXT,
    event_ts TIMESTAMPTZ,
    ingest_ts TIMESTAMPTZ,
    payload JSONB
);

-- anomalies table per specification
CREATE TABLE IF NOT EXISTS anomalies (
    id UUID PRIMARY KEY,
    user_id TEXT,
    anomaly_type TEXT,
    severity INT,
    detection_ts TIMESTAMPTZ,
    signal_event_id UUID REFERENCES signals(id),
    context JSONB
);

-- Essential indexes only
CREATE INDEX IF NOT EXISTS idx_signals_user_id ON signals(user_id);
CREATE INDEX IF NOT EXISTS idx_signals_event_ts ON signals(event_ts);
CREATE INDEX IF NOT EXISTS idx_anomalies_user_id ON anomalies(user_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_signal_event_id ON anomalies(signal_event_id);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;