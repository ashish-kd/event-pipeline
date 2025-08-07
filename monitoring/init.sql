-- Event Pipeline Database Initialization Script
-- This script creates the database schema and sets up initial indexes

-- Create signals table with proper indexing for high-performance queries
CREATE TABLE IF NOT EXISTS signals (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    type VARCHAR(50) NOT NULL,
    timestamp BIGINT NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for signals table to optimize queries
CREATE INDEX IF NOT EXISTS idx_signals_user_id ON signals(user_id);
CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(type);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_processed_at ON signals(processed_at);
CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);
CREATE INDEX IF NOT EXISTS idx_signals_payload_gin ON signals USING GIN(payload);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_signals_user_type ON signals(user_id, type);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp_type ON signals(timestamp, type);
CREATE INDEX IF NOT EXISTS idx_signals_user_timestamp ON signals(user_id, timestamp);

-- Create anomalies table for detected anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    anomaly_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
    description TEXT NOT NULL,
    original_signal_data JSONB,
    detection_timestamp BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for anomalies table
CREATE INDEX IF NOT EXISTS idx_anomalies_user_id ON anomalies(user_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies(anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_detection_timestamp ON anomalies(detection_timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_created_at ON anomalies(created_at);

-- Composite indexes for anomalies
CREATE INDEX IF NOT EXISTS idx_anomalies_user_severity ON anomalies(user_id, severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_type_severity ON anomalies(anomaly_type, severity);

-- Create processing_stats table for monitoring signal processing performance
CREATE TABLE IF NOT EXISTS processing_stats (
    id BIGSERIAL PRIMARY KEY,
    batch_size INTEGER NOT NULL,
    processing_time_ms INTEGER NOT NULL,
    events_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for processing stats
CREATE INDEX IF NOT EXISTS idx_processing_stats_created_at ON processing_stats(created_at);

-- Create failed_events table for dead letter queue functionality
CREATE TABLE IF NOT EXISTS failed_events (
    id BIGSERIAL PRIMARY KEY,
    original_message JSONB NOT NULL,
    error_reason TEXT,
    failed_at TIMESTAMP DEFAULT NOW(),
    retry_count INTEGER DEFAULT 0
);

-- Index for failed events
CREATE INDEX IF NOT EXISTS idx_failed_events_failed_at ON failed_events(failed_at);
CREATE INDEX IF NOT EXISTS idx_failed_events_retry_count ON failed_events(retry_count);

-- Create a view for recent signal statistics
CREATE OR REPLACE VIEW recent_signal_stats AS
SELECT 
    type,
    COUNT(*) as signal_count,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(EXTRACT(EPOCH FROM (processed_at - created_at))) as avg_processing_delay_seconds
FROM signals 
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY type
ORDER BY signal_count DESC;

-- Create a view for recent anomaly statistics
CREATE OR REPLACE VIEW recent_anomaly_stats AS
SELECT 
    anomaly_type,
    severity,
    COUNT(*) as anomaly_count,
    COUNT(DISTINCT user_id) as affected_users
FROM anomalies 
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY anomaly_type, severity
ORDER BY anomaly_count DESC;

-- Create a view for user activity summary
CREATE OR REPLACE VIEW user_activity_summary AS
SELECT 
    s.user_id,
    COUNT(s.id) as total_signals,
    COUNT(DISTINCT s.type) as signal_types,
    COUNT(a.id) as total_anomalies,
    MAX(s.timestamp) as last_signal_timestamp,
    MAX(a.detection_timestamp) as last_anomaly_timestamp
FROM signals s
LEFT JOIN anomalies a ON s.user_id = a.user_id
WHERE s.created_at > NOW() - INTERVAL '24 hours'
GROUP BY s.user_id
ORDER BY total_signals DESC;

-- Insert some sample configuration data if tables are empty
DO $$
BEGIN
    -- Check if this is a fresh installation
    IF NOT EXISTS (SELECT 1 FROM signals LIMIT 1) THEN
        -- Create a system info table for tracking database version and setup
        CREATE TABLE IF NOT EXISTS system_info (
            key VARCHAR(100) PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        
        INSERT INTO system_info (key, value) VALUES 
            ('schema_version', '1.0.0'),
            ('initialized_at', NOW()::text),
            ('description', 'Event Pipeline Database - Production Ready Schema');
            
        RAISE NOTICE 'Event Pipeline database schema initialized successfully';
    END IF;
END $$;

-- Create stored procedures for common operations
CREATE OR REPLACE FUNCTION get_user_risk_score(user_id_param INTEGER)
RETURNS TABLE(
    user_id INTEGER,
    total_anomalies BIGINT,
    high_severity_anomalies BIGINT,
    recent_anomalies BIGINT,
    risk_score NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        user_id_param,
        COUNT(a.id) as total_anomalies,
        COUNT(CASE WHEN a.severity = 'high' THEN 1 END) as high_severity_anomalies,
        COUNT(CASE WHEN a.created_at > NOW() - INTERVAL '24 hours' THEN 1 END) as recent_anomalies,
        CASE 
            WHEN COUNT(a.id) = 0 THEN 0
            ELSE (
                COUNT(CASE WHEN a.severity = 'high' THEN 1 END) * 3 +
                COUNT(CASE WHEN a.severity = 'medium' THEN 1 END) * 2 +
                COUNT(CASE WHEN a.severity = 'low' THEN 1 END) * 1 +
                COUNT(CASE WHEN a.created_at > NOW() - INTERVAL '24 hours' THEN 1 END) * 2
            )::NUMERIC / GREATEST(COUNT(a.id), 1)
        END as risk_score
    FROM anomalies a
    WHERE a.user_id = user_id_param;
END;
$$ LANGUAGE plpgsql;

-- Create function to clean old data (for maintenance)
CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INTEGER DEFAULT 30)
RETURNS TABLE(
    signals_deleted BIGINT,
    anomalies_deleted BIGINT,
    processing_stats_deleted BIGINT,
    failed_events_deleted BIGINT
) AS $$
DECLARE
    cutoff_date TIMESTAMP;
    signals_count BIGINT;
    anomalies_count BIGINT;
    stats_count BIGINT;
    failed_count BIGINT;
BEGIN
    cutoff_date := NOW() - INTERVAL '1 day' * days_to_keep;
    
    -- Delete old records and count them
    WITH deleted_signals AS (
        DELETE FROM signals WHERE created_at < cutoff_date RETURNING id
    )
    SELECT COUNT(*) INTO signals_count FROM deleted_signals;
    
    WITH deleted_anomalies AS (
        DELETE FROM anomalies WHERE created_at < cutoff_date RETURNING id
    )
    SELECT COUNT(*) INTO anomalies_count FROM deleted_anomalies;
    
    WITH deleted_stats AS (
        DELETE FROM processing_stats WHERE created_at < cutoff_date RETURNING id
    )
    SELECT COUNT(*) INTO stats_count FROM deleted_stats;
    
    WITH deleted_failed AS (
        DELETE FROM failed_events WHERE failed_at < cutoff_date RETURNING id
    )
    SELECT COUNT(*) INTO failed_count FROM deleted_failed;
    
    RETURN QUERY SELECT signals_count, anomalies_count, stats_count, failed_count;
END;
$$ LANGUAGE plpgsql;

-- Grant appropriate permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;

-- Create indexes on frequently accessed columns for better performance
-- These will help with time-based queries which are common in this system
CREATE INDEX IF NOT EXISTS idx_signals_created_at_desc ON signals(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_created_at_desc ON anomalies(created_at DESC);

-- Enable auto-vacuum for better performance
ALTER TABLE signals SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_analyze_scale_factor = 0.05);
ALTER TABLE anomalies SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_analyze_scale_factor = 0.05);

COMMIT; 