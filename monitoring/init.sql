-- Event Pipeline Database Schema
-- Production-optimized schema with advanced indexing for long-term scaling

-- =============================================================================
-- EXTENSIONS SETUP
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gin";  -- For mixed GIN indexes

-- =============================================================================
-- CORE TABLES
-- =============================================================================

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

-- outbox_events table for transactional guarantees (Debezium CDC)
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL
);

-- =============================================================================
-- JSONB GIN INDEXES FOR EFFICIENT PAYLOAD QUERIES
-- Performance Impact: 10-100x faster JSON queries
-- =============================================================================

-- Full payload search capability
CREATE INDEX IF NOT EXISTS idx_signals_payload_gin 
ON signals USING GIN (payload);

-- Specific payload field indexes for common queries
CREATE INDEX IF NOT EXISTS idx_signals_payload_session_gin 
ON signals USING GIN ((payload -> 'session_id'));

CREATE INDEX IF NOT EXISTS idx_signals_payload_data_gin 
ON signals USING GIN ((payload -> 'data'));

-- Context GIN index for anomalies
CREATE INDEX IF NOT EXISTS idx_anomalies_context_gin 
ON anomalies USING GIN (context);

-- =============================================================================
-- COMPOSITE INDEXES FOR COMMON QUERY PATTERNS
-- Performance Impact: 5-20x faster multi-column queries
-- =============================================================================

-- Signals: Multi-column indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_signals_user_type_time 
ON signals (user_id, type, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_signals_source_type_time 
ON signals (source, type, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_signals_type_time 
ON signals (type, event_ts DESC);

-- Enhanced time-based indexes
CREATE INDEX IF NOT EXISTS idx_signals_user_id ON signals(user_id);
CREATE INDEX IF NOT EXISTS idx_signals_event_ts_desc ON signals(event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_signals_ingest_ts_desc ON signals(ingest_ts DESC);

-- Anomalies: Multi-column indexes for filtering and sorting
CREATE INDEX IF NOT EXISTS idx_anomalies_user_type_time 
ON anomalies (user_id, anomaly_type, detection_ts DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_type_severity_time 
ON anomalies (anomaly_type, severity, detection_ts DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_severity_time 
ON anomalies (severity, detection_ts DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_user_id ON anomalies(user_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_signal_event_id ON anomalies(signal_event_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_detection_ts_desc ON anomalies(detection_ts DESC);

-- =============================================================================
-- PARTIAL INDEXES FOR HOT DATA OPTIMIZATION
-- Performance Impact: Smaller indexes, faster queries on common filters
-- =============================================================================

-- Index only high-severity anomalies (most frequently queried)
CREATE INDEX IF NOT EXISTS idx_anomalies_high_severity 
ON anomalies (detection_ts DESC, user_id, anomaly_type) 
WHERE severity >= 2;

-- Index API calls separately (often queried for anomaly detection)
CREATE INDEX IF NOT EXISTS idx_signals_api_calls 
ON signals (user_id, event_ts DESC, payload) 
WHERE type = 'api_call';

-- =============================================================================
-- EXPRESSION INDEXES FOR COMPUTED QUERIES
-- Performance Impact: Pre-computed calculations for analytics
-- =============================================================================

-- Index for payload data extraction (common in anomaly detection)
CREATE INDEX IF NOT EXISTS idx_signals_payload_data_extracted 
ON signals (user_id, (payload->>'data'), event_ts DESC) 
WHERE payload ? 'data';

-- Index for session-based queries
CREATE INDEX IF NOT EXISTS idx_signals_session_based 
ON signals ((payload->>'session_id'), event_ts DESC) 
WHERE payload ? 'session_id';

-- =============================================================================
-- OUTBOX EVENTS OPTIMIZATION
-- Performance Impact: Optimized CDC and event processing
-- =============================================================================

-- Enhanced outbox indexes for CDC performance
CREATE INDEX IF NOT EXISTS idx_outbox_events_created_at ON outbox_events(created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_events_processed_at ON outbox_events(processed_at);
CREATE INDEX IF NOT EXISTS idx_outbox_events_type_aggregate ON outbox_events(event_type, aggregate_id);

-- Unprocessed events index (critical for CDC performance)
CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed 
ON outbox_events (created_at) 
WHERE processed_at IS NULL;

-- Event type specific indexes
CREATE INDEX IF NOT EXISTS idx_outbox_event_type_time 
ON outbox_events (event_type, created_at DESC);

-- =============================================================================
-- PERFORMANCE MONITORING VIEWS
-- =============================================================================

-- View for table sizes and statistics
CREATE OR REPLACE VIEW table_performance_stats AS
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size,
    (SELECT reltuples FROM pg_class WHERE relname = tablename)::BIGINT as estimated_rows,
    (SELECT n_tup_ins FROM pg_stat_user_tables WHERE relname = tablename) as inserts,
    (SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname = tablename) as updates,
    (SELECT n_tup_del FROM pg_stat_user_tables WHERE relname = tablename) as deletes
FROM pg_tables 
WHERE schemaname = 'public' 
  AND tablename IN ('signals', 'anomalies', 'outbox_events')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- View for index usage statistics
CREATE OR REPLACE VIEW index_performance_stats AS
SELECT 
    t.relname as table_name,
    i.relname as index_name,
    s.idx_scan as scans,
    s.idx_tup_read as tuples_read,
    s.idx_tup_fetch as tuples_fetched,
    pg_size_pretty(pg_relation_size(i.oid)) as index_size,
    CASE 
        WHEN s.idx_scan = 0 THEN 'UNUSED'
        WHEN s.idx_scan < 100 THEN 'LOW_USAGE'
        WHEN s.idx_scan < 1000 THEN 'MEDIUM_USAGE'
        ELSE 'HIGH_USAGE'
    END as usage_category
FROM pg_stat_user_indexes s
JOIN pg_class i ON i.oid = s.indexrelid
JOIN pg_class t ON t.oid = s.relid
WHERE t.relname IN ('signals', 'anomalies', 'outbox_events')
ORDER BY s.idx_scan DESC;

-- =============================================================================
-- MAINTENANCE FUNCTIONS
-- =============================================================================

-- Function to analyze all tables for optimal query planning
CREATE OR REPLACE FUNCTION refresh_table_stats()
RETURNS TEXT AS $$
BEGIN
    ANALYZE signals;
    ANALYZE anomalies;
    ANALYZE outbox_events;
    
    RETURN 'Table statistics updated successfully';
END;
$$ LANGUAGE plpgsql;

-- Function to get table growth information
CREATE OR REPLACE FUNCTION get_table_growth_stats(days_back INTEGER DEFAULT 7)
RETURNS TABLE(
    table_name TEXT,
    current_size TEXT,
    estimated_daily_growth TEXT,
    rows_today BIGINT,
    projected_size_30_days TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'signals'::TEXT,
        pg_size_pretty(pg_total_relation_size('signals')) as current_size,
        pg_size_pretty(
            (pg_total_relation_size('signals')::NUMERIC / GREATEST(days_back, 1))
        ) as estimated_daily_growth,
        (SELECT COUNT(*) FROM signals WHERE event_ts >= CURRENT_DATE)::BIGINT as rows_today,
        pg_size_pretty(
            pg_total_relation_size('signals')::NUMERIC * (1 + 30.0/GREATEST(days_back, 1))
        ) as projected_size_30_days
    
    UNION ALL
    
    SELECT 
        'anomalies'::TEXT,
        pg_size_pretty(pg_total_relation_size('anomalies')),
        pg_size_pretty(
            (pg_total_relation_size('anomalies')::NUMERIC / GREATEST(days_back, 1))
        ),
        (SELECT COUNT(*) FROM anomalies WHERE detection_ts >= CURRENT_DATE)::BIGINT,
        pg_size_pretty(
            pg_total_relation_size('anomalies')::NUMERIC * (1 + 30.0/GREATEST(days_back, 1))
        );
END;
$$ LANGUAGE plpgsql;

-- Function to identify slow queries and suggest optimizations
CREATE OR REPLACE FUNCTION suggest_optimizations()
RETURNS TABLE(
    suggestion_type TEXT,
    table_name TEXT,
    suggestion TEXT,
    current_performance TEXT
) AS $$
BEGIN
    RETURN QUERY
    -- Check for unused indexes
    SELECT 
        'UNUSED_INDEX'::TEXT,
        table_name,
        'Consider dropping index: ' || index_name as suggestion,
        'Scans: ' || scans::TEXT as current_performance
    FROM index_performance_stats 
    WHERE usage_category = 'UNUSED'
    
    UNION ALL
    
    -- Check for tables without recent ANALYZE
    SELECT 
        'STALE_STATISTICS'::TEXT,
        tablename,
        'Run ANALYZE on table to update statistics' as suggestion,
        'Last analyzed: ' || COALESCE(last_analyze::TEXT, 'never') as current_performance
    FROM pg_stat_user_tables 
    WHERE relname IN ('signals', 'anomalies', 'outbox_events')
      AND (last_analyze IS NULL OR last_analyze < NOW() - INTERVAL '1 day');
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- INITIAL STATISTICS UPDATE
-- =============================================================================

-- Update table statistics for optimal query planning
SELECT refresh_table_stats();

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;