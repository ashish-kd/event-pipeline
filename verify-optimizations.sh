#!/bin/bash
# Verify Database Optimizations Script
# Confirms that the database is properly optimized after fresh deployment

set -e

echo "🔍 Verifying Database Optimizations..."
echo "=" * 50

# Wait for database to be ready
echo "⏳ Waiting for database to be ready..."
for i in {1..30}; do
    if docker compose exec postgres pg_isready -U postgres -d eventpipeline > /dev/null 2>&1; then
        echo "✅ Database is ready!"
        break
    fi
    echo "   Attempt $i/30: Database not ready yet..."
    sleep 2
done

# Check if database is responsive
if ! docker compose exec postgres pg_isready -U postgres -d eventpipeline > /dev/null 2>&1; then
    echo "❌ Database is not ready after 60 seconds"
    exit 1
fi

echo ""
echo "📊 Checking Database Optimizations..."

# 1. Check total number of indexes
TOTAL_INDEXES=$(docker compose exec postgres psql -U postgres -d eventpipeline -t -c "
SELECT COUNT(*) 
FROM pg_indexes 
WHERE tablename IN ('signals', 'anomalies', 'outbox_events');
" | tr -d ' ')

echo "📈 Total Indexes: $TOTAL_INDEXES (expected: 27)"

if [ "$TOTAL_INDEXES" -ge 25 ]; then
    echo "✅ Index count looks good!"
else
    echo "⚠️  Index count is lower than expected"
fi

# 2. Check for specific GIN indexes
echo ""
echo "🔍 Checking GIN Indexes..."

GIN_INDEXES=$(docker compose exec postgres psql -U postgres -d eventpipeline -t -c "
SELECT COUNT(*) 
FROM pg_indexes 
WHERE indexdef LIKE '%USING gin%' 
  AND tablename IN ('signals', 'anomalies');
" | tr -d ' ')

echo "📈 GIN Indexes: $GIN_INDEXES (expected: 4)"

if [ "$GIN_INDEXES" -ge 4 ]; then
    echo "✅ GIN indexes are present!"
else
    echo "⚠️  Some GIN indexes may be missing"
fi

# 3. Check for performance monitoring views
echo ""
echo "📋 Checking Performance Monitoring Views..."

VIEWS_COUNT=$(docker compose exec postgres psql -U postgres -d eventpipeline -t -c "
SELECT COUNT(*) 
FROM information_schema.views 
WHERE table_name IN ('table_performance_stats', 'index_performance_stats');
" | tr -d ' ')

echo "📈 Monitoring Views: $VIEWS_COUNT (expected: 2)"

if [ "$VIEWS_COUNT" -eq 2 ]; then
    echo "✅ Performance monitoring views are available!"
else
    echo "⚠️  Some monitoring views may be missing"
fi

# 4. Check for maintenance functions
echo ""
echo "🔧 Checking Maintenance Functions..."

FUNCTIONS_COUNT=$(docker compose exec postgres psql -U postgres -d eventpipeline -t -c "
SELECT COUNT(*) 
FROM information_schema.routines 
WHERE routine_name IN ('refresh_table_stats', 'get_table_growth_stats', 'suggest_optimizations');
" | tr -d ' ')

echo "📈 Maintenance Functions: $FUNCTIONS_COUNT (expected: 3)"

if [ "$FUNCTIONS_COUNT" -eq 3 ]; then
    echo "✅ Maintenance functions are available!"
else
    echo "⚠️  Some maintenance functions may be missing"
fi

# 5. Test a sample optimized query
echo ""
echo "⚡ Testing Optimized Query Performance..."

QUERY_TIME=$(docker compose exec postgres psql -U postgres -d eventpipeline -c "
\timing on
SELECT COUNT(*) FROM signals WHERE payload ? 'session_id';
" 2>&1 | grep "Time:" | awk '{print $2}' || echo "N/A")

echo "📈 Sample JSON Query Time: $QUERY_TIME"

# 6. Show table statistics
echo ""
echo "📊 Database Statistics:"
docker compose exec postgres psql -U postgres -d eventpipeline -c "
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as total_size,
    (SELECT COUNT(*) FROM pg_indexes WHERE tablename = t.tablename) as index_count
FROM (VALUES ('signals'), ('anomalies'), ('outbox_events')) AS t(tablename);
"

# 7. Summary
echo ""
echo "=" * 50
echo "🎯 Optimization Verification Summary:"
echo "   • Total Indexes: $TOTAL_INDEXES"
echo "   • GIN Indexes: $GIN_INDEXES" 
echo "   • Monitoring Views: $VIEWS_COUNT"
echo "   • Maintenance Functions: $FUNCTIONS_COUNT"

if [ "$TOTAL_INDEXES" -ge 25 ] && [ "$GIN_INDEXES" -ge 4 ] && [ "$VIEWS_COUNT" -eq 2 ] && [ "$FUNCTIONS_COUNT" -eq 3 ]; then
    echo ""
    echo "🎉 SUCCESS: Database is fully optimized!"
    echo "✅ All performance optimizations are in place"
    echo "🚀 Ready for high-performance production workloads"
    exit 0
else
    echo ""
    echo "⚠️  WARNING: Some optimizations may be missing"
    echo "🔧 Consider running: python3 optimize-database.py"
    exit 1
fi
