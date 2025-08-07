import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from enum import Enum

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Path, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, start_http_server
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
api_requests = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method', 'status_code'])
request_duration = Histogram('api_request_duration_seconds', 'API request duration', ['endpoint'])

# Pydantic models
class SignalType(str, Enum):
    login = "login"
    purchase = "purchase"
    click = "click"
    api_call = "api_call"
    file_access = "file_access"

class AnomalyType(str, Enum):
    high_frequency_login = "high_frequency_login"
    repeated_login_failures = "repeated_login_failures"
    unusual_location_login = "unusual_location_login"
    high_value_purchase = "high_value_purchase"
    rapid_successive_purchases = "rapid_successive_purchases"
    high_api_failure_rate = "high_api_failure_rate"
    unusual_time_activity = "unusual_time_activity"

class SeverityLevel(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"

class Signal(BaseModel):
    id: int
    user_id: int
    type: str
    timestamp: int
    payload: dict
    processed_at: datetime
    created_at: datetime

class Anomaly(BaseModel):
    id: int
    user_id: int
    anomaly_type: str
    severity: str
    description: str
    original_signal_data: Optional[dict]
    detection_timestamp: int
    created_at: datetime

class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1, description="Page number")
    size: int = Field(default=50, ge=1, le=1000, description="Page size")

class SignalFilters(BaseModel):
    user_id: Optional[int] = None
    signal_type: Optional[SignalType] = None
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None

class AnomalyFilters(BaseModel):
    user_id: Optional[int] = None
    anomaly_type: Optional[AnomalyType] = None
    severity: Optional[SeverityLevel] = None
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None

class MetricsResponse(BaseModel):
    total_signals: int
    total_anomalies: int
    signals_by_type: Dict[str, int]
    anomalies_by_type: Dict[str, int]
    anomalies_by_severity: Dict[str, int]
    top_users_by_signals: List[Dict[str, Any]]
    top_users_by_anomalies: List[Dict[str, Any]]
    time_range: str

class HealthResponse(BaseModel):
    status: str
    service: str
    database: str
    uptime_seconds: int
    version: str

class APIService:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/eventpipeline')
        self.db_pool = None
        self.start_time = datetime.now()
        
        logger.info("API service initialized")
    
    async def init_db_pool(self):
        """Initialize database connection pool."""
        try:
            self.db_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Database connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def get_signals(
        self,
        filters: SignalFilters,
        pagination: PaginationParams
    ) -> Dict[str, Any]:
        """Get signals with filtering and pagination."""
        try:
            # Build WHERE clause
            where_conditions = []
            query_params = []
            param_count = 0
            
            if filters.user_id is not None:
                param_count += 1
                where_conditions.append(f"user_id = ${param_count}")
                query_params.append(filters.user_id)
            
            if filters.signal_type is not None:
                param_count += 1
                where_conditions.append(f"type = ${param_count}")
                query_params.append(filters.signal_type.value)
            
            if filters.start_timestamp is not None:
                param_count += 1
                where_conditions.append(f"timestamp >= ${param_count}")
                query_params.append(filters.start_timestamp)
            
            if filters.end_timestamp is not None:
                param_count += 1
                where_conditions.append(f"timestamp <= ${param_count}")
                query_params.append(filters.end_timestamp)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Calculate offset
            offset = (pagination.page - 1) * pagination.size
            
            # Count query
            count_query = f"""
                SELECT COUNT(*) as total
                FROM signals
                {where_clause}
            """
            
            # Data query
            data_query = f"""
                SELECT id, user_id, type, timestamp, payload, processed_at, created_at
                FROM signals
                {where_clause}
                ORDER BY timestamp DESC
                LIMIT ${param_count + 1} OFFSET ${param_count + 2}
            """
            
            query_params.extend([pagination.size, offset])
            
            async with self.db_pool.acquire() as conn:
                # Get total count
                total_count = await conn.fetchval(count_query, *query_params[:-2])
                
                # Get data
                rows = await conn.fetch(data_query, *query_params)
                
                signals = [
                    Signal(
                        id=row['id'],
                        user_id=row['user_id'],
                        type=row['type'],
                        timestamp=row['timestamp'],
                        payload=row['payload'],
                        processed_at=row['processed_at'],
                        created_at=row['created_at']
                    ) for row in rows
                ]
                
                total_pages = (total_count + pagination.size - 1) // pagination.size
                
                return {
                    "signals": signals,
                    "pagination": {
                        "page": pagination.page,
                        "size": pagination.size,
                        "total": total_count,
                        "total_pages": total_pages,
                        "has_next": pagination.page < total_pages,
                        "has_prev": pagination.page > 1
                    }
                }
                
        except Exception as e:
            logger.error(f"Error getting signals: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")
    
    async def get_anomalies(
        self,
        filters: AnomalyFilters,
        pagination: PaginationParams
    ) -> Dict[str, Any]:
        """Get anomalies with filtering and pagination."""
        try:
            # Build WHERE clause
            where_conditions = []
            query_params = []
            param_count = 0
            
            if filters.user_id is not None:
                param_count += 1
                where_conditions.append(f"user_id = ${param_count}")
                query_params.append(filters.user_id)
            
            if filters.anomaly_type is not None:
                param_count += 1
                where_conditions.append(f"anomaly_type = ${param_count}")
                query_params.append(filters.anomaly_type.value)
            
            if filters.severity is not None:
                param_count += 1
                where_conditions.append(f"severity = ${param_count}")
                query_params.append(filters.severity.value)
            
            if filters.start_timestamp is not None:
                param_count += 1
                where_conditions.append(f"detection_timestamp >= ${param_count}")
                query_params.append(filters.start_timestamp)
            
            if filters.end_timestamp is not None:
                param_count += 1
                where_conditions.append(f"detection_timestamp <= ${param_count}")
                query_params.append(filters.end_timestamp)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Calculate offset
            offset = (pagination.page - 1) * pagination.size
            
            # Count query
            count_query = f"""
                SELECT COUNT(*) as total
                FROM anomalies
                {where_clause}
            """
            
            # Data query
            data_query = f"""
                SELECT id, user_id, anomaly_type, severity, description, 
                       original_signal_data, detection_timestamp, created_at
                FROM anomalies
                {where_clause}
                ORDER BY detection_timestamp DESC
                LIMIT ${param_count + 1} OFFSET ${param_count + 2}
            """
            
            query_params.extend([pagination.size, offset])
            
            async with self.db_pool.acquire() as conn:
                # Get total count
                total_count = await conn.fetchval(count_query, *query_params[:-2])
                
                # Get data
                rows = await conn.fetch(data_query, *query_params)
                
                anomalies = [
                    Anomaly(
                        id=row['id'],
                        user_id=row['user_id'],
                        anomaly_type=row['anomaly_type'],
                        severity=row['severity'],
                        description=row['description'],
                        original_signal_data=row['original_signal_data'],
                        detection_timestamp=row['detection_timestamp'],
                        created_at=row['created_at']
                    ) for row in rows
                ]
                
                total_pages = (total_count + pagination.size - 1) // pagination.size
                
                return {
                    "anomalies": anomalies,
                    "pagination": {
                        "page": pagination.page,
                        "size": pagination.size,
                        "total": total_count,
                        "total_pages": total_pages,
                        "has_next": pagination.page < total_pages,
                        "has_prev": pagination.page > 1
                    }
                }
                
        except Exception as e:
            logger.error(f"Error getting anomalies: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")
    
    async def get_metrics(self, hours: int = 24) -> MetricsResponse:
        """Get comprehensive metrics for the specified time range."""
        try:
            time_threshold = datetime.now() - timedelta(hours=hours)
            
            async with self.db_pool.acquire() as conn:
                # Get total counts
                total_signals = await conn.fetchval(
                    "SELECT COUNT(*) FROM signals WHERE created_at > $1", time_threshold
                )
                
                total_anomalies = await conn.fetchval(
                    "SELECT COUNT(*) FROM anomalies WHERE created_at > $1", time_threshold
                )
                
                # Get signals by type
                signals_by_type_rows = await conn.fetch("""
                    SELECT type, COUNT(*) as count
                    FROM signals 
                    WHERE created_at > $1
                    GROUP BY type
                    ORDER BY count DESC
                """, time_threshold)
                
                signals_by_type = {row['type']: row['count'] for row in signals_by_type_rows}
                
                # Get anomalies by type
                anomalies_by_type_rows = await conn.fetch("""
                    SELECT anomaly_type, COUNT(*) as count
                    FROM anomalies 
                    WHERE created_at > $1
                    GROUP BY anomaly_type
                    ORDER BY count DESC
                """, time_threshold)
                
                anomalies_by_type = {row['anomaly_type']: row['count'] for row in anomalies_by_type_rows}
                
                # Get anomalies by severity
                anomalies_by_severity_rows = await conn.fetch("""
                    SELECT severity, COUNT(*) as count
                    FROM anomalies 
                    WHERE created_at > $1
                    GROUP BY severity
                    ORDER BY count DESC
                """, time_threshold)
                
                anomalies_by_severity = {row['severity']: row['count'] for row in anomalies_by_severity_rows}
                
                # Get top users by signals
                top_users_signals = await conn.fetch("""
                    SELECT user_id, COUNT(*) as signal_count
                    FROM signals 
                    WHERE created_at > $1
                    GROUP BY user_id
                    ORDER BY signal_count DESC
                    LIMIT 10
                """, time_threshold)
                
                # Get top users by anomalies
                top_users_anomalies = await conn.fetch("""
                    SELECT user_id, COUNT(*) as anomaly_count
                    FROM anomalies 
                    WHERE created_at > $1
                    GROUP BY user_id
                    ORDER BY anomaly_count DESC
                    LIMIT 10
                """, time_threshold)
                
                return MetricsResponse(
                    total_signals=total_signals or 0,
                    total_anomalies=total_anomalies or 0,
                    signals_by_type=signals_by_type,
                    anomalies_by_type=anomalies_by_type,
                    anomalies_by_severity=anomalies_by_severity,
                    top_users_by_signals=[dict(row) for row in top_users_signals],
                    top_users_by_anomalies=[dict(row) for row in top_users_anomalies],
                    time_range=f"last_{hours}_hours"
                )
                
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

# Initialize API service
api_service = APIService()

# FastAPI app
app = FastAPI(
    title="Event Pipeline API",
    description="REST API for querying events and metrics from the event processing pipeline",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def metrics_middleware(request, call_next):
    """Middleware to collect API metrics."""
    start_time = datetime.now()
    
    response = await call_next(request)
    
    # Record metrics
    duration = (datetime.now() - start_time).total_seconds()
    endpoint = request.url.path
    method = request.method
    status_code = str(response.status_code)
    
    api_requests.labels(endpoint=endpoint, method=method, status_code=status_code).inc()
    request_duration.labels(endpoint=endpoint).observe(duration)
    
    return response

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    db_status = "connected" if api_service.db_pool else "disconnected"
    uptime = (datetime.now() - api_service.start_time).total_seconds()
    
    return HealthResponse(
        status="healthy",
        service="event-pipeline-api",
        database=db_status,
        uptime_seconds=int(uptime),
        version="1.0.0"
    )

@app.get("/signals")
async def get_signals(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    signal_type: Optional[SignalType] = Query(None, description="Filter by signal type"),
    start_timestamp: Optional[int] = Query(None, description="Start timestamp (milliseconds)"),
    end_timestamp: Optional[int] = Query(None, description="End timestamp (milliseconds)"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """Get signals with optional filtering and pagination."""
    filters = SignalFilters(
        user_id=user_id,
        signal_type=signal_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp
    )
    pagination = PaginationParams(page=page, size=size)
    
    return await api_service.get_signals(filters, pagination)

@app.get("/anomalies")
async def get_anomalies(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    anomaly_type: Optional[AnomalyType] = Query(None, description="Filter by anomaly type"),
    severity: Optional[SeverityLevel] = Query(None, description="Filter by severity"),
    start_timestamp: Optional[int] = Query(None, description="Start timestamp (milliseconds)"),
    end_timestamp: Optional[int] = Query(None, description="End timestamp (milliseconds)"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """Get anomalies with optional filtering and pagination."""
    filters = AnomalyFilters(
        user_id=user_id,
        anomaly_type=anomaly_type,
        severity=severity,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp
    )
    pagination = PaginationParams(page=page, size=size)
    
    return await api_service.get_anomalies(filters, pagination)

@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics(
    hours: int = Query(24, ge=1, le=168, description="Time range in hours (max 1 week)")
):
    """Get comprehensive metrics for the specified time range."""
    return await api_service.get_metrics(hours)

@app.get("/users/{user_id}/signals")
async def get_user_signals(
    user_id: int = Path(..., description="User ID"),
    signal_type: Optional[SignalType] = Query(None, description="Filter by signal type"),
    start_timestamp: Optional[int] = Query(None, description="Start timestamp (milliseconds)"),
    end_timestamp: Optional[int] = Query(None, description="End timestamp (milliseconds)"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """Get signals for a specific user."""
    filters = SignalFilters(
        user_id=user_id,
        signal_type=signal_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp
    )
    pagination = PaginationParams(page=page, size=size)
    
    return await api_service.get_signals(filters, pagination)

@app.get("/users/{user_id}/anomalies")
async def get_user_anomalies(
    user_id: int = Path(..., description="User ID"),
    anomaly_type: Optional[AnomalyType] = Query(None, description="Filter by anomaly type"),
    severity: Optional[SeverityLevel] = Query(None, description="Filter by severity"),
    start_timestamp: Optional[int] = Query(None, description="Start timestamp (milliseconds)"),
    end_timestamp: Optional[int] = Query(None, description="End timestamp (milliseconds)"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(50, ge=1, le=1000, description="Page size")
):
    """Get anomalies for a specific user."""
    filters = AnomalyFilters(
        user_id=user_id,
        anomaly_type=anomaly_type,
        severity=severity,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp
    )
    pagination = PaginationParams(page=page, size=size)
    
    return await api_service.get_anomalies(filters, pagination)

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    # Start Prometheus metrics server
    start_http_server(8001)
    logger.info("Prometheus metrics server started on port 8001")
    
    # Initialize database pool
    await api_service.init_db_pool()

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    if api_service.db_pool:
        await api_service.db_pool.close()
    logger.info("API service shutdown complete")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 