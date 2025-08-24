#!/usr/bin/env python3
"""
Structured Logging Configuration
Centralized logging with correlation IDs and JSON output for the event pipeline
"""

import os
import sys
import uuid
import time
from typing import Dict, Any, Optional
from contextvars import ContextVar
from functools import wraps

import structlog
from structlog.stdlib import LoggerFactory
from structlog.processors import JSONRenderer
from fastapi import Request
import asyncio

# Context variables for correlation tracking
correlation_id_var: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)
user_id_var: ContextVar[Optional[str]] = ContextVar('user_id', default=None)
request_id_var: ContextVar[Optional[str]] = ContextVar('request_id', default=None)

class CorrelationProcessor:
    """Add correlation context to log records."""
    
    def __call__(self, logger, method_name, event_dict):
        # Add correlation ID if available
        correlation_id = correlation_id_var.get()
        if correlation_id:
            event_dict['correlation_id'] = correlation_id
        
        # Add user ID if available
        user_id = user_id_var.get()
        if user_id:
            event_dict['user_id'] = user_id
        
        # Add request ID if available
        request_id = request_id_var.get()
        if request_id:
            event_dict['request_id'] = request_id
        
        # Add service information
        event_dict['service'] = os.getenv('SERVICE_NAME', 'event-pipeline')
        event_dict['version'] = os.getenv('SERVICE_VERSION', '1.0.0')
        event_dict['environment'] = os.getenv('ENVIRONMENT', 'development')
        
        return event_dict

class PerformanceProcessor:
    """Add performance metrics to log records."""
    
    def __call__(self, logger, method_name, event_dict):
        # Add timestamp with high precision
        event_dict['timestamp_ms'] = int(time.time() * 1000)
        
        # Add memory usage if available
        try:
            import psutil
            process = psutil.Process()
            event_dict['memory_mb'] = round(process.memory_info().rss / 1024 / 1024, 2)
            event_dict['cpu_percent'] = round(process.cpu_percent(), 2)
        except ImportError:
            pass
        
        return event_dict

class SanitizationProcessor:
    """Remove sensitive data from logs."""
    
    SENSITIVE_KEYS = {
        'password', 'token', 'secret', 'key', 'auth', 'credential',
        'api_key', 'access_token', 'refresh_token', 'jwt'
    }
    
    def __call__(self, logger, method_name, event_dict):
        return self._sanitize_dict(event_dict)
    
    def _sanitize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively sanitize sensitive data."""
        if not isinstance(data, dict):
            return data
        
        sanitized = {}
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in self.SENSITIVE_KEYS):
                sanitized[key] = '[REDACTED]'
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_dict(value)
            elif isinstance(value, list):
                sanitized[key] = [self._sanitize_dict(item) if isinstance(item, dict) else item for item in value]
            else:
                sanitized[key] = value
        
        return sanitized

def configure_structured_logging(service_name: str = None, log_level: str = None):
    """Configure structured logging for the service."""
    
    # Set service name
    if service_name:
        os.environ['SERVICE_NAME'] = service_name
    
    # Configure log level
    log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            CorrelationProcessor(),
            PerformanceProcessor(),
            SanitizationProcessor(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    import logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )
    
    # Create logger instance
    logger = structlog.get_logger()
    logger.info("Structured logging configured", 
                service=service_name, 
                log_level=log_level,
                correlation_enabled=True)
    
    return logger

def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())

def set_correlation_context(correlation_id: str = None, user_id: str = None, request_id: str = None):
    """Set correlation context for current execution."""
    if correlation_id:
        correlation_id_var.set(correlation_id)
    if user_id:
        user_id_var.set(user_id)
    if request_id:
        request_id_var.set(request_id)

def get_correlation_context() -> Dict[str, Optional[str]]:
    """Get current correlation context."""
    return {
        'correlation_id': correlation_id_var.get(),
        'user_id': user_id_var.get(),
        'request_id': request_id_var.get()
    }

def with_correlation(correlation_id: str = None):
    """Decorator to add correlation context to function execution."""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            cid = correlation_id or generate_correlation_id()
            correlation_id_var.set(cid)
            try:
                return await func(*args, **kwargs)
            finally:
                correlation_id_var.set(None)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            cid = correlation_id or generate_correlation_id()
            correlation_id_var.set(cid)
            try:
                return func(*args, **kwargs)
            finally:
                correlation_id_var.set(None)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

async def logging_middleware(request: Request, call_next):
    """Middleware to add correlation tracking to FastAPI requests."""
    
    # Generate or extract correlation ID
    correlation_id = request.headers.get('X-Correlation-ID') or generate_correlation_id()
    request_id = str(uuid.uuid4())
    
    # Extract user ID from JWT if available
    user_id = None
    try:
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            # In production, decode JWT to get user ID
            user_id = 'extracted_from_jwt'
    except Exception:
        pass
    
    # Set correlation context
    set_correlation_context(
        correlation_id=correlation_id,
        user_id=user_id,
        request_id=request_id
    )
    
    # Get logger with context
    logger = structlog.get_logger()
    
    # Log request start
    start_time = time.time()
    logger.info("Request started",
                method=request.method,
                path=request.url.path,
                query_params=dict(request.query_params),
                client_ip=request.client.host if request.client else 'unknown',
                user_agent=request.headers.get('User-Agent', 'unknown'))
    
    try:
        # Process request
        response = await call_next(request)
        
        # Calculate duration
        duration_ms = round((time.time() - start_time) * 1000, 2)
        
        # Log request completion
        logger.info("Request completed",
                    status_code=response.status_code,
                    duration_ms=duration_ms)
        
        # Add correlation headers to response
        response.headers['X-Correlation-ID'] = correlation_id
        response.headers['X-Request-ID'] = request_id
        
        return response
        
    except Exception as e:
        # Calculate duration
        duration_ms = round((time.time() - start_time) * 1000, 2)
        
        # Log request error
        logger.error("Request failed",
                     error=str(e),
                     error_type=type(e).__name__,
                     duration_ms=duration_ms,
                     exc_info=True)
        raise
    
    finally:
        # Clear correlation context
        correlation_id_var.set(None)
        user_id_var.set(None)
        request_id_var.set(None)

class StructuredLogger:
    """Enhanced logger with additional methods for common use cases."""
    
    def __init__(self, service_name: str):
        self.logger = configure_structured_logging(service_name)
        self.service_name = service_name
    
    def log_business_event(self, event_type: str, **kwargs):
        """Log business events for analytics."""
        self.logger.info("Business event",
                        event_type=event_type,
                        business_event=True,
                        **kwargs)
    
    def log_performance_metric(self, operation: str, duration_ms: float, **kwargs):
        """Log performance metrics."""
        self.logger.info("Performance metric",
                        operation=operation,
                        duration_ms=duration_ms,
                        performance_metric=True,
                        **kwargs)
    
    def log_security_event(self, event_type: str, severity: str = "info", **kwargs):
        """Log security-related events."""
        log_func = getattr(self.logger, severity.lower(), self.logger.info)
        log_func("Security event",
                event_type=event_type,
                security_event=True,
                severity=severity,
                **kwargs)
    
    def log_data_event(self, event_type: str, entity_type: str, entity_id: str, **kwargs):
        """Log data-related events for audit trails."""
        self.logger.info("Data event",
                        event_type=event_type,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        data_event=True,
                        **kwargs)
    
    def log_error_with_context(self, error: Exception, operation: str, **kwargs):
        """Log errors with full context."""
        self.logger.error("Operation failed",
                         operation=operation,
                         error=str(error),
                         error_type=type(error).__name__,
                         exc_info=True,
                         **kwargs)

# Factory function for easy logger creation
def get_structured_logger(service_name: str) -> StructuredLogger:
    """Get a structured logger for a service."""
    return StructuredLogger(service_name)
