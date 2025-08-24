#!/usr/bin/env python3
"""
Rate Limiting Middleware
Advanced rate limiting and DDoS protection for event pipeline services
"""

import time
import json
from typing import Dict, Any, Optional
from functools import wraps
import asyncio

from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
import redis.asyncio as redis
from prometheus_client import Counter, Histogram
import structlog

logger = structlog.get_logger()

# Prometheus metrics
rate_limit_requests_total = Counter('rate_limit_requests_total', 'Total rate limit checks', ['status', 'endpoint'])
rate_limit_violations_total = Counter('rate_limit_violations_total', 'Total rate limit violations', ['type'])
rate_limit_latency_seconds = Histogram('rate_limit_latency_seconds', 'Rate limiting check latency')

class RateLimiter:
    """Advanced rate limiter with multiple strategies."""
    
    def __init__(self, redis_url: str = "redis://redis:6379", redis_password: str = "redis123"):
        self.redis_client = None
        self.redis_url = redis_url
        self.redis_password = redis_password
        
        # Rate limiting configurations
        self.rate_limits = {
            # Per-endpoint limits
            "/emit": {"requests": 100, "window": 60, "burst": 10},  # 100/min with 10 burst
            "/health": {"requests": 1000, "window": 60, "burst": 50},  # 1000/min health checks
            "/auth/token": {"requests": 10, "window": 60, "burst": 2},  # 10 auth/min
            "/auth/refresh": {"requests": 50, "window": 60, "burst": 5},  # 50 refresh/min
            
            # Global limits
            "global": {"requests": 10000, "window": 60, "burst": 100},  # 10k/min globally
            
            # Per-IP limits
            "per_ip": {"requests": 500, "window": 60, "burst": 20},  # 500/min per IP
            
            # Per-user limits (after authentication)
            "per_user": {"requests": 1000, "window": 60, "burst": 50},  # 1000/min per user
        }
        
        # DDoS protection thresholds
        self.ddos_thresholds = {
            "requests_per_second": 100,  # Trigger DDoS protection at 100 RPS from single IP
            "unique_ips_threshold": 1000,  # Track if we see more than 1000 unique IPs
            "error_rate_threshold": 0.5,  # 50% error rate triggers protection
        }
        
    async def init_redis(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.Redis.from_url(
                self.redis_url,
                password=self.redis_password,
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info("Rate limiter Redis connection established")
        except Exception as e:
            logger.error("Failed to connect to Redis for rate limiting", error=str(e))
            raise
    
    async def check_rate_limit(self, key: str, limit_config: Dict[str, int]) -> Dict[str, Any]:
        """Check rate limit using sliding window algorithm."""
        if not self.redis_client:
            return {"allowed": True, "remaining": limit_config["requests"]}
        
        start_time = time.time()
        
        try:
            current_time = int(time.time())
            window = limit_config["window"]
            max_requests = limit_config["requests"]
            burst_limit = limit_config.get("burst", max_requests // 10)
            
            # Use sliding window with Redis sorted sets
            pipe = self.redis_client.pipeline()
            
            # Remove expired entries
            pipe.zremrangebyscore(key, 0, current_time - window)
            
            # Count current requests in window
            pipe.zcard(key)
            
            # Add current request
            pipe.zadd(key, {str(current_time): current_time})
            
            # Set expiration
            pipe.expire(key, window + 1)
            
            results = await pipe.execute()
            current_requests = results[1]
            
            # Check burst protection (requests in last 10 seconds)
            burst_key = f"{key}:burst"
            burst_window = 10
            
            pipe = self.redis_client.pipeline()
            pipe.zremrangebyscore(burst_key, 0, current_time - burst_window)
            pipe.zcard(burst_key)
            pipe.zadd(burst_key, {str(current_time): current_time})
            pipe.expire(burst_key, burst_window + 1)
            
            burst_results = await pipe.execute()
            burst_requests = burst_results[1]
            
            # Rate limiting decision
            allowed = current_requests <= max_requests and burst_requests <= burst_limit
            remaining = max(0, max_requests - current_requests)
            
            rate_limit_latency_seconds.observe(time.time() - start_time)
            
            return {
                "allowed": allowed,
                "remaining": remaining,
                "limit": max_requests,
                "window": window,
                "burst_remaining": max(0, burst_limit - burst_requests),
                "burst_limit": burst_limit
            }
            
        except Exception as e:
            logger.error("Rate limit check failed", error=str(e), key=key)
            # Fail open - allow request if Redis is down
            return {"allowed": True, "remaining": limit_config["requests"]}
    
    async def check_ddos_protection(self, client_ip: str) -> bool:
        """Check for DDoS patterns."""
        if not self.redis_client:
            return False
        
        try:
            current_time = int(time.time())
            
            # Check requests per second from this IP
            rps_key = f"rps:{client_ip}"
            await self.redis_client.zadd(rps_key, {str(current_time): current_time})
            await self.redis_client.expire(rps_key, 1)
            
            # Count requests in last second
            rps_count = await self.redis_client.zcard(rps_key)
            
            if rps_count > self.ddos_thresholds["requests_per_second"]:
                logger.warning("DDoS protection triggered - high RPS", client_ip=client_ip, rps=rps_count)
                rate_limit_violations_total.labels(type="ddos_rps").inc()
                return True
            
            # Track unique IPs for distributed DDoS detection
            unique_ips_key = "unique_ips"
            await self.redis_client.sadd(unique_ips_key, client_ip)
            await self.redis_client.expire(unique_ips_key, 60)
            
            unique_ip_count = await self.redis_client.scard(unique_ips_key)
            
            if unique_ip_count > self.ddos_thresholds["unique_ips_threshold"]:
                logger.warning("DDoS protection triggered - too many unique IPs", unique_ips=unique_ip_count)
                rate_limit_violations_total.labels(type="ddos_distributed").inc()
                return True
            
            return False
            
        except Exception as e:
            logger.error("DDoS check failed", error=str(e))
            return False
    
    async def is_ip_blocked(self, client_ip: str) -> bool:
        """Check if IP is temporarily blocked."""
        if not self.redis_client:
            return False
        
        try:
            blocked = await self.redis_client.get(f"blocked_ip:{client_ip}")
            return blocked is not None
        except Exception:
            return False
    
    async def block_ip(self, client_ip: str, duration: int = 300):
        """Temporarily block an IP address."""
        if self.redis_client:
            try:
                await self.redis_client.setex(f"blocked_ip:{client_ip}", duration, "blocked")
                logger.warning("IP temporarily blocked", client_ip=client_ip, duration=duration)
            except Exception as e:
                logger.error("Failed to block IP", error=str(e))

# Global rate limiter instance
rate_limiter = RateLimiter()

def get_client_ip(request: Request) -> str:
    """Extract client IP from request headers."""
    # Check for forwarded headers (from load balancer/proxy)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip
    
    # Fall back to direct connection IP
    return request.client.host if request.client else "unknown"

def get_user_id(request: Request) -> Optional[str]:
    """Extract user ID from JWT token if available."""
    try:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            # In production, decode JWT to get user ID
            # For now, use a placeholder
            return "authenticated_user"
    except Exception:
        pass
    return None

async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware for FastAPI."""
    start_time = time.time()
    
    try:
        client_ip = get_client_ip(request)
        user_id = get_user_id(request)
        endpoint = request.url.path
        
        # Check if IP is blocked
        if await rate_limiter.is_ip_blocked(client_ip):
            rate_limit_requests_total.labels(status="blocked", endpoint=endpoint).inc()
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"error": "IP temporarily blocked due to rate limiting violations"}
            )
        
        # DDoS protection check
        if await rate_limiter.check_ddos_protection(client_ip):
            await rate_limiter.block_ip(client_ip, 300)  # Block for 5 minutes
            rate_limit_requests_total.labels(status="ddos_blocked", endpoint=endpoint).inc()
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"error": "DDoS protection activated"}
            )
        
        # Rate limit checks
        checks_to_perform = []
        
        # 1. Global rate limit
        checks_to_perform.append(("global", rate_limiter.rate_limits["global"]))
        
        # 2. Per-IP rate limit
        checks_to_perform.append((f"ip:{client_ip}", rate_limiter.rate_limits["per_ip"]))
        
        # 3. Per-endpoint rate limit
        if endpoint in rate_limiter.rate_limits:
            checks_to_perform.append((f"endpoint:{endpoint}", rate_limiter.rate_limits[endpoint]))
        
        # 4. Per-user rate limit (if authenticated)
        if user_id:
            checks_to_perform.append((f"user:{user_id}", rate_limiter.rate_limits["per_user"]))
        
        # Perform all rate limit checks
        for key, limit_config in checks_to_perform:
            result = await rate_limiter.check_rate_limit(key, limit_config)
            
            if not result["allowed"]:
                rate_limit_requests_total.labels(status="limited", endpoint=endpoint).inc()
                rate_limit_violations_total.labels(type="rate_limit").inc()
                
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "error": "Rate limit exceeded",
                        "limit": result["limit"],
                        "remaining": result["remaining"],
                        "window": result["window"],
                        "retry_after": result["window"]
                    },
                    headers={
                        "X-RateLimit-Limit": str(result["limit"]),
                        "X-RateLimit-Remaining": str(result["remaining"]),
                        "X-RateLimit-Reset": str(int(time.time()) + result["window"]),
                        "Retry-After": str(result["window"])
                    }
                )
        
        # Process request
        response = await call_next(request)
        
        rate_limit_requests_total.labels(status="allowed", endpoint=endpoint).inc()
        
        # Add rate limit headers to response
        if checks_to_perform:
            last_check = checks_to_perform[-1]
            result = await rate_limiter.check_rate_limit(last_check[0], last_check[1])
            
            response.headers["X-RateLimit-Limit"] = str(result["limit"])
            response.headers["X-RateLimit-Remaining"] = str(result["remaining"])
            response.headers["X-RateLimit-Reset"] = str(int(time.time()) + result["window"])
        
        return response
        
    except Exception as e:
        logger.error("Rate limiting middleware error", error=str(e))
        # Fail open - continue processing request
        response = await call_next(request)
        return response
