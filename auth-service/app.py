#!/usr/bin/env python3
"""
JWT Authentication Service
Centralized authentication and session management for the event pipeline
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
import asyncio

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from jose import JWTError, jwt
import redis
from prometheus_client import Counter, Histogram, start_http_server
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Prometheus metrics
auth_tokens_issued_total = Counter('auth_tokens_issued_total', 'Total JWT tokens issued')
auth_tokens_validated_total = Counter('auth_tokens_validated_total', 'Total token validations', ['status'])
auth_requests_total = Counter('auth_requests_total', 'Total authentication requests', ['endpoint'])
auth_latency_seconds = Histogram('auth_latency_seconds', 'Authentication latency in seconds')

# JWT Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

# Redis configuration for session management
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "redis123")

class TokenRequest(BaseModel):
    username: str
    password: str
    service: str = "event-pipeline"

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int

class RefreshRequest(BaseModel):
    refresh_token: str

class TokenData(BaseModel):
    username: Optional[str] = None
    service: Optional[str] = None
    scopes: list[str] = []

class AuthService:
    def __init__(self):
        self.redis_client = None
        self.security = HTTPBearer()
        
        # User database (in production, use proper database)
        self.users = {
            "admin": {
                "password": "admin123",  # Hash this in production
                "scopes": ["read", "write", "admin"]
            },
            "producer": {
                "password": "producer123",
                "scopes": ["read", "write"]
            },
            "consumer": {
                "password": "consumer123", 
                "scopes": ["read"]
            }
        }
        
        logger.info("JWT Authentication Service initialized")
    
    async def init_redis(self):
        """Initialize Redis connection for session management."""
        try:
            import redis.asyncio as redis_async
            self.redis_client = redis_async.Redis.from_url(
                REDIS_URL,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info("Redis connection established for session management")
        except Exception as e:
            logger.error("Failed to connect to Redis", error=str(e))
            raise
    
    def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user credentials."""
        if username in self.users:
            user = self.users[username]
            if user["password"] == password:  # Use proper hashing in production
                return {
                    "username": username,
                    "scopes": user["scopes"]
                }
        return None
    
    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
        """Create JWT access token."""
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "type": "access",
            "jti": str(uuid.uuid4())  # JWT ID for tracking
        })
        
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    
    def create_refresh_token(self, data: dict) -> str:
        """Create JWT refresh token."""
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "type": "refresh",
            "jti": str(uuid.uuid4())
        })
        
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    
    async def store_refresh_token(self, jti: str, username: str, expires_in: int):
        """Store refresh token in Redis with expiration."""
        if self.redis_client:
            await self.redis_client.setex(f"refresh_token:{jti}", expires_in, username)
    
    async def revoke_refresh_token(self, jti: str):
        """Revoke refresh token."""
        if self.redis_client:
            await self.redis_client.delete(f"refresh_token:{jti}")
    
    async def is_refresh_token_valid(self, jti: str) -> bool:
        """Check if refresh token is still valid."""
        if self.redis_client:
            result = await self.redis_client.get(f"refresh_token:{jti}")
            return result is not None
        return False
    
    def verify_token(self, token: str) -> Optional[TokenData]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            service: str = payload.get("service", "unknown")
            scopes: list = payload.get("scopes", [])
            
            if username is None:
                return None
                
            token_data = TokenData(username=username, service=service, scopes=scopes)
            return token_data
        except JWTError as e:
            logger.warning("JWT verification failed", error=str(e))
            return None

# Global auth service
auth_service = AuthService()

# FastAPI app
app = FastAPI(title="JWT Authentication Service", version="1.0.0")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(auth_service.security)) -> TokenData:
    """Get current authenticated user from JWT token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    token_data = auth_service.verify_token(credentials.credentials)
    if token_data is None:
        auth_tokens_validated_total.labels(status="invalid").inc()
        raise credentials_exception
    
    auth_tokens_validated_total.labels(status="valid").inc()
    return token_data

@app.post("/auth/token", response_model=TokenResponse)
async def login_for_access_token(request: TokenRequest):
    """Generate JWT tokens for authenticated users."""
    auth_requests_total.labels(endpoint="token").inc()
    
    with auth_latency_seconds.time():
        user = auth_service.authenticate_user(request.username, request.password)
        if not user:
            logger.warning("Authentication failed", username=request.username, service=request.service)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Create tokens
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        token_data = {
            "sub": user["username"],
            "service": request.service,
            "scopes": user["scopes"]
        }
        
        access_token = auth_service.create_access_token(
            data=token_data, expires_delta=access_token_expires
        )
        refresh_token = auth_service.create_refresh_token(data=token_data)
        
        # Store refresh token
        refresh_payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        refresh_jti = refresh_payload.get("jti")
        await auth_service.store_refresh_token(
            refresh_jti, 
            user["username"], 
            int(timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS).total_seconds())
        )
        
        auth_tokens_issued_total.inc()
        logger.info("JWT tokens issued", username=user["username"], service=request.service)
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
        }

@app.post("/auth/refresh", response_model=TokenResponse)
async def refresh_access_token(request: RefreshRequest):
    """Refresh access token using refresh token."""
    auth_requests_total.labels(endpoint="refresh").inc()
    
    try:
        payload = jwt.decode(request.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=400, detail="Invalid token type")
        
        jti = payload.get("jti")
        if not await auth_service.is_refresh_token_valid(jti):
            raise HTTPException(status_code=401, detail="Refresh token revoked or expired")
        
        # Create new access token
        token_data = {
            "sub": payload.get("sub"),
            "service": payload.get("service"),
            "scopes": payload.get("scopes", [])
        }
        
        access_token = auth_service.create_access_token(data=token_data)
        
        logger.info("Access token refreshed", username=payload.get("sub"))
        
        return {
            "access_token": access_token,
            "refresh_token": request.refresh_token,  # Keep same refresh token
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
        }
        
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

@app.post("/auth/revoke")
async def revoke_refresh_token(request: RefreshRequest, current_user: TokenData = Depends(get_current_user)):
    """Revoke refresh token (logout)."""
    try:
        payload = jwt.decode(request.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        jti = payload.get("jti")
        await auth_service.revoke_refresh_token(jti)
        logger.info("Refresh token revoked", username=current_user.username)
        return {"message": "Token revoked successfully"}
    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid refresh token")

@app.get("/auth/verify")
async def verify_token(current_user: TokenData = Depends(get_current_user)):
    """Verify token and return user info."""
    return {
        "username": current_user.username,
        "service": current_user.service,
        "scopes": current_user.scopes,
        "valid": True
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    redis_status = "connected" if auth_service.redis_client else "disconnected"
    return {
        "status": "healthy",
        "service": "jwt-auth",
        "redis": redis_status
    }

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global auth_service
    
    # Start Prometheus metrics server
    start_http_server(8001)
    logger.info("Prometheus metrics server started on port 8001")
    
    # Initialize Redis connection
    await auth_service.init_redis()
    
    logger.info("JWT Authentication Service fully operational!")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    if auth_service.redis_client:
        await auth_service.redis_client.close()
    logger.info("JWT Authentication Service shutdown complete")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
