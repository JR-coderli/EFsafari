"""
FastAPI Application for AdData Dashboard API.

This API provides data endpoints for the frontend dashboard,
querying ClickHouse for marketing performance data.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime
import logging
import json

from api.routers.dashboard import router as dashboard_router
from api.routers.auth import router as auth_router
from api.users.router import router as users_router


# Custom JSON encoder for datetime
class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


# Custom response class with datetime encoding
class CustomJSONResponse(JSONResponse):
    """JSONResponse that uses DateTimeEncoder."""
    def render(self, content) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            cls=DateTimeEncoder,
        ).encode("utf-8")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="AdData Dashboard API",
    description="API for fetching marketing performance data from ClickHouse",
    version="1.0.0",
    default_response_class=CustomJSONResponse
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative port
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(dashboard_router)
app.include_router(auth_router)
app.include_router(users_router)


@app.get("/")
async def root():
    """Root endpoint - API info."""
    return {
        "name": "AdData Dashboard API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/api/dashboard/health",
            "platforms": "/api/dashboard/platforms",
            "data": "/api/dashboard/data",
            "daily": "/api/dashboard/daily",
            "dimensions": "/api/dashboard/dimensions",
            "metrics": "/api/dashboard/metrics",
            "aggregate": "/api/dashboard/aggregate",
            "login": "/api/auth/login",
            "verify": "/api/auth/verify",
            "users": "/api/users"
        }
    }


@app.get("/health")
async def health():
    """Quick health check."""
    return {"status": "ok"}


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
