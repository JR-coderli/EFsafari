"""
Authentication API router.
"""
from fastapi import APIRouter, HTTPException, status, Depends
from datetime import timedelta
import logging

from api.auth import (
    authenticate_user,
    create_access_token,
    get_current_user,
    require_admin,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from api.users.models import LoginRequest, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login")
async def login(login_data: LoginRequest):
    """Authenticate user and return access token."""
    user = authenticate_user(login_data.username, login_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token({
        "sub": user.username,
        "user_id": user.id,
        "role": user.role
    })
    
    import json
    response_data = {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "keywords": user.keywords,
            "showRevenue": user.showRevenue,
            "created_at": user.created_at.isoformat(),
            "updated_at": user.updated_at.isoformat() if user.updated_at else None
        }
    }
    print(f"DEBUG RESPONSE: {json.dumps(response_data)}")
    return response_data


@router.post("/verify")
async def verify_token(current_user: dict = Depends(get_current_user)):
    """Verify if the current token is valid and return user info."""
    return current_user


@router.get("/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    """Get current user info."""
    return current_user
