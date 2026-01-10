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


@router.post("/login", response_model=None)
async def login(login_data: LoginRequest):
    """Authenticate user and return access token."""
    user = authenticate_user(login_data.username, login_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create access token with role
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user.username,
            "user_id": user.id,
            "role": user.role
        },
        expires_delta=access_token_expires
    )

    # Debug logging
    logger.info(f"User object: {user}")
    logger.info(f"User keywords type: {type(user.keywords)}, value: {user.keywords}")
    logger.info(f"User role type: {type(user.role)}, value: {user.role}")

    # Return user without password - test with plain text
    from fastapi.responses import PlainTextResponse
    test_data = '{"access_token":"test","user":{"keywords":["test"]}}'
    logger.info(f"Returning: {test_data}")
    return PlainTextResponse(content=test_data)


@router.post("/verify")
async def verify_token(current_user: dict = Depends(get_current_user)):
    """Verify if the current token is valid and return user info."""
    return current_user


@router.get("/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    """Get current user info."""
    return current_user
