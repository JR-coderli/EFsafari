"""
User management API router.
"""
from fastapi import APIRouter, HTTPException, status, Depends
import logging

from api.auth import get_current_user, require_admin, get_all_users, create_user, update_user, delete_user, get_user_by_id
from api.models.user import UserCreate, UserUpdate

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/users", tags=["users"])


@router.get("")
async def list_users(current_user: dict = Depends(require_admin)):
    """List all users. Admin only."""
    try:
        users = get_all_users()
        return {"users": users}
    except Exception as e:
        logger.error(f"Error listing users: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """Get current user info."""
    return current_user


@router.post("")
async def create_new_user(
    user_create: UserCreate,
    current_user: dict = Depends(require_admin)
):
    """Create a new user. Admin only."""
    try:
        new_user = create_user(user_create)
        return new_user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{user_id}")
async def update_user_info(
    user_id: str,
    user_update: UserUpdate,
    current_user: dict = Depends(require_admin)
):
    """Update a user. Admin only."""
    try:
        updates = {}
        if user_update.name is not None:
            updates["name"] = user_update.name
        if user_update.email is not None:
            updates["email"] = user_update.email
        if user_update.password is not None:
            updates["password"] = user_update.password
        if user_update.allowed_keywords is not None:
            updates["allowed_keywords"] = user_update.allowed_keywords

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        updated_user = update_user(user_id, updates)
        if not updated_user:
            raise HTTPException(status_code=404, detail="User not found")

        return updated_user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating user: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{user_id}")
async def delete_user_endpoint(
    user_id: str,
    current_user: dict = Depends(require_admin)
):
    """Delete a user. Admin only."""
    try:
        success = delete_user(user_id)
        if not success:
            raise HTTPException(status_code=404, detail="User not found")
        return {"message": "User deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail=str(e))
