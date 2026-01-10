"""
User management API router.
"""
from fastapi import APIRouter, HTTPException, status, Depends
import logging

from api.users.service import get_user_service, UserService
from api.users.models import UserCreate, UserUpdate, User, UserRole

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/users", tags=["users"])


async def get_current_user_info(token_data: dict) -> dict:
    """Get current user from token data (injected by auth middleware)."""
    service = get_user_service()
    user_id = token_data.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    user = service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    return {
        "id": user.id,
        "name": user.name,
        "username": user.username,
        "email": user.email,
        "role": user.role.value,
        "keywords": user.keywords,
        "created_at": user.created_at.isoformat(),
        "updated_at": user.updated_at.isoformat() if user.updated_at else None
    }


async def require_admin(current_user: dict = Depends(get_current_user_info)) -> dict:
    """Require the current user to be admin."""
    if current_user.get("role") != UserRole.ADMIN.value:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user


@router.get("/me")
async def get_current_user_endpoint(current_user: dict = Depends(get_current_user_info)):
    """Get current user info."""
    return current_user


@router.get("")
async def list_users(current_user: dict = Depends(require_admin)):
    """List all users. Admin only."""
    try:
        service = get_user_service()
        users = service.get_all_users()
        return {"users": users}
    except Exception as e:
        logger.error(f"Error listing users: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def create_new_user(
    user_create: UserCreate,
    current_user: dict = Depends(require_admin)
):
    """Create a new user. Admin only."""
    try:
        service = get_user_service()
        new_user = service.create_user(user_create)
        return {
            "id": new_user.id,
            "name": new_user.name,
            "username": new_user.username,
            "email": new_user.email,
            "role": new_user.role.value,
            "keywords": new_user.keywords,
            "created_at": new_user.created_at.isoformat(),
            "updated_at": None
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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
        if user_update.role is not None:
            updates["role"] = user_update.role
        if user_update.keywords is not None:
            updates["keywords"] = user_update.keywords

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        service = get_user_service()
        updated_user = service.update_user(user_id, updates)
        if not updated_user:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "id": updated_user.id,
            "name": updated_user.name,
            "username": updated_user.username,
            "email": updated_user.email,
            "role": updated_user.role.value,
            "keywords": updated_user.keywords,
            "created_at": updated_user.created_at.isoformat(),
            "updated_at": updated_user.updated_at.isoformat() if updated_user.updated_at else None
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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
        service = get_user_service()
        success = service.delete_user(user_id)
        if not success:
            raise HTTPException(status_code=404, detail="User not found")
        return {"message": "User deleted successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail=str(e))
