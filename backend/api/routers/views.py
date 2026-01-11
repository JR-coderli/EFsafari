"""
Saved views API router.
"""
from fastapi import APIRouter, HTTPException, status, Depends
import logging

from api.users.view_service import get_view_service, ViewService
from api.users.models import SavedViewCreate, SavedViewUpdate, SavedViewResponse
from api.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/views", tags=["views"])


@router.get("")
async def list_views(current_user: dict = Depends(get_current_user)):
    """List all views for current user."""
    try:
        service = get_view_service()
        views = service.get_user_views(current_user["id"])
        return {"views": views}
    except Exception as e:
        logger.error(f"Error listing views: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def create_view(
    view_create: SavedViewCreate,
    current_user: dict = Depends(get_current_user)
):
    """Create a new saved view."""
    try:
        service = get_view_service()
        new_view = service.create_view(current_user["id"], view_create)
        return new_view
    except Exception as e:
        import traceback
        logger.error(f"Error creating view: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/default")
async def get_default_view_endpoint(current_user: dict = Depends(get_current_user)):
    """Get the default view for current user."""
    try:
        service = get_view_service()
        view = service.get_default_view(current_user["id"])
        if not view:
            return {"view": None}
        return {"view": view}
    except Exception as e:
        logger.error(f"Error getting default view: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{view_id}")
async def update_view(
    view_id: str,
    view_update: SavedViewUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update a view."""
    try:
        updates = {}
        if view_update.name is not None:
            updates["name"] = view_update.name
        if view_update.dimensions is not None:
            updates["dimensions"] = view_update.dimensions
        if view_update.visible_metrics is not None:
            updates["visible_metrics"] = view_update.visible_metrics
        if view_update.color_mode is not None:
            updates["color_mode"] = view_update.color_mode
        if view_update.is_default is not None:
            updates["is_default"] = view_update.is_default

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        service = get_view_service()
        updated_view = service.update_view(view_id, current_user["id"], updates)
        if not updated_view:
            raise HTTPException(status_code=404, detail="View not found")

        return updated_view
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating view: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{view_id}/set-default")
async def set_default_view_endpoint(
    view_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Set a view as the default view."""
    try:
        service = get_view_service()
        view = service.set_default_view(current_user["id"], view_id)
        if not view:
            raise HTTPException(status_code=404, detail="View not found")
        return view
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error setting default view: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{view_id}")
async def delete_view_endpoint(
    view_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a view."""
    try:
        service = get_view_service()
        success = service.delete_view(view_id, current_user["id"])
        if not success:
            raise HTTPException(status_code=404, detail="View not found")
        return {"message": "View deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting view: {e}")
        raise HTTPException(status_code=500, detail=str(e))
