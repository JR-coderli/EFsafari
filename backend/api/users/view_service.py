"""
Saved view service for managing user saved views with default view support.
"""
from typing import Optional, List
from datetime import datetime
import logging

from api.database import get_db
from api.users.models import SavedViewCreate, SavedViewResponse

logger = logging.getLogger(__name__)

VIEW_TABLE = "user_views"


class ViewService:
    """Service for saved view management operations."""

    def __init__(self):
        self.db = get_db()

    def _get_table_name(self) -> str:
        """Get full table name."""
        return f"{self.db.database}.{VIEW_TABLE}"

    def init_table(self):
        """Create user_views table if not exists."""
        table_name = self._get_table_name()
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id String,
                user_id String,
                name String,
                dimensions Array(String),
                visible_metrics Array(String),
                color_mode UInt8 DEFAULT 0,
                is_default UInt8 DEFAULT 0,
                created_at DateTime,
                updated_at Nullable(DateTime)
            ) ENGINE = MergeTree()
            ORDER BY (user_id, created_at)
        """
        client = self.db.connect()
        client.command(create_sql)
        logger.info(f"Table {table_name} ensured")

    def create_view(self, user_id: str, view_create: SavedViewCreate) -> SavedViewResponse:
        """Create a new saved view."""
        # If setting as default, clear other defaults
        if view_create.is_default:
            self._clear_default_views(user_id)

        view_id = f"view_{int(datetime.now().timestamp() * 1000)}"
        now = datetime.now()

        table_name = self._get_table_name()
        client = self.db.connect()

        # Insert data using row-oriented format
        # Let ClickHouse auto-detect column order from table definition
        data = [[
            view_id,
            user_id,
            view_create.name,
            view_create.dimensions,
            view_create.visible_metrics,
            1 if view_create.color_mode else 0,
            1 if view_create.is_default else 0,
            now,
            None
        ]]
        client.insert(table_name, data)

        return self.get_view(view_id)

    def get_view(self, view_id: str) -> Optional[SavedViewResponse]:
        """Get a view by ID."""
        table_name = self._get_table_name()
        client = self.db.connect()
        result = client.query(f"SELECT * FROM {table_name} WHERE id = '{view_id}' LIMIT 1")
        for row in result.named_results():
            return self._row_to_response(row)
        return None

    def get_user_views(self, user_id: str) -> List[SavedViewResponse]:
        """Get all views for a user."""
        table_name = self._get_table_name()
        client = self.db.connect()
        result = client.query(f"SELECT * FROM {table_name} WHERE user_id = '{user_id}' ORDER BY created_at DESC")
        views = []
        for row in result.named_results():
            views.append(self._row_to_response(row))
        return views

    def get_default_view(self, user_id: str) -> Optional[SavedViewResponse]:
        """Get the default view for a user."""
        table_name = self._get_table_name()
        client = self.db.connect()
        result = client.query(f"SELECT * FROM {table_name} WHERE user_id = '{user_id}' AND is_default = 1 LIMIT 1")
        for row in result.named_results():
            return self._row_to_response(row)
        return None

    def set_default_view(self, user_id: str, view_id: str) -> Optional[SavedViewResponse]:
        """Set a view as the default for a user."""
        # First clear all defaults for this user
        self._clear_default_views(user_id)

        # Then set the new default
        table_name = self._get_table_name()
        client = self.db.connect()
        alter_sql = f"ALTER TABLE {table_name} UPDATE is_default = 1, updated_at = now() WHERE id = '{view_id}' AND user_id = '{user_id}'"
        client.command(alter_sql)

        return self.get_view(view_id)

    def update_view(self, view_id: str, user_id: str, updates: dict) -> Optional[SavedViewResponse]:
        """Update a view."""
        view = self.get_view(view_id)
        if not view or view.user_id != user_id:
            return None

        update_fields = []

        if "name" in updates:
            update_fields.append(f"name = '{updates['name'].replace("'", "''")}'")
        if "dimensions" in updates:
            dim_list = "', '".join(updates["dimensions"])
            update_fields.append(f"dimensions = ['{dim_list}']")
        if "visible_metrics" in updates:
            metrics_list = "', '".join(updates["visible_metrics"])
            update_fields.append(f"visible_metrics = ['{metrics_list}']")
        if "color_mode" in updates:
            update_fields.append(f"color_mode = {1 if updates['color_mode'] else 0}")
        if "is_default" in updates:
            if updates["is_default"]:
                self._clear_default_views(user_id)
            update_fields.append(f"is_default = {1 if updates['is_default'] else 0}")

        if not update_fields:
            return view

        update_fields.append("updated_at = now()")

        table_name = self._get_table_name()
        client = self.db.connect()
        alter_sql = f"ALTER TABLE {table_name} UPDATE {', '.join(update_fields)} WHERE id = '{view_id}'"
        client.command(alter_sql)

        return self.get_view(view_id)

    def delete_view(self, view_id: str, user_id: str) -> bool:
        """Delete a view."""
        view = self.get_view(view_id)
        if not view or view.user_id != user_id:
            return False

        table_name = self._get_table_name()
        client = self.db.connect()
        client.command(f"ALTER TABLE {table_name} DELETE WHERE id = '{view_id}'")
        return True

    def _clear_default_views(self, user_id: str):
        """Clear all default flags for a user."""
        table_name = self._get_table_name()
        client = self.db.connect()
        alter_sql = f"ALTER TABLE {table_name} UPDATE is_default = 0, updated_at = now() WHERE user_id = '{user_id}' AND is_default = 1"
        client.command(alter_sql)

    def _row_to_response(self, row: dict) -> SavedViewResponse:
        """Convert database row to SavedViewResponse."""
        return SavedViewResponse(
            id=row["id"],
            user_id=row["user_id"],
            name=row["name"],
            dimensions=list(row["dimensions"]) if row["dimensions"] else [],
            visible_metrics=list(row["visible_metrics"]) if row["visible_metrics"] else [],
            color_mode=bool(row["color_mode"]),
            is_default=bool(row["is_default"]),
            created_at=row["created_at"],
            updated_at=row.get("updated_at")
        )


# Global service instance
_view_service: Optional[ViewService] = None


def get_view_service() -> ViewService:
    """Get global view service instance."""
    global _view_service
    if _view_service is None:
        _view_service = ViewService()
        _view_service.init_table()
    return _view_service
