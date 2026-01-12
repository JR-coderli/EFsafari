"""
User service for ClickHouse-based user management.

Handles user CRUD operations, authentication, and permission filtering.
"""
from typing import Optional, List
from datetime import datetime
import hashlib
import secrets
import logging

from api.database import get_db
from api.users.models import User, UserInDB, UserCreate, UserUpdate

logger = logging.getLogger(__name__)

# ClickHouse table name
USER_TABLE = "users"


def hash_password(password: str) -> str:
    """Hash a password using SHA256 with salt."""
    salt = secrets.token_hex(16)
    pwd_hash = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return f"${salt}${pwd_hash}"


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    try:
        salt, pwd_hash = hashed_password.split('$')[1:]
        new_hash = hashlib.sha256(f"{salt}{plain_password}".encode()).hexdigest()
        return secrets.compare_digest(new_hash, pwd_hash)
    except (ValueError, IndexError):
        return False


class UserService:
    """Service for user management operations."""

    def __init__(self):
        self.db = get_db()

    def _get_table_name(self) -> str:
        """Get full table name."""
        return f"{self.db.database}.{USER_TABLE}"

    def init_table(self):
        """Create users table if not exists."""
        table_name = self._get_table_name()
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id String,
                name String,
                username String,
                password_hash String,
                email String,
                role Enum('admin', 'ops', 'business'),
                keywords Array(String),
                created_at DateTime,
                updated_at Nullable(DateTime)
            ) ENGINE = MergeTree()
            ORDER BY id
        """
        client = self.db.connect()
        client.command(create_sql)
        logger.info(f"Table {table_name} ensured")

    def init_admin_user(self):
        """Create default admin user if not exists."""
        existing = self.get_user_by_username("admin")
        if existing:
            logger.info("Admin user already exists")
            return

        admin_user = UserInDB(
            id="admin",
            name="Admin User",
            username="admin",
            email="admin@addata.ai",
            password_hash=hash_password("password"),
            role='admin',
            keywords=[],
            created_at=datetime.now(),
            updated_at=None
        )
        self._insert_user(admin_user)
        logger.info("Default admin user created (admin/password)")

    def _insert_user(self, user: UserInDB):
        """Insert a user into the database using SQL INSERT."""
        print(f"DEBUG _insert_user called with user.username={user.username}, user.keywords={user.keywords}")
        table_name = self._get_table_name()
        client = self.db.connect()

        # Format keywords as ClickHouse array with proper escaping
        if user.keywords:
            escaped_items = []
            for k in user.keywords:
                # Escape backslashes first, then single quotes
                escaped = k.replace("\\", "\\\\").replace("'", "''")
                escaped_items.append(f"'{escaped}'")
            keywords_str = f"[{', '.join(escaped_items)}]"
        else:
            keywords_str = "[]"

        # Format datetime
        created_at_str = user.created_at.strftime('%Y-%m-%d %H:%M:%S') if user.created_at else 'now()'
        updated_at_str = user.updated_at.strftime('%Y-%m-%d %H:%M:%S') if user.updated_at else 'NULL'

        # Role is now a string literal
        role_value = user.role if isinstance(user.role, str) else str(user.role)

        # Escape special characters in string fields
        name_escaped = user.name.replace("\\", "\\\\").replace("'", "''")

        insert_sql = f"""
            INSERT INTO {table_name} (id, name, username, password_hash, email, role, keywords, created_at, updated_at)
            VALUES (
                '{user.id}',
                '{name_escaped}',
                '{user.username}',
                '{user.password_hash}',
                '{user.email}',
                '{role_value}',
                {keywords_str},
                '{created_at_str}',
                {updated_at_str}
            )
        """
        client.command(insert_sql)

    def get_user_by_username(self, username: str) -> Optional[UserInDB]:
        """Get a user by username."""
        print(f"DEBUG: get_user_by_username called with username={username}")
        table_name = self._get_table_name()
        client = self.db.connect()
        result = client.query(f"SELECT * FROM {table_name} WHERE username = '{username}' LIMIT 1")
        for row in result.named_results():
            keywords = list(row["keywords"]) if row["keywords"] else []
            print(f"DEBUG get_user_by_username: keywords = {keywords}, type = {type(keywords)}")
            user = UserInDB(
                id=row["id"],
                name=row["name"],
                username=row["username"],
                password_hash=row["password_hash"],
                email=row["email"],
                role=row["role"],
                keywords=keywords,
                created_at=row["created_at"],
                updated_at=row.get("updated_at")
            )
            print(f"DEBUG get_user_by_username: user.keywords = {user.keywords}")
            return user
        print(f"DEBUG: get_user_by_username: no user found")
        return None

    def get_user_by_id(self, user_id: str) -> Optional[UserInDB]:
        """Get a user by ID."""
        table_name = self._get_table_name()
        client = self.db.connect()
        result = client.query(f"SELECT * FROM {table_name} WHERE id = '{user_id}' LIMIT 1")
        for row in result.named_results():
            return UserInDB(
                id=row["id"],
                name=row["name"],
                username=row["username"],
                password_hash=row["password_hash"],
                email=row["email"],
                role=row["role"],
                keywords=list(row["keywords"]) if row["keywords"] else [],
                created_at=row["created_at"],
                updated_at=row.get("updated_at")
            )
        return None

    def get_all_users(self) -> List[dict]:
        """Get all users."""
        table_name = self._get_table_name()
        client = self.db.connect()
        result = client.query(f"SELECT id, name, username, email, role, keywords, created_at, updated_at FROM {table_name} ORDER BY created_at DESC")
        users = []
        for row in result.named_results():
            users.append({
                "id": row["id"],
                "name": row["name"],
                "username": row["username"],
                "email": row["email"],
                "role": row["role"],
                "keywords": list(row["keywords"]) if row["keywords"] else [],
                "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], 'isoformat') else str(row["created_at"]),
                "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") and hasattr(row["updated_at"], 'isoformat') else (row.get("updated_at") if row.get("updated_at") else None)
            })
        return users

    def create_user(self, user_create: UserCreate) -> User:
        """Create a new user."""
        # Check if username already exists
        existing = self.get_user_by_username(user_create.username)
        if existing:
            raise ValueError("Username already exists")

        # Generate ID
        user_id = f"user_{int(datetime.now().timestamp() * 1000)}"

        new_user = UserInDB(
            id=user_id,
            name=user_create.name,
            username=user_create.username,
            email=user_create.email,
            password_hash=hash_password(user_create.password),
            role=user_create.role,
            keywords=user_create.keywords,
            created_at=datetime.now(),
            updated_at=None
        )

        self._insert_user(new_user)

        return User(
            id=new_user.id,
            name=new_user.name,
            username=new_user.username,
            email=new_user.email,
            role=new_user.role,
            keywords=new_user.keywords,
            created_at=new_user.created_at,
            updated_at=None
        )

    def update_user(self, user_id: str, updates: dict) -> Optional[User]:
        """Update a user."""
        user = self.get_user_by_id(user_id)
        if not user:
            return None

        # Build update fields
        update_fields = []
        update_values = {}

        if "name" in updates:
            name_val = updates["name"].replace("'", "''")
            update_fields.append(f"name = '{name_val}'")
        if "email" in updates:
            update_fields.append(f"email = '{updates['email']}'")
        if "password" in updates:
            update_fields.append(f"password_hash = '{hash_password(updates['password'])}'")
        if "role" in updates:
            role_value = updates["role"] if isinstance(updates["role"], str) else str(updates["role"])
            update_fields.append(f"role = '{role_value}'")
        if "keywords" in updates:
            # Use array() function for safer ClickHouse array handling
            keywords_arr = updates["keywords"]
            if keywords_arr:
                escaped_items = []
                for k in keywords_arr:
                    # Escape single quotes by doubling them and escape backslashes
                    escaped = k.replace("\\", "\\\\").replace("'", "''")
                    escaped_items.append(f"'{escaped}'")
                formatted_items = ", ".join(escaped_items)
                update_fields.append(f"keywords = [{formatted_items}]")
            else:
                update_fields.append("keywords = []")

        if not update_fields:
            return user

        update_fields.append(f"updated_at = now()")

        table_name = self._get_table_name()
        client = self.db.connect()
        alter_sql = f"ALTER TABLE {table_name} UPDATE {', '.join(update_fields)} WHERE id = '{user_id}'"
        client.command(alter_sql)

        # Fetch updated user
        updated_user = self.get_user_by_id(user_id)
        if not updated_user:
            return None

        return User(
            id=updated_user.id,
            name=updated_user.name,
            username=updated_user.username,
            email=updated_user.email,
            role=updated_user.role,
            keywords=updated_user.keywords,
            created_at=updated_user.created_at,
            updated_at=updated_user.updated_at
        )

    def delete_user(self, user_id: str) -> bool:
        """Delete a user."""
        if user_id == "admin":
            raise ValueError("Cannot delete admin user")

        user = self.get_user_by_id(user_id)
        if not user:
            return False

        table_name = self._get_table_name()
        client = self.db.connect()
        client.command(f"ALTER TABLE {table_name} DELETE WHERE id = '{user_id}'")
        return True

    def authenticate(self, username: str, password: str) -> Optional[UserInDB]:
        """Authenticate a user with username and password."""
        user = self.get_user_by_username(username)
        if not user:
            return None
        if not verify_password(password, user.password_hash):
            return None
        return user

    def build_permission_filter(self, user: User) -> Optional[str]:
        """Build SQL WHERE clause for user's data access permissions.

        Args:
            user: The user to build permissions for

        Returns:
            SQL WHERE clause fragment, or None for admin (no restriction)
        """
        if user.role == 'admin':
            return None

        # Empty keywords means no restriction
        if not user.keywords:
            return None

        # Build keyword filter based on role
        if user.role == 'ops':
            # Filter by Adset column
            keyword_conditions = [f"lower(Adset) LIKE lower('%{k}%')" for k in user.keywords]
            return f"({' OR '.join(keyword_conditions)})"
        elif user.role == 'ops02':
            # Filter by platform column
            keyword_conditions = [f"lower(platform) LIKE lower('%{k}%')" for k in user.keywords]
            return f"({' OR '.join(keyword_conditions)})"
        elif user.role == 'business':
            # Filter by offer column
            keyword_conditions = [f"lower(offer) LIKE lower('%{k}%')" for k in user.keywords]
            return f"({' OR '.join(keyword_conditions)})"

        return None

    def can_access_all_data(self, user: User) -> bool:
        """Check if user can access all data without filtering."""
        if user.role == 'admin':
            return True
        if not user.keywords:
            return True
        return False


# Global service instance
_user_service: Optional[UserService] = None


def get_user_service() -> UserService:
    """Get global user service instance."""
    global _user_service
    if _user_service is None:
        _user_service = UserService()
        _user_service.init_table()
        _user_service.init_admin_user()
    return _user_service
