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
from api.users.models import User, UserInDB, UserCreate, UserRole, UserUpdate

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
            role=UserRole.ADMIN,
            keywords=[],
            created_at=datetime.now(),
            updated_at=None
        )
        self._insert_user(admin_user)
        logger.info("Default admin user created (admin/password)")

    def _insert_user(self, user: UserInDB):
        """Insert a user into the database."""
        table_name = self._get_table_name()
        client = self.db.connect()
        # Use column-oriented format for ClickHouse insert
        data = {
            'id': [user.id],
            'name': [user.name],
            'username': [user.username],
            'password_hash': [user.password_hash],
            'email': [user.email],
            'role': [user.role.value if isinstance(user.role, UserRole) else user.role],
            'keywords': [user.keywords],
            'created_at': [user.created_at],
            'updated_at': [user.updated_at],
        }
        client.insert(table_name, data, column_oriented=True)

    def get_user_by_username(self, username: str) -> Optional[UserInDB]:
        """Get a user by username."""
        table_name = self._get_table_name()
        client = self.db.connect()
        result = client.query(f"SELECT * FROM {table_name} WHERE username = '{username}' LIMIT 1")
        for row in result.named_results():
            return UserInDB(
                id=row["id"],
                name=row["name"],
                username=row["username"],
                password_hash=row["password_hash"],
                email=row["email"],
                role=UserRole(row["role"]),
                keywords=list(row["keywords"]) if row["keywords"] else [],
                created_at=row["created_at"],
                updated_at=row.get("updated_at")
            )
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
                role=UserRole(row["role"]),
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
            role_value = updates["role"].value if isinstance(updates["role"], UserRole) else updates["role"]
            update_fields.append(f"role = '{role_value}'")
        if "keywords" in updates:
            keywords_arr = updates["keywords"]
            keywords_str = str(keywords_arr).replace("'", "\\'")
            update_fields.append(f"keywords = {keywords_str}")

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
        if user.role == UserRole.ADMIN:
            return None

        # Empty keywords means no restriction
        if not user.keywords:
            return None

        # Build keyword filter based on role
        if user.role == UserRole.OPS:
            # Filter by Adset column
            keyword_conditions = [f"lower(Adset) LIKE lower('%{k}%')" for k in user.keywords]
            return f"({' OR '.join(keyword_conditions)})"
        elif user.role == UserRole.BUSINESS:
            # Filter by offer column
            keyword_conditions = [f"lower(offer) LIKE lower('%{k}%')" for k in user.keywords]
            return f"({' OR '.join(keyword_conditions)})"

        return None

    def can_access_all_data(self, user: User) -> bool:
        """Check if user can access all data without filtering."""
        if user.role == UserRole.ADMIN:
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
