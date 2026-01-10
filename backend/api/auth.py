"""
Authentication and authorization utilities.

Handles JWT token generation/validation, password hashing,
and user database management.
"""
from datetime import datetime, timedelta
from typing import Optional
import hashlib
import json
import os
import secrets
from pathlib import Path

from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from api.models.user import User, UserInDB, UserCreate, TokenData

# JWT Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "addata-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

# HTTP Bearer token
security = HTTPBearer()

# User database file (JSON file for simplicity)
USER_DB_FILE = Path(__file__).parent / "users.json"


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


def get_user_db_path() -> Path:
    """Get user database file path."""
    return USER_DB_FILE


def init_user_db():
    """Initialize user database with default admin user."""
    if not USER_DB_FILE.exists():
        default_admin = {
            "id": "admin",
            "name": "Admin User",
            "username": "admin",
            "email": "admin@addata.ai",
            "password_hash": hash_password("password"),
            "allowed_keywords": [],
            "created_at": datetime.now().isoformat(),
            "updated_at": None
        }
        USER_DB_FILE.write_text(json.dumps({"users": [default_admin]}, indent=2))


def load_users() -> dict:
    """Load users from database."""
    if not USER_DB_FILE.exists():
        init_user_db()
    data = USER_DB_FILE.read_text()
    return json.loads(data)


def save_users(data: dict):
    """Save users to database."""
    USER_DB_FILE.write_text(json.dumps(data, indent=2))


def get_user(username: str) -> Optional[UserInDB]:
    """Get a user by username."""
    data = load_users()
    for user_data in data.get("users", []):
        if user_data["username"] == username:
            return UserInDB(**user_data)
    return None


def get_user_by_id(user_id: str) -> Optional[UserInDB]:
    """Get a user by ID."""
    data = load_users()
    for user_data in data.get("users", []):
        if user_data["id"] == user_id:
            return UserInDB(**user_data)
    return None


def create_user(user_create: UserCreate) -> User:
    """Create a new user."""
    data = load_users()

    # Check if username already exists
    for user_data in data.get("users", []):
        if user_data["username"] == user_create.username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already exists"
            )

    # Generate ID
    user_id = f"user_{int(datetime.now().timestamp() * 1000)}"

    new_user = UserInDB(
        id=user_id,
        name=user_create.name,
        username=user_create.username,
        email=user_create.email,
        password_hash=hash_password(user_create.password),
        allowed_keywords=user_create.allowed_keywords,
        created_at=datetime.now(),
        updated_at=None
    )

    # Convert to dict and handle datetime serialization
    user_dict = new_user.model_dump()
    user_dict["created_at"] = user_dict["created_at"].isoformat()
    if user_dict.get("updated_at"):
        user_dict["updated_at"] = user_dict["updated_at"].isoformat()

    data["users"].append(user_dict)
    save_users(data)

    # Return dict directly to avoid serialization issues
    return {
        "id": new_user.id,
        "name": new_user.name,
        "username": new_user.username,
        "email": new_user.email,
        "allowed_keywords": new_user.allowed_keywords,
        "created_at": new_user.created_at.isoformat(),
        "updated_at": None
    }


def update_user(user_id: str, updates: dict) -> Optional[User]:
    """Update a user."""
    data = load_users()

    for i, user_data in enumerate(data.get("users", [])):
        if user_data["id"] == user_id:
            # Update fields
            if "name" in updates:
                data["users"][i]["name"] = updates["name"]
            if "email" in updates:
                data["users"][i]["email"] = updates["email"]
            if "password" in updates:
                data["users"][i]["password_hash"] = hash_password(updates["password"])
            if "allowed_keywords" in updates:
                data["users"][i]["allowed_keywords"] = updates["allowed_keywords"]
            data["users"][i]["updated_at"] = datetime.now().isoformat()

            save_users(data)

            user_data = data["users"][i]
            return {
                "id": user_data["id"],
                "name": user_data["name"],
                "username": user_data["username"],
                "email": user_data["email"],
                "allowed_keywords": user_data["allowed_keywords"],
                "created_at": user_data["created_at"],
                "updated_at": user_data.get("updated_at")
            }

    return None


def delete_user(user_id: str) -> bool:
    """Delete a user."""
    if user_id == "admin":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete admin user"
        )

    data = load_users()
    original_length = len(data.get("users", []))
    data["users"] = [u for u in data.get("users", []) if u["id"] != user_id]

    if len(data["users"]) < original_length:
        save_users(data)
        return True
    return False


def get_all_users() -> list[dict]:
    """Get all users."""
    data = load_users()
    users = []
    for user_data in data.get("users", []):
        users.append({
            "id": user_data["id"],
            "name": user_data["name"],
            "username": user_data["username"],
            "email": user_data["email"],
            "allowed_keywords": user_data.get("allowed_keywords", []),
            "created_at": user_data["created_at"],
            "updated_at": user_data.get("updated_at")
        })
    return users


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_token(token: str) -> TokenData:
    """Decode and validate a JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user_id: str = payload.get("user_id")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return TokenData(username=username, user_id=user_id)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def authenticate_user(username: str, password: str) -> Optional[UserInDB]:
    """Authenticate a user with username and password."""
    user = get_user(username)
    if not user:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


# Dependency for getting current user from token
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    """Get the current authenticated user from the request."""
    token = credentials.credentials
    token_data = decode_token(token)

    user = get_user(token_data.username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return {
        "id": user.id,
        "name": user.name,
        "username": user.username,
        "email": user.email,
        "allowed_keywords": user.allowed_keywords,
        "created_at": user.created_at.isoformat(),
        "updated_at": user.updated_at.isoformat() if user.updated_at else None
    }


# Dependency for requiring admin user
async def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """Require the current user to be admin."""
    if current_user["id"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user


# Initialize user database on module load
init_user_db()
