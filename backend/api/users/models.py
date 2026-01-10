"""
User models for authentication and authorization.
"""
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional
from datetime import datetime
from enum import Enum


class UserRole(str, Enum):
    """User role enumeration."""
    ADMIN = "admin"      # Full access to all data
    OPS = "ops"          # Delivery role - keywords filter Adset
    BUSINESS = "business"  # Business role - keywords filter offer


class UserBase(BaseModel):
    """Base user fields."""
    name: str = Field(..., min_length=1, max_length=100)
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    role: UserRole = Field(default=UserRole.OPS)
    keywords: List[str] = Field(default_factory=list, description="Keywords for filtering data. Empty means no restriction.")


class UserCreate(UserBase):
    """User creation schema."""
    password: str = Field(..., min_length=6)


class UserUpdate(BaseModel):
    """User update schema."""
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    role: Optional[UserRole] = None
    keywords: Optional[List[str]] = None


class UserInDB(UserBase):
    """User with ID (stored in DB)."""
    id: str
    password_hash: str
    created_at: datetime
    updated_at: Optional[datetime] = None


class User(UserBase):
    """User response schema (without password)."""
    id: str
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class LoginRequest(BaseModel):
    """Login request schema."""
    username: str
    password: str


class LoginResponse(BaseModel):
    """Login response with token."""
    access_token: str
    token_type: str = "bearer"
    user: User


class TokenData(BaseModel):
    """Token payload data."""
    username: Optional[str] = None
    user_id: Optional[str] = None
    role: Optional[UserRole] = None
