"""
User models for authentication and authorization.
"""
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Literal
from datetime import datetime


# Role type as string literal
UserRole = Literal['admin', 'ops', 'business']


class UserBase(BaseModel):
    """Base user fields."""
    name: str = Field(..., min_length=1, max_length=100)
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., pattern=r'^[^@]+@[^@]+\.[^@]+$')
    role: UserRole = 'ops'
    keywords: List[str] = Field(default_factory=list, description="Keywords for filtering data. Empty means no restriction.")


class UserCreate(UserBase):
    """User creation schema."""
    password: str = Field(..., min_length=6)


class UserUpdate(BaseModel):
    """User update schema."""
    name: Optional[str] = None
    email: Optional[str] = None
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


# ============= Saved View Models =============

class SavedViewCreate(BaseModel):
    """Create saved view schema."""
    name: str = Field(..., min_length=1, max_length=100)
    dimensions: List[str] = Field(default_factory=list)
    visible_metrics: List[str] = Field(default_factory=list)
    color_mode: bool = False
    is_default: bool = False


class SavedViewUpdate(BaseModel):
    """Update saved view schema."""
    name: Optional[str] = None
    dimensions: Optional[List[str]] = None
    visible_metrics: Optional[List[str]] = None
    color_mode: Optional[bool] = None
    is_default: Optional[bool] = None


class SavedViewResponse(BaseModel):
    """Saved view response schema."""
    id: str
    user_id: str
    name: str
    dimensions: List[str]
    visible_metrics: List[str]
    color_mode: bool
    is_default: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
