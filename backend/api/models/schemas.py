"""
Pydantic schemas for API request/response validation.
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import date


# Dimension types matching frontend
DimensionType = Literal[
    "platform",
    "advertiser",
    "offer",
    "lander",
    "campaign_name",
    "sub_campaign_name",
    "creative_name",
    "date"
]

# Frontend dimension name to ClickHouse column mapping
DIMENSION_COLUMN_MAP = {
    "platform": "Media",
    "advertiser": "advertiser",
    "offer": "offer",
    "lander": "lander",
    "campaign_name": "Campaign",
    "sub_campaign_name": "Adset",
    "creative_name": "Ads",
    "date": "reportDate",
}


class DateRangeFilter(BaseModel):
    """Date range filter for queries."""
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")


class DimensionFilter(BaseModel):
    """Dimension filter for queries."""
    dimension: DimensionType
    value: str


class DataQueryRequest(DateRangeFilter):
    """Request for aggregated data query."""
    group_by: List[DimensionType] = Field(
        default=["platform"],
        description="Dimensions to group by (in order for hierarchy)"
    )
    filters: List[DimensionFilter] = Field(
        default_factory=list,
        description="Filters to apply"
    )
    limit: Optional[int] = Field(default=1000, description="Max results to return")


class DailyDataRequest(DateRangeFilter):
    """Request for daily breakdown data."""
    filters: List[DimensionFilter] = Field(
        default_factory=list,
        description="Filters for the specific row to get daily data for"
    )
    limit: Optional[int] = Field(default=100, description="Max daily records to return")


class DailyBreakdown(BaseModel):
    """Daily breakdown data."""
    date: str
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    spend: float = 0.0
    revenue: float = 0.0
    m_imp: int = 0
    m_clicks: int = 0
    m_conv: int = 0


class AdRow(BaseModel):
    """Data row matching frontend AdRow interface."""
    id: str
    name: str
    level: int
    dimensionType: DimensionType
    impressions: int
    clicks: int
    conversions: int
    spend: float
    revenue: float
    m_imp: int
    m_clicks: int
    m_conv: int
    # Computed metrics
    ctr: float
    cvr: float
    roi: float
    cpa: float
    rpa: float
    epc: float
    epv: float
    m_epc: float
    m_epv: float
    m_cpc: float
    m_cpv: float
    hasChild: bool
    isExpanded: Optional[bool] = None
    # Optional fields for specific dimensions
    landerUrl: Optional[str] = None  # Lander URL for lander dimension
    filterPath: Optional[List[DimensionFilter]] = None  # Full filter path for hierarchy navigation


class DataQueryResponse(BaseModel):
    """Response for data query."""
    data: List[AdRow]
    total: int
    dateRange: DateRangeFilter


class PlatformInfo(BaseModel):
    """Platform/Media source information."""
    name: str
    displayName: Optional[str] = None


class PlatformsResponse(BaseModel):
    """Response for platforms list."""
    platforms: List[PlatformInfo]


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    clickhouse: str
    database: str
    table: str


class ErrorResponse(BaseModel):
    """Error response."""
    error: str
    detail: Optional[str] = None
