"""
Dashboard API router for data panel queries.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging

from api.database import get_db, format_row_for_frontend, format_daily_row
from api.models.schemas import (
    DIMENSION_COLUMN_MAP,
    DataQueryRequest,
    DataQueryResponse,
    DailyDataRequest,
    DailyBreakdown,
    PlatformsResponse,
    PlatformInfo,
    DimensionType,
    DateRangeFilter
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _build_group_by_clause(dimensions: List[DimensionType]) -> str:
    """Build GROUP BY clause from dimension types."""
    columns = [DIMENSION_COLUMN_MAP.get(d, d) for d in dimensions]
    return ", ".join(columns)


def _build_where_clause(start_date: str, end_date: str, filters: List[dict]) -> str:
    """Build WHERE clause for query.

    Returns:
        str: where_clause_sql
    """
    conditions = [f"reportDate >= '{start_date}' AND reportDate <= '{end_date}'"]

    for f in filters:
        dim = f.get("dimension")
        value = f.get("value")
        column = DIMENSION_COLUMN_MAP.get(dim, dim)
        # Escape single quotes in value
        escaped_value = value.replace("'", "''")
        conditions.append(f"{column} = '{escaped_value}'")

    return " AND ".join(conditions)


@router.get("/health", response_model=dict)
async def health_check():
    """Health check endpoint."""
    db = get_db()
    try:
        client = db.connect()
        # Test connection with a simple query
        result = client.query(f"SELECT count() FROM {db.database}.{db.table}")
        row_count = result.first_row
        if row_count:
            row_count = row_count[0]

        # Get sample data to debug
        sample_result = client.query(f"SELECT * FROM {db.database}.{db.table} LIMIT 1")
        sample_columns = list(sample_result.column_names) if sample_result.column_names else []

        # Get date range
        date_result = client.query(f"SELECT min(reportDate) as min_date, max(reportDate) as max_date FROM {db.database}.{db.table}")
        date_row = date_result.first_row

        return {
            "status": "healthy",
            "clickhouse": "connected",
            "database": db.database,
            "table": db.table,
            "row_count": row_count,
            "columns": sample_columns,
            "date_range": {"min": date_row[0] if date_row else None, "max": date_row[1] if date_row else None}
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"ClickHouse connection failed: {str(e)}")


@router.get("/platforms", response_model=PlatformsResponse)
async def get_platforms():
    """Get list of available platforms (Media sources)."""
    db = get_db()
    try:
        client = db.connect()
        query = f"""
            SELECT DISTINCT Media as name
            FROM {db.database}.{db.table}
            WHERE Media != '' AND length(Media) > 0
            ORDER BY Media
        """
        result = client.query(query)
        platforms = [PlatformInfo(name=row[0]) for row in result.named_results()]

        return PlatformsResponse(platforms=platforms)
    except Exception as e:
        logger.error(f"Error fetching platforms: {e}")
        # If Media column is empty or query fails, return empty list
        return PlatformsResponse(platforms=[])


@router.get("/data", response_model=DataQueryResponse)
async def get_aggregated_data(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    group_by: str = Query(..., description="Comma-separated dimensions"),
    filters: Optional[str] = Query(None, description="JSON encoded filters"),
    limit: int = Query(1000, description="Max results")
):
    """Get aggregated data for the dashboard.

    Query parameters:
    - start_date: Start date in YYYY-MM-DD format
    - end_date: End date in YYYY-MM-DD format
    - group_by: Comma-separated dimension types (e.g., "platform,advertiser,offer")
    - filters: Optional JSON encoded filter list: [{"dimension":"platform","value":"Clickflare"}]
    - limit: Maximum number of results

    Returns aggregated data grouped by the specified dimensions.
    """
    import json

    # Parse dimensions
    dimensions = [d.strip() for d in group_by.split(",") if d.strip()]
    if not dimensions:
        raise HTTPException(status_code=400, detail="At least one dimension required in group_by")

    # Parse filters
    filter_list = []
    if filters:
        try:
            filter_list = json.loads(filters)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid filters JSON")

    # Build query
    db = get_db()
    try:
        client = db.connect()

        # Get the primary dimension (first in list) for grouping
        primary_dim = dimensions[0]
        primary_column = DIMENSION_COLUMN_MAP.get(primary_dim, primary_dim)

        # Build WHERE clause
        where_clause = _build_where_clause(start_date, end_date, filter_list)

        # Build GROUP BY - use the primary dimension for top-level aggregation
        group_clause = primary_column

        # Build SELECT with aggregations
        # Use aliases to avoid column name conflicts
        query = f"""
            SELECT
                {primary_column} as group_{primary_dim},
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(spend) as spend,
                sum(revenue) as total_revenue,
                sum(m_imp) as m_imp,
                sum(m_clicks) as m_clicks,
                sum(m_conv) as m_conv
            FROM {db.database}.{db.table}
            WHERE {where_clause}
            GROUP BY {group_clause}
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """

        logger.info(f"Executing query: {query[:200]}...")
        # Execute query
        result = client.query(query)

        # Format results
        formatted_data = []
        filter_values = [f.get("value") for f in filter_list]

        for row in result.named_results():
            formatted_row = format_row_for_frontend(
                row,
                dimension_type=primary_dim,
                level=len(filter_list),
                filter_values=filter_values
            )
            # Determine if this row can have children
            formatted_row["hasChild"] = len(dimensions) > 1
            formatted_data.append(formatted_row)

        return DataQueryResponse(
            data=formatted_data,
            total=len(formatted_data),
            dateRange=DateRangeFilter(start_date=start_date, end_date=end_date)
        )

    except Exception as e:
        import traceback
        logger.error(f"Error fetching data: {e}\nQuery: {query[:500] if 'query' in locals() else 'N/A'}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/daily", response_model=List[DailyBreakdown])
async def get_daily_breakdown(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    filters: str = Query(..., description="JSON encoded filters for the row"),
    limit: int = Query(100, description="Max daily records")
):
    """Get daily breakdown data for a specific data row.

    Query parameters:
    - start_date: Start date in YYYY-MM-DD format
    - end_date: End date in YYYY-MM-DD format
    - filters: JSON encoded filter list (required)
    - limit: Maximum number of daily records

    Returns daily data for the filtered entity.
    """
    import json

    # Parse filters
    try:
        filter_list = json.loads(filters)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid filters JSON")

    if not filter_list:
        raise HTTPException(status_code=400, detail="Filters are required for daily breakdown")

    db = get_db()
    try:
        client = db.connect()

        # Build WHERE clause
        where_clause = _build_where_clause(start_date, end_date, filter_list)

        query = f"""
            SELECT
                reportDate as date,
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(spend) as spend,
                sum(revenue) as total_revenue,
                sum(m_imp) as m_imp,
                sum(m_clicks) as m_clicks,
                sum(m_conv) as m_conv
            FROM {db.database}.{db.table}
            WHERE {where_clause}
            GROUP BY reportDate
            ORDER BY reportDate DESC
            LIMIT {limit}
        """

        result = client.query(query)

        daily_data = []
        for row in result.named_results():
            daily_data.append(format_daily_row(row))

        return daily_data

    except Exception as e:
        logger.error(f"Error fetching daily data: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching daily data: {str(e)}")


@router.get("/dimensions")
async def get_available_dimensions():
    """Get list of available dimensions for grouping."""
    dimensions = [
        {"value": "platform", "label": "Media"},
        {"value": "advertiser", "label": "Advertiser"},
        {"value": "offer", "label": "Offer"},
        {"value": "campaign_name", "label": "Campaign"},
        {"value": "sub_campaign_name", "label": "Adset"},
        {"value": "creative_name", "label": "Ads"},
    ]
    return {"dimensions": dimensions}


@router.get("/metrics")
async def get_available_metrics():
    """Get list of available metrics."""
    metrics = [
        {"key": "spend", "label": "Spend", "type": "money", "group": "Basic"},
        {"key": "conversions", "label": "Conversions", "type": "number", "group": "Basic"},
        {"key": "revenue", "label": "Revenue", "type": "money", "group": "Basic"},
        {"key": "impressions", "label": "Impressions", "type": "number", "group": "Basic"},
        {"key": "clicks", "label": "Clicks", "type": "number", "group": "Basic"},
        {"key": "m_imp", "label": "m_imp", "type": "number", "group": "Basic"},
        {"key": "m_clicks", "label": "m_clicks", "type": "number", "group": "Basic"},
        {"key": "m_conv", "label": "m_conv", "type": "number", "group": "Basic"},
        {"key": "ctr", "label": "CTR", "type": "percent", "group": "Calculated"},
        {"key": "cvr", "label": "CVR", "type": "percent", "group": "Calculated"},
        {"key": "roi", "label": "ROI", "type": "percent", "group": "Calculated"},
        {"key": "cpa", "label": "CPA", "type": "money", "group": "Calculated"},
    ]
    return {"metrics": metrics}


@router.get("/aggregate")
async def get_aggregate_summary(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    filters: Optional[str] = Query(None, description="JSON encoded filters")
):
    """Get summary metrics (aggregated across all data matching filters).

    Useful for showing total stats at the top of the dashboard.
    """
    import json

    filter_list = []
    if filters:
        try:
            filter_list = json.loads(filters)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid filters JSON")

    db = get_db()
    try:
        client = db.connect()

        where_clause = _build_where_clause(start_date, end_date, filter_list)

        query = f"""
            SELECT
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(spend) as spend,
                sum(revenue) as total_revenue,
                sum(m_imp) as m_imp,
                sum(m_clicks) as m_clicks,
                sum(m_conv) as m_conv
            FROM {db.database}.{db.table}
            WHERE {where_clause}
        """

        result = client.query(query)
        row = result.first_row

        if row:
            impressions, clicks, conversions, spend, revenue, m_imp, m_clicks, m_conv = row

            # Calculate computed metrics
            ctr = clicks / (impressions or 1)
            cvr = conversions / (clicks or 1)
            roi = (revenue - spend) / (spend or 1)
            cpa = spend / (conversions or 1)
        else:
            impressions = clicks = conversions = spend = revenue = 0
            m_imp = m_clicks = m_conv = 0
            ctr = cvr = roi = cpa = 0

        return {
            "impressions": impressions or 0,
            "clicks": clicks or 0,
            "conversions": conversions or 0,
            "spend": spend or 0.0,
            "revenue": revenue or 0.0,
            "m_imp": m_imp or 0,
            "m_clicks": m_clicks or 0,
            "m_conv": m_conv or 0,
            "ctr": ctr,
            "cvr": cvr,
            "roi": roi,
            "cpa": cpa,
        }

    except Exception as e:
        logger.error(f"Error fetching aggregate: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching aggregate: {str(e)}")
