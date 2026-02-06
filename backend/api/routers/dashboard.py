"""
Dashboard API router for data panel queries.

Includes role-based data filtering:
- admin: sees all data
- ops: filtered by Adset keywords
- ops02: filtered by Media (platform) keywords
- business: filtered by offer keywords
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
import logging

from api.database import get_db, format_row_for_frontend, format_daily_row
from api.auth import get_current_user
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
from api.cache import cache_key, get_cache, set_cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _build_group_by_clause(dimensions: List[DimensionType]) -> str:
    """Build GROUP BY clause from dimension types."""
    columns = [DIMENSION_COLUMN_MAP.get(d, d) for d in dimensions]
    return ", ".join(columns)


def _build_permission_filter(user_role: str, user_keywords: List[str]) -> Optional[str]:
    """Build permission filter SQL based on user role and keywords.

    Args:
        user_role: User role (admin, ops, ops02, business)
        user_keywords: User's keywords list

    Returns:
        SQL WHERE clause fragment, or None if no filtering needed
    """
    # Admin or empty keywords = no restriction
    if user_role == 'admin' or not user_keywords:
        logger.debug(f"[Permission] No filtering: role={user_role}, keywords={user_keywords}")
        return None

    # Build keyword filter based on role (use actual ClickHouse column names)
    if user_role == 'ops':
        # Filter by Adset column
        keyword_conditions = [f"lower(Adset) LIKE lower('%{k}%')" for k in user_keywords]
        filter_sql = f"({' OR '.join(keyword_conditions)})"
        logger.debug(f"[Permission] ops role filter: {filter_sql}")
        return filter_sql
    elif user_role == 'ops02':
        # Filter by Media column (platform dimension)
        keyword_conditions = [f"lower(Media) LIKE lower('%{k}%')" for k in user_keywords]
        filter_sql = f"({' OR '.join(keyword_conditions)})"
        logger.debug(f"[Permission] ops02 role filter: {filter_sql}")
        return filter_sql
    elif user_role == 'business':
        # Filter by offer column
        keyword_conditions = [f"lower(offer) LIKE lower('%{k}%')" for k in user_keywords]
        filter_sql = f"({' OR '.join(keyword_conditions)})"
        logger.debug(f"[Permission] business role filter: {filter_sql}")
        return filter_sql

    return None


def _build_where_clause(start_date: str, end_date: str, filters: List[dict], permission_filter: Optional[str] = None) -> str:
    """Build WHERE clause for query.

    Args:
        start_date: Start date string
        end_date: End date string
        filters: List of filter dicts with dimension and value
        permission_filter: Optional permission filter SQL

    Returns:
        str: where_clause_sql
    """
    conditions = [f"reportDate >= '{start_date}' AND reportDate <= '{end_date}'"]

    # Add permission filter if present
    if permission_filter:
        conditions.append(permission_filter)

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
async def get_platforms(current_user: dict = Depends(get_current_user)):
    """Get list of available platforms (Media sources)."""
    db = get_db()
    try:
        client = db.connect()

        # Build permission filter
        user_role = current_user.get("role")
        user_keywords = current_user.get("keywords", [])
        permission_filter = _build_permission_filter(user_role, user_keywords)

        base_query = f"""
            SELECT DISTINCT Media as name
            FROM {db.database}.{db.table}
            WHERE Media != '' AND length(Media) > 0
        """

        # Add permission filter if present
        if permission_filter:
            base_query += f" AND {permission_filter}"

        base_query += " ORDER BY Media"

        result = client.query(base_query)
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
    limit: int = Query(1000, description="Max results"),
    current_user: dict = Depends(get_current_user)
):
    """Get aggregated data for the dashboard with role-based filtering.

    Query parameters:
    - start_date: Start date in YYYY-MM-DD format
    - end_date: End date in YYYY-MM-DD format
    - group_by: Comma-separated dimension types (e.g., "platform,advertiser,offer")
    - filters: Optional JSON encoded filter list: [{"dimension":"platform","value":"Clickflare"}]
    - limit: Maximum number of results

    Returns aggregated data grouped by the specified dimensions.
    Data is filtered based on user role and keywords.
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

    user_id = current_user.get("id", "anonymous")
    cache_key_val = cache_key("data", start_date, end_date, dimensions, filter_list, user_id)

    # Define the data fetcher function for cache refresh
    def fetch_data():
        """Fetch data from ClickHouse - used for initial load and async refresh."""
        db = get_db()
        client = db.connect()

        # Get the primary dimension (first in list) for grouping
        primary_dim = dimensions[0]
        primary_column = DIMENSION_COLUMN_MAP.get(primary_dim, primary_dim)

        # Build permission filter based on user role
        user_role = current_user.get("role")
        user_keywords = current_user.get("keywords", [])
        # IMPORTANT: Log user info for debugging
        logger.info(f"[DATA API] User: id={current_user.get('id')}, role={user_role}, keywords={user_keywords}, group_by={dimensions}")
        permission_filter = _build_permission_filter(user_role, user_keywords)

        # Build WHERE clause with permission filter
        where_clause = _build_where_clause(start_date, end_date, filter_list, permission_filter)

        # Build GROUP BY - use the primary dimension for top-level aggregation
        group_clause = primary_column

        # Check if we need to JOIN lander URL mapping (when primary dimension is lander)
        is_lander_dimension = primary_dim == "lander"
        # Check if we need to include offerID (when primary dimension is offer)
        is_offer_dimension = primary_dim == "offer"

        if is_lander_dimension:
            # For lander dimension, JOIN with the URL mapping table
            query = f"""
                SELECT
                    t.lander as group_{primary_dim},
                    m.landerUrl as landerUrl,
                    sum(t.impressions) as impressions,
                    sum(t.clicks) as clicks,
                    sum(t.conversions) as conversions,
                    sum(t.spend) as spend,
                    sum(t.revenue) as total_revenue,
                    sum(t.m_imp) as m_imp,
                    sum(t.m_clicks) as m_clicks,
                    sum(t.m_conv) as m_conv
                FROM {db.database}.{db.table} t
                LEFT JOIN {db.database}.dim_lander_url_mapping m ON t.landerID = m.landerID
                WHERE {where_clause}
                GROUP BY t.lander, m.landerUrl
                ORDER BY total_revenue DESC
                LIMIT {limit}
                SETTINGS max_memory_usage=2000000000, max_threads=4
            """
        elif is_offer_dimension:
            # For offer dimension, include offerID for matching with offer details
            query = f"""
                SELECT
                    {primary_column} as group_{primary_dim},
                    MAX(offerID) as offerID,
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
                SETTINGS max_memory_usage=2000000000, max_threads=4
            """
        else:
            # Build SELECT with aggregations (non-lander and non-offer dimension)
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
                SETTINGS max_memory_usage=2000000000, max_threads=4
            """

        logger.info(f"Executing query: {query[:200]}...")
        # Execute query
        result = client.query(query)

        # Format results
        formatted_data = []
        filter_values = [f.get("value") for f in filter_list]

        for idx, row in enumerate(result.named_results()):
            # Debug: log raw row for offer dimension
            if idx < 3 and primary_dim == "offer":
                logger.info(f"[DATA API] Raw row {idx}: columns={list(row.keys())}, offerID={row.get('offerID')}, name={row.get(f'group_{primary_dim}')}")
            formatted_row = format_row_for_frontend(
                row,
                dimension_type=primary_dim,
                level=len(filter_list),
                filter_values=filter_values,
                all_dimensions=dimensions,
                filter_list=filter_list
            )
            # Debug: log formatted row
            if idx < 3 and primary_dim == "offer":
                logger.info(f"[DATA API] Formatted row {idx}: has offerID={'offerID' in formatted_row}, offerID={formatted_row.get('offerID')}, keys={list(formatted_row.keys())[:15]}")
            # Determine if this row can have children
            formatted_row["hasChild"] = len(dimensions) > 1
            formatted_data.append(formatted_row)

        return DataQueryResponse(
            data=formatted_data,
            total=len(formatted_data),
            dateRange=DateRangeFilter(start_date=start_date, end_date=end_date)
        ).dict()

    # Check cache with async refresh capability
    cached_result = get_cache(cache_key_val, refresher=fetch_data)
    if cached_result is not None:
        return DataQueryResponse(**cached_result)

    # Cache miss - fetch and store
    try:
        response_data = fetch_data()
        set_cache(cache_key_val, response_data)
        return DataQueryResponse(**response_data)

    except Exception as e:
        import traceback
        logger.error(f"Error fetching data: {e}\nQuery: {query[:500] if 'query' in locals() else 'N/A'}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/daily", response_model=List[DailyBreakdown])
async def get_daily_breakdown(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    filters: str = Query(..., description="JSON encoded filters for the row"),
    limit: int = Query(100, description="Max daily records"),
    current_user: dict = Depends(get_current_user)
):
    """Get daily breakdown data for a specific data row with role-based filtering.

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

        # Build permission filter based on user role
        user_role = current_user.get("role")
        user_keywords = current_user.get("keywords", [])
        # IMPORTANT: Log user info for debugging
        logger.info(f"[DAILY API] User: id={current_user.get('id')}, role={user_role}, keywords={user_keywords}, filters={filter_list}")
        permission_filter = _build_permission_filter(user_role, user_keywords)

        # Build WHERE clause with permission filter
        where_clause = _build_where_clause(start_date, end_date, filter_list, permission_filter)

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
        {"value": "lander", "label": "Lander"},
        {"value": "campaign_name", "label": "Campaign"},
        {"value": "sub_campaign_name", "label": "Adset"},
        {"value": "creative_name", "label": "Ads"},
        {"value": "date", "label": "Date"},
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
    filters: Optional[str] = Query(None, description="JSON encoded filters"),
    current_user: dict = Depends(get_current_user)
):
    """Get summary metrics (aggregated across all data matching filters) with role-based filtering.

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

        # Build permission filter based on user role
        user_role = current_user.get("role")
        user_keywords = current_user.get("keywords", [])
        permission_filter = _build_permission_filter(user_role, user_keywords)

        # Build WHERE clause with permission filter
        where_clause = _build_where_clause(start_date, end_date, filter_list, permission_filter)

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
            roi = (revenue - spend) / spend if spend > 0 else 0
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


@router.get("/hierarchy")
async def get_data_hierarchy(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    dimensions: str = Query(..., description="Comma-separated dimensions"),
    current_user: dict = Depends(get_current_user)
):
    """
    Preload all hierarchy data at once.
    Returns a nested structure with all levels for the given dimensions.

    This reduces API calls from N+1 to just 1 request.
    """
    # Parse dimensions
    dim_list = [d.strip() for d in dimensions.split(",") if d.strip()]
    if not dim_list:
        raise HTTPException(status_code=400, detail="At least one dimension required")

    user_id = current_user.get("id", "anonymous")
    cache_key_val = cache_key("hierarchy", start_date, end_date, dim_list, user_id)

    # Define the hierarchy fetcher function for cache refresh
    def fetch_hierarchy():
        """Fetch hierarchy data from ClickHouse - used for initial load and async refresh."""
        # Build permission filter
        user_role = current_user.get("role")
        user_keywords = current_user.get("keywords", [])
        # IMPORTANT: Log user info for debugging
        logger.info(f"[HIERARCHY API] User: id={current_user.get('id')}, role={user_role}, keywords={user_keywords}, dimensions={dim_list}")
        permission_filter = _build_permission_filter(user_role, user_keywords)

        db = get_db()
        client = db.connect()

        # Check if special dimensions are included
        has_lander = "lander" in dim_list
        has_offer = "offer" in dim_list

        if has_lander:
            # Build query with JOIN to get landerUrl for all dimensions
            # Include all dimension columns in SELECT and GROUP BY
            columns = [DIMENSION_COLUMN_MAP.get(d, d) for d in dim_list]

            # Find lander column index and name for special handling
            lander_idx = dim_list.index("lander")
            lander_column = DIMENSION_COLUMN_MAP.get("lander", "lander")

            # Build SELECT: all dimension columns + landerUrl + offerID (if offer exists)
            # We need to include landerUrl with MAX to avoid GROUP BY issues
            select_parts = []
            for i, col in enumerate(columns):
                if i == lander_idx:
                    # For lander column, we still select it normally
                    select_parts.append(f"t.{lander_column}")
                else:
                    select_parts.append(f"t.{col}")

            # Add landerUrl and optionally offerID
            extra_selects = ["MAX(m.landerUrl) as landerUrl"]
            if has_offer:
                extra_selects.append("MAX(t.offerID) as offerID")

            base_query = f"""
                SELECT
                    {', '.join(select_parts)},
                    {', '.join(extra_selects)},
                    sum(t.impressions) as impressions,
                    sum(t.clicks) as clicks,
                    sum(t.conversions) as conversions,
                    sum(t.spend) as spend,
                    sum(t.revenue) as revenue,
                    sum(t.m_imp) as m_imp,
                    sum(t.m_clicks) as m_clicks,
                    sum(t.m_conv) as m_conv
                FROM {db.database}.{db.table} t
                LEFT JOIN {db.database}.dim_lander_url_mapping m ON t.landerID = m.landerID
                WHERE t.reportDate >= '{start_date}' AND t.reportDate <= '{end_date}'
            """

            # Add permission filter
            if permission_filter:
                base_query += f" AND {permission_filter}"

            # GROUP BY all dimension columns (not landerUrl, we use MAX)
            group_by_clause = ", ".join([f"t.{col}" for col in columns])
            base_query += f" GROUP BY {group_by_clause} ORDER BY revenue DESC"
        elif has_offer:
            # Build query with offerID for offer dimension matching
            columns = [DIMENSION_COLUMN_MAP.get(d, d) for d in dim_list]
            group_by_clause = ", ".join(columns)

            # For offer dimension, we need to include offerID using MAX
            base_query = f"""
                SELECT
                    {', '.join(columns)},
                    MAX(offerID) as offerID,
                    sum(impressions) as impressions,
                    sum(clicks) as clicks,
                    sum(conversions) as conversions,
                    sum(spend) as spend,
                    sum(revenue) as revenue,
                    sum(m_imp) as m_imp,
                    sum(m_clicks) as m_clicks,
                    sum(m_conv) as m_conv
                FROM {db.database}.{db.table}
                WHERE reportDate >= '{start_date}' AND reportDate <= '{end_date}'
            """

            # Add permission filter
            if permission_filter:
                base_query += f" AND {permission_filter}"

            base_query += f" GROUP BY {group_by_clause} ORDER BY revenue DESC"
        else:
            # Fetch all data grouped by all dimensions at once (no lander, no offer)
            columns = [DIMENSION_COLUMN_MAP.get(d, d) for d in dim_list]
            group_by_clause = ", ".join(columns)

            base_query = f"""
                SELECT
                    {', '.join(columns)},
                    sum(impressions) as impressions,
                    sum(clicks) as clicks,
                    sum(conversions) as conversions,
                    sum(spend) as spend,
                    sum(revenue) as revenue,
                    sum(m_imp) as m_imp,
                    sum(m_clicks) as m_clicks,
                    sum(m_conv) as m_conv
                FROM {db.database}.{db.table}
                WHERE reportDate >= '{start_date}' AND reportDate <= '{end_date}'
            """

            # Add permission filter
            if permission_filter:
                base_query += f" AND {permission_filter}"

            base_query += f" GROUP BY {group_by_clause} ORDER BY revenue DESC"

        result = client.query(base_query)

        # Build hierarchy structure
        hierarchy = {}
        row_count = 0

        for row in result.named_results():
            row_count += 1
            # Debug: log first few rows to see what columns we have
            if row_count <= 3:
                logger.info(f"[HIERARCHY ROW {row_count}] columns: {list(row.keys())}, offerID={row.get('offerID')}, landerUrl={row.get('landerUrl')}")
            # Build nested path
            current_level = hierarchy
            for i, dim in enumerate(dim_list):
                col_name = DIMENSION_COLUMN_MAP.get(dim, dim)
                value = row.get(col_name, "Unknown")
                if value == "" or value is None:
                    value = "Unknown"

                # Create key for this level
                level_key = value

                is_last = (i == len(dim_list) - 1)

                if level_key not in current_level:
                    # Create new node with metrics
                    node_data = {
                        "_metrics": {
                            "impressions": row.get("impressions", 0),
                            "clicks": row.get("clicks", 0),
                            "conversions": row.get("conversions", 0),
                            "spend": row.get("spend", 0),
                            "revenue": row.get("revenue", 0),
                            "profit": (row.get("revenue", 0) or 0) - (row.get("spend", 0) or 0),
                            "m_imp": row.get("m_imp", 0),
                            "m_clicks": row.get("m_clicks", 0),
                            "m_conv": row.get("m_conv", 0),
                            "ctr": (row.get("clicks", 0) or 0) / (row.get("impressions", 0) or 1),
                            "cvr": (row.get("conversions", 0) or 0) / (row.get("clicks", 0) or 1),
                            "roi": ((row.get("revenue", 0) or 0) - (row.get("spend", 0) or 0)) / (row.get("spend", 0) or 0) if (row.get("spend", 0) or 0) > 0 else 0,
                            "cpa": (row.get("spend", 0) or 0) / (row.get("conversions", 0) or 1),
                        },
                        "_dimension": dim,
                        "_children": {} if not is_last else None
                    }
                    # Add landerUrl for lander dimension nodes (include empty strings)
                    if dim == "lander" and row.get("landerUrl") is not None:
                        node_data["landerUrl"] = row.get("landerUrl")
                        # Debug log for hierarchy landerUrl
                        logger.info(f"[HIERARCHY LANDER] name={level_key}, landerUrl={row.get('landerUrl')}")
                    # Add offerID for offer dimension nodes
                    if dim == "offer" and row.get("offerID") is not None:
                        node_data["offerID"] = row.get("offerID")
                        # Debug log for hierarchy offerID
                        logger.info(f"[HIERARCHY OFFER] name={level_key}, offerID={row.get('offerID')}")
                    current_level[level_key] = node_data
                else:
                    # Node exists - accumulate metrics for aggregation
                    existing_metrics = current_level[level_key]["_metrics"]
                    existing_metrics["impressions"] = (existing_metrics.get("impressions", 0) or 0) + (row.get("impressions", 0) or 0)
                    existing_metrics["clicks"] = (existing_metrics.get("clicks", 0) or 0) + (row.get("clicks", 0) or 0)
                    existing_metrics["conversions"] = (existing_metrics.get("conversions", 0) or 0) + (row.get("conversions", 0) or 0)
                    existing_metrics["spend"] = (existing_metrics.get("spend", 0) or 0) + (row.get("spend", 0) or 0)
                    existing_metrics["revenue"] = (existing_metrics.get("revenue", 0) or 0) + (row.get("revenue", 0) or 0)
                    existing_metrics["profit"] = (existing_metrics.get("revenue", 0) or 0) - (existing_metrics.get("spend", 0) or 0)
                    existing_metrics["m_imp"] = (existing_metrics.get("m_imp", 0) or 0) + (row.get("m_imp", 0) or 0)
                    existing_metrics["m_clicks"] = (existing_metrics.get("m_clicks", 0) or 0) + (row.get("m_clicks", 0) or 0)
                    existing_metrics["m_conv"] = (existing_metrics.get("m_conv", 0) or 0) + (row.get("m_conv", 0) or 0)
                    # Recalculate ratios
                    imp = existing_metrics.get("impressions", 0) or 1
                    clicks = existing_metrics.get("clicks", 0) or 1
                    conversions = existing_metrics.get("conversions", 0) or 1
                    spend = existing_metrics.get("spend", 0) or 0
                    revenue = existing_metrics.get("revenue", 0) or 0
                    existing_metrics["ctr"] = clicks / imp
                    existing_metrics["cvr"] = conversions / clicks
                    existing_metrics["roi"] = (revenue - spend) / spend if spend > 0 else 0
                    existing_metrics["cpa"] = spend / conversions

                # Move to next level
                if not is_last and current_level[level_key].get("_children") is not None:
                    current_level = current_level[level_key]["_children"]

        # Log hierarchy build results
        top_level_count = len(hierarchy)
        logger.info(f"[HIERARCHY BUILD] dim_list={dim_list}, total_rows={row_count}, top_level_nodes={top_level_count}, keys={list(hierarchy.keys())[:10]}")

        # Debug: Check if offerID is in the final hierarchy
        sample_keys = list(hierarchy.keys())[:3]
        for key in sample_keys:
            node = hierarchy[key]
            offer_id_in_node = node.get("offerID", "MISSING")
            logger.info(f"[HIERARCHY FINAL] key={key}, offerID={offer_id_in_node}, node_fields={list(node.keys())}")

        return {
            "dimensions": dim_list,
            "hierarchy": hierarchy,
            "startDate": start_date,
            "endDate": end_date
        }

    # Check cache with async refresh capability
    cached_result = get_cache(cache_key_val, refresher=fetch_hierarchy)
    if cached_result is not None:
        return cached_result

    # Cache miss - fetch and store
    try:
        result = fetch_hierarchy()
        set_cache(cache_key_val, result)
        return result
    except Exception as e:
        logger.error(f"Error fetching hierarchy: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching hierarchy: {str(e)}")


@router.get("/etl-status")
async def get_etl_status():
    """Get ETL status from Redis."""
    from api.cache import get_cache as redis_get_cache

    try:
        status = redis_get_cache("etl:last_update")
        if status:
            return status
    except Exception as e:
        logger.error(f"Error getting ETL status from cache: {e}")

    return {
        "last_update": None,
        "report_date": None,
        "all_success": False
    }

