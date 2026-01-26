"""
ClickHouse database connection and query module with connection pooling.
"""
from typing import Optional, List, Dict, Any
import clickhouse_connect
from contextlib import contextmanager
import yaml
import os
import threading
import logging

logger = logging.getLogger(__name__)


class ClickHousePool:
    """ClickHouse connection pool for managing multiple concurrent connections."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        secure: bool = False,
        pool_size: int = 10,
        connect_timeout: int = 15,
        send_receive_timeout: int = 60
    ):
        """Initialize connection pool.

        Args:
            host: ClickHouse host
            port: ClickHouse port
            database: Database name
            username: Username
            password: Password
            secure: Use SSL/TLS
            pool_size: Maximum number of connections in pool
            connect_timeout: Connection timeout in seconds
            send_receive_timeout: Query timeout in seconds
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.secure = secure
        self.pool_size = pool_size
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout

        self._pool: List[clickhouse_connect.driver.Client] = []
        self._lock = threading.Lock()
        self._created = 0

    def _create_client(self) -> clickhouse_connect.driver.Client:
        """Create a new ClickHouse client connection."""
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            secure=self.secure,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
        )

    def get_client(self) -> clickhouse_connect.driver.Client:
        """Get a connection from the pool.

        Returns:
            ClickHouse client connection. If pool is exhausted, creates a temporary connection.
        """
        with self._lock:
            if self._pool:
                return self._pool.pop()
            if self._created < self.pool_size:
                self._created += 1
                return self._create_client()
            # Pool exhausted - create temporary connection (will be closed when returned)
            logger.warning("Connection pool exhausted, creating temporary connection")
            return self._create_client()

    def return_client(self, client: clickhouse_connect.driver.Client):
        """Return a connection to the pool.

        Args:
            client: The client connection to return
        """
        with self._lock:
            # Check if we have room in pool for this connection
            if len(self._pool) < self.pool_size and self._created >= self.pool_size:
                self._pool.append(client)
            else:
                # Pool is full or this was a temporary connection - close it
                try:
                    client.close()
                except Exception as e:
                    logger.debug(f"Error closing temporary connection: {e}")

    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for client in self._pool:
                try:
                    client.close()
                except Exception:
                    pass
            self._pool.clear()
            self._created = 0

    @contextmanager
    def connection(self):
        """Context manager for getting and returning a connection.

        Usage:
            with pool.connection() as client:
                result = client.query("SELECT * FROM table")
        """
        client = self.get_client()
        try:
            yield client
        finally:
            self.return_client(client)


# Global connection pool instance
_ch_pool: Optional[ClickHousePool] = None


def get_connection_pool() -> ClickHousePool:
    """Get or create the global connection pool instance."""
    global _ch_pool
    if _ch_pool is None:
        # Create from ClickHouseClient configuration
        client = ClickHouseClient()
        _ch_pool = ClickHousePool(
            host=client.host,
            port=client.port,
            database=client.database,
            username=client.username,
            password=client.password,
            secure=client.secure,
            pool_size=10,           # Maximum 10 connections
            connect_timeout=15,     # 15 seconds to connect
            send_receive_timeout=60 # 60 seconds for query
        )
        logger.info(f"ClickHouse connection pool created: host={client.host}, pool_size=10")
    return _ch_pool


class ClickHouseClient:
    """ClickHouse client wrapper for API service."""

    def __init__(self, config_path: str = None):
        """Initialize ClickHouse client from config file."""
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        ch_config = config.get("clickhouse", {})
        # Also try to read from existing ETL configs
        if not ch_config:
            # Fallback to clickflare_etl config
            cf_config_path = os.path.join(os.path.dirname(__file__), "../clickflare_etl/config.yaml")
            if os.path.exists(cf_config_path):
                with open(cf_config_path, "r", encoding="utf-8") as cf:
                    cf_config = yaml.safe_load(cf)
                    ch_config = cf_config.get("clickhouse", {})

        self.host = ch_config.get("host", "localhost")
        self.port = ch_config.get("port", 8123)
        self.database = ch_config.get("database", "ad_platform")
        self.table = ch_config.get("table", "dwd_marketing_report_daily")
        self.username = ch_config.get("username", "default")
        self.password = ch_config.get("password", "")
        self.secure = ch_config.get("secure", False)

        self._client: Optional[clickhouse_connect.driver.Client] = None

    def connect(self) -> clickhouse_connect.driver.Client:
        """Establish and return ClickHouse connection (deprecated - use get_client context manager)."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                secure=self.secure,
                connect_timeout=15,
                send_receive_timeout=60
            )
        return self._client

    @contextmanager
    def get_client(self):
        """Context manager for ClickHouse client using connection pool."""
        pool = get_connection_pool()
        with pool.connection() as client:
            yield client

    def close(self):
        """Close ClickHouse connection."""
        if self._client:
            self._client.close()
            self._client = None


# Global client instance
_ch_client: Optional[ClickHouseClient] = None


def get_db() -> ClickHouseClient:
    """Get global ClickHouse client instance."""
    global _ch_client
    if _ch_client is None:
        _ch_client = ClickHouseClient()
    return _ch_client


def build_date_filter(start_date: str, end_date: str) -> str:
    """Build SQL date filter clause."""
    return f"reportDate BETWEEN '{start_date}' AND '{end_date}'"


def build_filters(filters: Dict[str, Any]) -> tuple[str, List[str]]:
    """Build SQL WHERE clause from filters dict.

    Returns:
        tuple: (where_clause, params)
    """
    conditions = []
    params = []

    # Map frontend dimension names to ClickHouse columns
    column_mapping = {
        "platform": "Media",
        "advertiser": "advertiser",
        "offer": "offer",
        "campaign_name": "Campaign",
        "sub_campaign_name": "Adset",
        "creative_name": "Ads",
    }

    for dim, value in filters.items():
        column = column_mapping.get(dim, dim)
        conditions.append(f"{column} = ?")
        params.append(value)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    return where_clause, params


def format_row_for_frontend(row: Dict[str, Any], dimension_type: str, level: int = 0, filter_values: List[str] = None, all_dimensions: List[str] = None, filter_list: List[Dict] = None) -> Dict[str, Any]:
    """Format ClickHouse row for frontend consumption.

    Args:
        row: Raw row from ClickHouse
        dimension_type: The dimension type for this row
        level: Hierarchy level
        filter_values: Parent filter values for building ID
        all_dimensions: Complete list of dimensions in the hierarchy (for building filterPath)
        filter_list: Parent filter list with dimension and value

    Returns:
        Formatted dict matching frontend AdRow interface
    """
    # Build unique ID from filter values + current dimension value
    filter_values = filter_values or []
    dim_value = row.get(f"group_{dimension_type}", "Unknown")
    # Convert dim_value to string (handles datetime.date for 'date' dimension)
    dim_value = str(dim_value) if dim_value is not None else "Unknown"
    row_id = "|".join(filter_values + [dim_value]) if filter_values else dim_value

    # Get metrics
    impressions = row.get("impressions", 0) or 0
    clicks = row.get("clicks", 0) or 0
    conversions = row.get("conversions", 0) or 0
    spend = row.get("spend", 0) or 0
    revenue = row.get("total_revenue", 0) or row.get("revenue", 0) or 0  # Support both column names
    m_imp = row.get("m_imp", 0) or 0
    m_clicks = row.get("m_clicks", 0) or 0
    m_conv = row.get("m_conv", 0) or 0

    # Calculate computed metrics
    ctr = clicks / (impressions or 1)
    cvr = conversions / (clicks or 1)
    roi = (revenue - spend) / (spend or 1)
    cpa = spend / (conversions or 1)
    rpa = revenue / (conversions or 1)
    epc = revenue / (clicks or 1)
    epv = revenue / (impressions or 1)
    m_epc = revenue / (m_clicks or 1)
    m_epv = revenue / (m_imp or 1)
    m_cpc = spend / (m_clicks or 1)
    m_cpv = spend / (m_imp or 1)

    # Build filterPath for frontend hierarchy navigation
    # filterPath is an array of {dimension, value} objects representing the full path to this row
    filter_path = []
    if filter_list:
        # Add parent filters from filter_list
        filter_path.extend(filter_list)
    # Add current row's dimension and value
    filter_path.append({"dimension": dimension_type, "value": dim_value})

    result = {
        "id": row_id,
        "name": dim_value,
        "level": level,
        "dimensionType": dimension_type,
        "impressions": impressions,
        "clicks": clicks,
        "conversions": conversions,
        "spend": spend,
        "revenue": revenue,
        "m_imp": m_imp,
        "m_clicks": m_clicks,
        "m_conv": m_conv,
        "ctr": ctr,
        "cvr": cvr,
        "roi": roi,
        "cpa": cpa,
        "rpa": rpa,
        "epc": epc,
        "epv": epv,
        "m_epc": m_epc,
        "m_epv": m_epv,
        "m_cpc": m_cpc,
        "m_cpv": m_cpv,
        "hasChild": True,  # Will be determined by query logic
        "filterPath": filter_path,  # Add filterPath for frontend
    }

    # Add landerUrl if present (for lander dimension)
    lander_url = row.get("landerUrl")
    if lander_url is not None:  # Changed from truthy check to None check
        result["landerUrl"] = lander_url

    return result


def format_daily_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Format daily breakdown row for frontend."""
    # Convert date to string
    date_value = row.get("date") or row.get("reportDate")
    if hasattr(date_value, 'isoformat'):
        date_str = date_value.isoformat()
    else:
        date_str = str(date_value) if date_value else ""

    return {
        "date": date_str,
        "impressions": row.get("impressions", 0) or 0,
        "clicks": row.get("clicks", 0) or 0,
        "conversions": row.get("conversions", 0) or 0,
        "spend": row.get("spend", 0) or 0,
        "revenue": row.get("total_revenue", 0) or row.get("revenue", 0) or 0,  # Support both column names
        "m_imp": row.get("m_imp", 0) or 0,
        "m_clicks": row.get("m_clicks", 0) or 0,
        "m_conv": row.get("m_conv", 0) or 0,
    }
