"""
Hourly Report API router.

按小时粒度查询 Clickflare 数据，支持 UTC+0 和 UTC+8 时区切换
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from datetime import datetime, timedelta, timezone as dt_timezone
import logging

from api.database import get_db
from api.auth import get_current_user
from api.cache import cache_key, get_cache, set_cache
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/hourly", tags=["hourly"])


# ==================== Schema Models ====================

class HourlyDataRequest(BaseModel):
    """请求数据模型"""
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD
    group_by: str    # 维度，逗号分隔
    filters: Optional[str] = None  # JSON encoded filters
    limit: int = 1000
    timezone: str = "UTC"  # UTC 或 Asia/Shanghai


class HourlyDataRow(BaseModel):
    """小时数据行"""
    id: str
    name: str
    level: int
    dimensionType: str
    hour: Optional[int] = None
    timezone: Optional[str] = None
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    spend: float = 0.0
    revenue: float = 0.0
    profit: float = 0.0
    ctr: float = 0.0
    cvr: float = 0.0
    roi: float = 0.0
    cpa: float = 0.0
    rpa: float = 0.0
    epc: float = 0.0
    epv: float = 0.0
    epa: float = 0.0
    hasChild: bool = False


class HourlyDataResponse(BaseModel):
    """响应数据模型"""
    data: List[HourlyDataRow]
    total: int
    dateRange: dict


class TimezoneResponse(BaseModel):
    """时区响应"""
    timezones: List[dict]
    current: str


# ==================== 维度映射 ====================

DIMENSION_COLUMN_MAP = {
    'platform': 'Media',
    'adset': 'Adset',
    'hour': 'reportHour',
    'offer': 'offer',
    'advertiser': 'advertiser',
    'campaign': 'Campaign',
}


# ==================== 辅助函数 ====================

def _build_group_by_clause(dimensions: List[str]) -> str:
    """构建 GROUP BY 子句"""
    columns = [DIMENSION_COLUMN_MAP.get(d, d) for d in dimensions]
    return ", ".join(columns)


def _build_permission_filter(user_role: str, user_keywords: List[str]) -> Optional[str]:
    """构建权限过滤 SQL"""
    if user_role == 'admin' or not user_keywords:
        return None

    if user_role == 'ops':
        keyword_conditions = [f"lower(Adset) LIKE lower('%{k}%')" for k in user_keywords]
        return f"({' OR '.join(keyword_conditions)})"
    elif user_role == 'ops02':
        keyword_conditions = [f"lower(Media) LIKE lower('%{k}%')" for k in user_keywords]
        return f"({' OR '.join(keyword_conditions)})"
    # business 角色按 offer 筛选
    elif user_role == 'business':
        keyword_conditions = [f"lower(offer) LIKE lower('%{k}%')" for k in user_keywords]
        return f"({' OR '.join(keyword_conditions)})"

    return None


def _build_where_clause(start_date: str, end_date: str, filters: List[dict],
                        permission_filter: Optional[str], timezone: str) -> str:
    """构建 WHERE 子句"""
    conditions = [f"reportDate >= '{start_date}' AND reportDate <= '{end_date}'"]
    conditions.append(f"timezone = '{timezone}'")

    if permission_filter:
        conditions.append(permission_filter)

    for f in filters:
        dim = f.get("dimension")
        value = f.get("value")
        column = DIMENSION_COLUMN_MAP.get(dim, dim)

        # hour 维度特殊处理：从 "03:00" 格式中提取数字
        if dim == "hour":
            # 尝试从 "HH:MM" 格式中提取小时
            if ":" in str(value):
                try:
                    hour_val = int(value.split(":")[0])
                    conditions.append(f"{column} = {hour_val}")
                except (ValueError, IndexError):
                    # 如果解析失败，使用原始值（兼容数字字符串）
                    conditions.append(f"{column} = {value}")
            else:
                # 已经是数字格式
                conditions.append(f"{column} = {value}")
        else:
            # 其他维度使用字符串匹配
            escaped_value = value.replace("'", "''")
            conditions.append(f"{column} = '{escaped_value}'")

    return " AND ".join(conditions)


def _format_row_for_frontend(row: dict, dimension_type: str, level: int,
                               filter_values: List[str], all_dimensions: List[str],
                               filter_list: List[dict], timezone: str) -> dict:
    """格式化数据行给前端"""
    # 获取分组值（SQL 返回的是 group_{dimension_type}）
    group_key = f"group_{dimension_type}"
    raw_value = row.get(group_key, "")

    # 根据维度类型格式化名称
    if dimension_type == "hour":
        hour_val = int(raw_value) if raw_value != "" else 0
        name = f"{hour_val:02d}:00"
        hour_value = hour_val
    else:
        name = str(raw_value) if raw_value else ""
        hour_value = None

    # 构建 ID
    id_parts = filter_values + [name]
    row_id = "|".join(id_parts)

    # 计算指标
    impressions = row.get("impressions", 0)
    clicks = row.get("clicks", 0)
    conversions = row.get("conversions", 0)
    spend = row.get("spend", 0.0)
    revenue = row.get("revenue", 0.0)

    ctr = clicks / (impressions or 1)
    cvr = conversions / (clicks or 1)
    roi = (revenue - spend) / (spend or 1)
    cpa = spend / (conversions or 1)
    rpa = revenue / (conversions or 1)
    epc = revenue / (clicks or 1)
    epv = revenue / (impressions or 1)
    epa = revenue / (conversions or 1)  # EPA = revenue / conversions

    # 是否有子级
    has_child = len(all_dimensions) > 1

    result = {
        "id": row_id,
        "name": name,
        "level": level,
        "dimensionType": dimension_type,
        "hour": hour_value,
        "timezone": timezone,
        "impressions": impressions,
        "clicks": clicks,
        "conversions": conversions,
        "spend": round(spend, 2),
        "revenue": round(revenue, 2),
        "profit": round(revenue - spend, 2),
        "ctr": round(ctr, 4),
        "cvr": round(cvr, 4),
        "roi": round(roi, 4),
        "cpa": round(cpa, 2),
        "rpa": round(rpa, 2),
        "epc": round(epc, 2),
        "epv": round(epv, 4),
        "epa": round(epa, 2),
        "hasChild": has_child
    }

    # 添加过滤路径
    filter_path = []
    for i, val in enumerate(filter_values):
        dim = all_dimensions[i] if i < len(all_dimensions) else ""
        filter_path.append({"dimension": dim, "value": val})
    result["filterPath"] = filter_path

    return result


# ==================== API 端点 ====================

@router.get("/timezones", response_model=TimezoneResponse)
async def get_timezones():
    """获取可用时区列表"""
    return TimezoneResponse(
        timezones=[
            {"value": "UTC", "label": "UTC+0"},
            {"value": "Asia/Shanghai", "label": "UTC+8"}
        ],
        current="UTC"
    )


@router.get("/data", response_model=HourlyDataResponse)
async def get_hourly_data(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    group_by: str = Query(..., description="Comma-separated dimensions"),
    timezone: str = Query("UTC", description="Timezone: UTC or Asia/Shanghai"),
    filters: Optional[str] = Query(None, description="JSON encoded filters"),
    limit: int = Query(1000, description="Max results"),
    current_user: dict = Depends(get_current_user)
):
    """获取小时级数据，支持角色权限过滤

    Query parameters:
    - start_date: 开始日期 (YYYY-MM-DD)
    - end_date: 结束日期 (YYYY-MM-DD)
    - group_by: 逗号分隔的维度 (如 "hour,platform,offer")
    - timezone: 时区 (UTC 或 Asia/Shanghai)
    - filters: JSON 编码的过滤器列表
    - limit: 最大结果数
    """
    import json

    # 解析维度
    dimensions = [d.strip() for d in group_by.split(",") if d.strip()]
    if not dimensions:
        raise HTTPException(status_code=400, detail="At least one dimension required")

    # 解析过滤器
    filter_list = []
    if filters:
        try:
            filter_list = json.loads(filters)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid filters JSON")

    user_id = current_user.get("id", "anonymous")
    cache_key_val = cache_key("hourly", start_date, end_date, dimensions, filter_list, timezone, user_id)

    # 时区偏移配置（相对于 UTC）
    TIMEZONE_OFFSETS = {
        "UTC": 0,
        "Asia/Shanghai": 8,
        "EST": -5,
        "PST": -8
    }

    # 数据获取函数
    def fetch_data():
        db = get_db()
        client = db.connect()

        user_role = current_user.get("role")
        user_keywords = current_user.get("keywords", [])

        logger.info(f"[HOURLY API] User: id={current_user.get('id')}, role={user_role}, timezone={timezone}")

        # 获取时区偏移
        tz_offset = TIMEZONE_OFFSETS.get(timezone, 0)

        logger.info(f"[HOURLY API] Timezone: {timezone}, offset: {tz_offset}")

        # 根据用户选择的日期（start_date）计算 UTC 查询范围
        # 目标时区的 D 日 00:00 = UTC (D日 00:00 + tz_offset)
        # EST (-5): D日00:00 = UTC D日05:00
        # PST (-8): D日00:00 = UTC D日08:00
        # UTC+8:   D日00:00 = UTC D-1日16:00

        # 用户选择的日期（目标时区的日期）
        target_date = datetime.strptime(start_date, "%Y-%m-%d")

        # 计算目标时区该日 00:00 对应的 UTC 时间
        utc_start_dt = target_date + timedelta(hours=tz_offset)

        # 计算目标时区该日 23:59:59 对应的 UTC 时间
        utc_day_end = target_date + timedelta(days=1) - timedelta(seconds=1) + timedelta(hours=tz_offset)

        # 判断选择的日期是否是目标时区的"今天"
        utc_now = datetime.now(dt_timezone.utc).replace(tzinfo=None)
        # 目标时区当前时间 = UTC 当前时间 + tz_offset
        target_now = utc_now + timedelta(hours=tz_offset)
        target_today = target_now.date()

        # 用户选择的日期
        selected_date = target_date.date()

        is_today = (selected_date == target_today)

        if is_today:
            # 今天：只查询到当前 UTC 时间
            utc_end_dt = utc_now
        else:
            # 历史日期：查询全天
            utc_end_dt = utc_day_end

        utc_start_ts = utc_start_dt.strftime("%Y-%m-%d %H:%M:%S")
        utc_end_ts = utc_end_dt.strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"[HOURLY API] {timezone} date {start_date}: UTC {utc_start_ts} to {utc_end_ts} (is_today={is_today}, target_today={target_today})")

        permission_filter = _build_permission_filter(user_role, user_keywords)

        # 构建基础 WHERE 条件
        # 使用完整时间戳比较：>= start, < end（不包含 end）
        timestamp_filter = f"(toDateTime(reportDate) + toIntervalHour(reportHour)) >= toDateTime('{utc_start_ts}') AND (toDateTime(reportDate) + toIntervalHour(reportHour)) < toDateTime('{utc_end_ts}')"
        base_conditions = [
            timestamp_filter,
            f"timezone = 'UTC'"
        ]
        logger.info(f"[HOURLY API] Timestamp filter: {timestamp_filter}")

        if permission_filter:
            base_conditions.append(permission_filter)

        # 处理过滤器（需要根据时区调整 hour 过滤器）
        for f in filter_list:
            dim = f.get("dimension")
            value = f.get("value")
            column = DIMENSION_COLUMN_MAP.get(dim, dim)

            if dim == "hour":
                # hour 维度需要反向转换：用户看到的时区小时 -> UTC 小时
                # UTC小时 = 目标时区小时 - 偏移量
                try:
                    if ":" in str(value):
                        hour_val = int(value.split(":")[0])
                    else:
                        hour_val = int(value)
                    utc_hour = (hour_val - tz_offset + 24) % 24
                    base_conditions.append(f"{column} = {utc_hour}")
                except (ValueError, IndexError):
                    base_conditions.append(f"{column} = {value}")
            else:
                # 其他维度使用字符串匹配
                escaped_value = value.replace("'", "''")
                base_conditions.append(f"{column} = '{escaped_value}'")

        where_clause = " AND ".join(base_conditions)

        primary_dim = dimensions[0]
        primary_column = DIMENSION_COLUMN_MAP.get(primary_dim, primary_dim)

        # 根据主维度选择分组表达式
        if primary_dim == "hour":
            # 对于 hour 维度，使用时区转换后的小时
            # 公式: UTC小时 + 偏移量 = 目标时区小时
            # PST (UTC-8): UTC 0 + (-8) + 24 = 16, UTC 7 + (-8) = -1 + 24 = 23
            # UTC+8: UTC 0 + 8 = 8, UTC 15 + 8 = 23
            group_expr = f"((reportHour + {tz_offset} + 24) % 24)"
            logger.info(f"[HOURLY API] Hour dimension: group_expr={group_expr}, tz_offset={tz_offset}")
        else:
            # 其他维度使用原始列
            group_expr = primary_column

        query = f"""
            SELECT
                {group_expr} as group_{primary_dim},
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(spend) as spend,
                sum(revenue) as revenue
            FROM {db.database}.hourly_report
            WHERE {where_clause}
            GROUP BY {group_expr}
            ORDER BY revenue DESC
            LIMIT {limit}
            SETTINGS max_memory_usage=2000000000, max_threads=4
        """

        logger.info(f"Executing query: {query[:300]}...")
        result = client.query(query)

        formatted_data = []
        filter_values = [f.get("value") for f in filter_list]

        # 将生成器转换为列表以获取行数
        rows = list(result.named_results())
        logger.info(f"[HOURLY API] Query returned {len(rows)} raw rows")

        # Debug: 打印前几行的原始数据
        if rows:
            sample_rows = rows[:min(5, len(rows))]
            for i, sr in enumerate(sample_rows):
                logger.info(f"[HOURLY API] Sample row {i}: group_{primary_dim}={sr.get(f'group_{primary_dim}')}, impressions={sr.get('impressions')}, revenue={sr.get('revenue')}")

        for row in rows:
            formatted_row = _format_row_for_frontend(
                row,
                dimension_type=primary_dim,
                level=len(filter_list),
                filter_values=filter_values,
                all_dimensions=dimensions,
                filter_list=filter_list,
                timezone=timezone
            )
            formatted_data.append(formatted_row)

        logger.info(f"[HOURLY API] Returning {len(formatted_data)} formatted rows for dimension={primary_dim}")

        return HourlyDataResponse(
            data=formatted_data,
            total=len(formatted_data),
            dateRange={"start_date": start_date, "end_date": end_date}
        ).dict()

    # 检查缓存
    cached_result = get_cache(cache_key_val, refresher=fetch_data)
    if cached_result is not None:
        return HourlyDataResponse(**cached_result)

    # 缓存未命中，获取数据
    try:
        response_data = fetch_data()
        set_cache(cache_key_val, response_data)
        return HourlyDataResponse(**response_data)
    except Exception as e:
        import traceback
        logger.error(f"Error fetching hourly data: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@router.get("/status")
async def get_etl_status():
    """获取 Hourly ETL 状态"""
    from api.cache import get_redis
    r = get_redis()
    if not r:
        return {"utc": None, "utc8": None, "est": None, "pst": None}

    import json

    # Try to get hourly ETL status (with timezone-specific keys)
    utc_status = r.get("hourly_etl:last_update:UTC")
    utc8_status = r.get("hourly_etl:last_update:Asia/Shanghai")
    est_status = r.get("hourly_etl:last_update:EST")
    pst_status = r.get("hourly_etl:last_update:PST")

    # If hourly ETL status keys exist, use them
    if utc_status or utc8_status:
        return {
            "utc": json.loads(utc_status) if utc_status else None,
            "utc8": json.loads(utc8_status) if utc8_status else None,
            "est": json.loads(est_status) if est_status else None,
            "pst": json.loads(pst_status) if pst_status else None
        }

    # Fallback: use main ETL status and convert to expected format
    main_etl_status = r.get("etl:last_update")
    if main_etl_status:
        etl_data = json.loads(main_etl_status)
        # Convert to format expected by frontend
        return {
            "utc": {"last_update": etl_data.get("last_update"), "success": etl_data.get("success")},
            "utc8": {"last_update": etl_data.get("last_update"), "success": etl_data.get("success")},
            "est": None,
            "pst": None
        }

    return {"utc": None, "utc8": None, "est": None, "pst": None}


@router.post("/refresh")
async def refresh_hourly_data(current_user: dict = Depends(get_current_user)):
    """手动触发 Hourly ETL 刷新

    会触发 UTC0 和 UTC8 两个时区的数据拉取。
    返回任务状态，实际执行是异步的。

    注意：ETL 脚本现在一次运行同时生成两个时区的数据。
    """
    import asyncio

    # 检查权限（只有 admin 可以手动触发）
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admin can trigger ETL refresh")

    # 在后台执行 ETL（只运行一次，会同时生成两个时区的数据）
    async def run_etl_background():
        from api.tasks.scheduler import run_hourly_etl
        try:
            await run_hourly_etl("UTC")
        except Exception as e:
            logger.error(f"Background ETL failed: {e}")

    # 启动后台任务
    asyncio.create_task(run_etl_background())

    return {
        "status": "triggered",
        "message": "Hourly ETL refresh started (both timezones)"
    }


@router.get("/health")
async def health_check():
    """健康检查"""
    db = get_db()
    try:
        client = db.connect()
        result = client.query(f"SELECT count() FROM {db.database}.hourly_report")
        row_count = result.first_row[0] if result.first_row else 0
        return {
            "status": "healthy",
            "clickhouse": "connected",
            "database": db.database,
            "table": "hourly_report",
            "row_count": row_count
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"ClickHouse connection failed: {str(e)}")


@router.post("/metrics-order")
async def save_metrics_order(
    metrics: List[str],
    current_user: dict = Depends(get_current_user)
):
    """保存指标列顺序"""
    from api.users.view_service import get_view_service
    user_id = current_user.get("id", "anonymous")
    view_service = get_view_service()
    success = view_service.save_hourly_metrics_order(user_id, metrics)
    return {"success": success, "metrics": metrics}


@router.get("/metrics-order")
async def get_metrics_order(current_user: dict = Depends(get_current_user)):
    """获取保存的指标列顺序"""
    from api.users.view_service import get_view_service
    user_id = current_user.get("id", "anonymous")
    view_service = get_view_service()
    metrics = view_service.get_hourly_metrics_order(user_id)
    return {"metrics": metrics}
