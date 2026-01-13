"""
Daily Report API router.

独立的日报功能，支持 media + date 两层维度，支持 spend 手动修正。
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from pydantic import BaseModel
import logging

from api.database import get_db
from api.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/daily-report", tags=["daily-report"])


# ==================== Schema Models ====================

class DimensionItem(BaseModel):
    """维度项"""
    value: str
    label: str


class DimensionsResponse(BaseModel):
    """可用维度响应（Daily Report 只有 media 和 date）"""
    dimensions: List[DimensionItem]


class HierarchyNodeMetrics(BaseModel):
    """层级节点指标"""
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    spend: float = 0.0
    revenue: float = 0.0
    profit: float = 0.0
    m_imp: int = 0
    m_clicks: int = 0
    m_conv: int = 0
    ctr: float = 0.0
    cvr: float = 0.0
    roi: float = 0.0
    cpa: float = 0.0
    rpa: float = 0.0
    epc: float = 0.0
    epv: float = 0.0
    m_epc: float = 0.0
    m_epv: float = 0.0
    m_cpc: float = 0.0
    m_cpv: float = 0.0


class HierarchyNode(BaseModel):
    """层级节点"""
    _dimension: str
    _metrics: HierarchyNodeMetrics
    _children: Optional[Dict[str, "HierarchyNode"]] = None


class HierarchyResponse(BaseModel):
    """层级数据响应"""
    dimensions: List[str]  # ["media", "date"]
    hierarchy: Dict[str, Any]  # Changed from HierarchyNode to Any to avoid serialization issues
    startDate: str
    endDate: str


class FilterPathItem(BaseModel):
    """过滤路径项"""
    dimension: str
    value: str


class DailyReportRow(BaseModel):
    """Daily Report 数据行（带层级支持）"""
    id: str
    name: str
    level: int
    dimensionType: str
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    spend: float = 0.0
    revenue: float = 0.0
    profit: float = 0.0
    m_imp: int = 0
    m_clicks: int = 0
    m_conv: int = 0
    ctr: float = 0.0
    cvr: float = 0.0
    roi: float = 0.0
    cpa: float = 0.0
    rpa: float = 0.0
    epc: float = 0.0
    epv: float = 0.0
    m_epc: float = 0.0
    m_epv: float = 0.0
    m_cpc: float = 0.0
    m_cpv: float = 0.0
    hasChild: bool = True
    filterPath: List[FilterPathItem] = []
    spend_manual: float = 0.0  # 手动修正值（前端用于标识是否修改过）


class SpendUpdateRequest(BaseModel):
    """Spend 最终值设置请求"""
    date: str
    media: str
    spend_value: float  # 最终 spend 值 (spend_final)，系统自动计算 spend_manual = spend_final - spend_original


class DailyReportSummary(BaseModel):
    """Daily Report 汇总数据"""
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    revenue: float = 0.0
    spend: float = 0.0
    profit: float = 0.0
    m_imp: int = 0
    m_clicks: int = 0
    m_conv: int = 0
    ctr: float = 0.0
    cvr: float = 0.0
    roi: float = 0.0
    cpa: float = 0.0
    rpa: float = 0.0


class MediaItem(BaseModel):
    """媒体列表项"""
    name: str


class MediaListResponse(BaseModel):
    """媒体列表响应"""
    media: List[MediaItem]


class SyncDataRequest(BaseModel):
    """数据同步请求"""
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD


class LockDateRequest(BaseModel):
    """锁定日期请求"""
    date: str  # YYYY-MM-DD
    lock: bool = True  # True=锁定, False=解锁


# ==================== Helper Functions ====================

def _build_permission_filter(user_role: str, user_keywords: List[str]) -> Optional[str]:
    """构建权限过滤 SQL（复用 dashboard 逻辑）"""
    if user_role == 'admin' or not user_keywords:
        return None

    if user_role == 'ops':
        keyword_conditions = [f"lower(Media) LIKE lower('%{k}%')" for k in user_keywords]
        return f"({' OR '.join(keyword_conditions)})"

    return None


def _calculate_metrics(row: Dict[str, Any]) -> Dict[str, float]:
    """计算衍生指标"""
    impressions = int(row.get("impressions", 0) or 0)
    clicks = int(row.get("clicks", 0) or 0)
    conversions = int(row.get("conversions", 0) or 0)
    spend = float(row.get("spend_final", row.get("spend", 0)) or 0)
    revenue = float(row.get("revenue", 0) or 0)
    m_imp = int(row.get("m_imp", 0) or 0)
    m_clicks = int(row.get("m_clicks", 0) or 0)

    return {
        "ctr": clicks / (impressions or 1),
        "cvr": conversions / (clicks or 1),
        "roi": (revenue - spend) / (spend or 1),
        "cpa": spend / (conversions or 1),
        "rpa": revenue / (conversions or 1),
        "epc": revenue / (clicks or 1),
        "epv": revenue / (impressions or 1),
        "m_epc": revenue / (m_clicks or 1),
        "m_epv": revenue / (m_imp or 1),
        "m_cpc": spend / (m_clicks or 1),
        "m_cpv": spend / (m_imp or 1),
        "profit": revenue - spend,
    }


def _build_filter_path(media: str, date_value: str = None) -> List[FilterPathItem]:
    """构建过滤路径"""
    path = [FilterPathItem(dimension="media", value=media)]
    if date_value:
        path.append(FilterPathItem(dimension="date", value=date_value))
    return path


# ==================== API Endpoints ====================

@router.get("/dimensions", response_model=DimensionsResponse)
async def get_dimensions():
    """
    获取 Daily Report 可用的维度。

    固定返回 media 和 date 两个维度。
    """
    return DimensionsResponse(dimensions=[
        DimensionItem(value="media", label="Media"),
        DimensionItem(value="date", label="Date"),
    ])


@router.get("/hierarchy", response_model=HierarchyResponse)
async def get_hierarchy(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取 Daily Report 层级数据。

    返回 date -> media 的两层结构（日期在上，媒体在下）。
    """
    db = get_db()
    try:
        client = db.connect()

        # 构建权限过滤
        user_role = current_user.get("role", "")
        user_keywords = current_user.get("keywords", [])
        permission_filter = _build_permission_filter(user_role, user_keywords)

        conditions = [
            f"reportDate >= '{start_date}'",
            f"reportDate <= '{end_date}'"
        ]
        if permission_filter:
            conditions.append(permission_filter)
        where_clause = " AND ".join(conditions)

        # 查询所有数据
        query = f"""
            SELECT
                formatDateTime(reportDate, '%Y-%m-%d') as date_str,
                Media as media,
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(revenue) as revenue,
                sum(spend_final) as spend,
                sum(m_imp) as m_imp,
                sum(m_clicks) as m_clicks,
                sum(m_conv) as m_conv
            FROM ad_platform.dwd_daily_report
            WHERE {where_clause}
            GROUP BY reportDate, Media
            ORDER BY reportDate DESC, Media
        """

        result = client.query(query)

        # 构建层级结构: date -> media
        hierarchy: Dict[str, Any] = {}

        for row in result.named_results():
            date_str = row.get("date_str", "")
            media_name = row.get("media", "Unknown")

            # 创建 Date 层级节点
            if date_str not in hierarchy:
                hierarchy[date_str] = {
                    "_dimension": "date",
                    "_metrics": {
                        "impressions": 0,
                        "clicks": 0,
                        "conversions": 0,
                        "spend": 0.0,
                        "revenue": 0.0,
                        "profit": 0.0,
                        "m_imp": 0,
                        "m_clicks": 0,
                        "m_conv": 0,
                        "ctr": 0.0,
                        "cvr": 0.0,
                        "roi": 0.0,
                        "cpa": 0.0,
                        "rpa": 0.0,
                        "epc": 0.0,
                        "epv": 0.0,
                        "m_epc": 0.0,
                        "m_epv": 0.0,
                        "m_cpc": 0.0,
                        "m_cpv": 0.0,
                    },
                    "_children": {}
                }

            # 累加 Date 层级指标
            date_node = hierarchy[date_str]
            for key in ["impressions", "clicks", "conversions", "m_imp", "m_clicks", "m_conv"]:
                date_node["_metrics"][key] += int(row.get(key, 0) or 0)
            for key in ["spend", "revenue"]:
                date_node["_metrics"][key] += float(row.get(key, 0) or 0)

            # 创建 Media 子节点
            metrics = _calculate_metrics(row)
            media_node = {
                "_dimension": "media",
                "_metrics": {
                    "impressions": int(row.get("impressions", 0) or 0),
                    "clicks": int(row.get("clicks", 0) or 0),
                    "conversions": int(row.get("conversions", 0) or 0),
                    "spend": float(row.get("spend", 0) or 0),
                    "revenue": float(row.get("revenue", 0) or 0),
                    "profit": metrics["profit"],
                    "m_imp": int(row.get("m_imp", 0) or 0),
                    "m_clicks": int(row.get("m_clicks", 0) or 0),
                    "m_conv": int(row.get("m_conv", 0) or 0),
                    "ctr": metrics["ctr"],
                    "cvr": metrics["cvr"],
                    "roi": metrics["roi"],
                    "cpa": metrics["cpa"],
                    "rpa": metrics["rpa"],
                    "epc": metrics["epc"],
                    "epv": metrics["epv"],
                    "m_epc": metrics["m_epc"],
                    "m_epv": metrics["m_epv"],
                    "m_cpc": metrics["m_cpc"],
                    "m_cpv": metrics["m_cpv"],
                },
                "_children": {}  # 叶子节点
            }
            date_node["_children"][media_name] = media_node

        # 计算 Date 层级的衍生指标
        for date_str, date_node in hierarchy.items():
            m = date_node["_metrics"]
            m["profit"] = m["revenue"] - m["spend"]
            m["ctr"] = m["clicks"] / (m["impressions"] or 1)
            m["cvr"] = m["conversions"] / (m["clicks"] or 1)
            m["roi"] = m["profit"] / (m["spend"] or 1)
            m["cpa"] = m["spend"] / (m["conversions"] or 1)
            m["rpa"] = m["revenue"] / (m["conversions"] or 1)
            m["epc"] = m["revenue"] / (m["clicks"] or 1)
            m["epv"] = m["revenue"] / (m["impressions"] or 1)
            m["m_epc"] = m["revenue"] / (m["m_clicks"] or 1)
            m["m_epv"] = m["revenue"] / (m["m_imp"] or 1)
            m["m_cpc"] = m["spend"] / (m["m_clicks"] or 1)
            m["m_cpv"] = m["spend"] / (m["m_imp"] or 1)

        return HierarchyResponse(
            dimensions=["date", "media"],
            hierarchy=hierarchy,
            startDate=start_date,
            endDate=end_date,
        )

    except Exception as e:
        logger.error(f"Error fetching hierarchy: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching hierarchy: {str(e)}")


@router.get("/data", response_model=List[DailyReportRow])
async def get_daily_report(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    media: Optional[str] = Query(None, description="媒体筛选（用于子层级查询）"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取 Daily Report 数据。

    根据 media 参数返回不同层级：
    - 无 media 参数：返回 Media 层级数据
    - 有 media 参数：返回该 Media 下的 Date 层级数据
    """
    db = get_db()
    try:
        client = db.connect()

        # 构建 WHERE 子句
        conditions = [
            f"reportDate >= '{start_date}'",
            f"reportDate <= '{end_date}'"
        ]

        if media:
            escaped_media = media.replace("'", "''")
            conditions.append(f"Media = '{escaped_media}'")

        # 权限过滤
        user_role = current_user.get("role", "")
        user_keywords = current_user.get("keywords", [])
        permission_filter = _build_permission_filter(user_role, user_keywords)
        if permission_filter:
            conditions.append(permission_filter)

        where_clause = " AND ".join(conditions)

        if media:
            # 查询特定 Media 下的 Date 数据
            query = f"""
                SELECT
                    formatDateTime(reportDate, '%Y-%m-%d') as date_str,
                    Media as media,
                    impressions,
                    clicks,
                    conversions,
                    revenue,
                    spend_final,
                    spend_manual,
                    m_imp,
                    m_clicks,
                    m_conv
                FROM ad_platform.dwd_daily_report
                WHERE {where_clause}
                ORDER BY reportDate DESC
            """
        else:
            # 聚合查询 Media 层级数据
            query = f"""
                SELECT
                    Media as name,
                    sum(impressions) as impressions,
                    sum(clicks) as clicks,
                    sum(conversions) as conversions,
                    sum(revenue) as revenue,
                    sum(spend_final) as spend,
                    sum(m_imp) as m_imp,
                    sum(m_clicks) as m_clicks,
                    sum(m_conv) as m_conv
                FROM ad_platform.dwd_daily_report
                WHERE {where_clause}
                GROUP BY Media
                ORDER BY Media
            """

        result = client.query(query)

        data = []
        for row in result.named_results():
            metrics = _calculate_metrics(row)

            if media:
                # Date 层级行
                date_str = row.get("date_str", "")
                filter_path = _build_filter_path(media, date_str)
                data.append({
                    "id": f"{media}|{date_str}",
                    "name": date_str,
                    "level": 1,
                    "dimensionType": "date",
                    "impressions": int(row.get("impressions", 0) or 0),
                    "clicks": int(row.get("clicks", 0) or 0),
                    "conversions": int(row.get("conversions", 0) or 0),
                    "spend": float(row.get("spend_final", 0) or 0),
                    "revenue": float(row.get("revenue", 0) or 0),
                    "profit": metrics["profit"],
                    "m_imp": int(row.get("m_imp", 0) or 0),
                    "m_clicks": int(row.get("m_clicks", 0) or 0),
                    "m_conv": int(row.get("m_conv", 0) or 0),
                    "ctr": metrics["ctr"],
                    "cvr": metrics["cvr"],
                    "roi": metrics["roi"],
                    "cpa": metrics["cpa"],
                    "rpa": metrics["rpa"],
                    "epc": metrics["epc"],
                    "epv": metrics["epv"],
                    "m_epc": metrics["m_epc"],
                    "m_epv": metrics["m_epv"],
                    "m_cpc": metrics["m_cpc"],
                    "m_cpv": metrics["m_cpv"],
                    "hasChild": False,  # Date 是叶子节点
                    "filterPath": [p.dict() for p in filter_path],
                    "spend_manual": float(row.get("spend_manual", 0) or 0),
                })
            else:
                # Media 层级行
                media_name = row.get("name", "")
                filter_path = _build_filter_path(media_name)
                data.append({
                    "id": media_name,
                    "name": media_name,
                    "level": 0,
                    "dimensionType": "media",
                    "impressions": int(row.get("impressions", 0) or 0),
                    "clicks": int(row.get("clicks", 0) or 0),
                    "conversions": int(row.get("conversions", 0) or 0),
                    "spend": float(row.get("spend", 0) or 0),
                    "revenue": float(row.get("revenue", 0) or 0),
                    "profit": metrics["profit"],
                    "m_imp": int(row.get("m_imp", 0) or 0),
                    "m_clicks": int(row.get("m_clicks", 0) or 0),
                    "m_conv": int(row.get("m_conv", 0) or 0),
                    "ctr": metrics["ctr"],
                    "cvr": metrics["cvr"],
                    "roi": metrics["roi"],
                    "cpa": metrics["cpa"],
                    "rpa": metrics["rpa"],
                    "epc": metrics["epc"],
                    "epv": metrics["epv"],
                    "m_epc": metrics["m_epc"],
                    "m_epv": metrics["m_epv"],
                    "m_cpc": metrics["m_cpc"],
                    "m_cpv": metrics["m_cpv"],
                    "hasChild": True,  # Media 有 Date 子节点
                    "filterPath": [p.dict() for p in filter_path],
                    "spend_manual": 0.0,
                })

        return data

    except Exception as e:
        logger.error(f"Error fetching daily report: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching daily report: {str(e)}")


@router.post("/update-spend")
async def update_spend(
    request: SpendUpdateRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    设置指定日期和媒体的最终 spend 值（spend_final）。

    spend_value: 用户填写的最终花费值（spend_final）
                 系统自动计算: spend_manual = spend_final - spend_original
    """
    db = get_db()
    try:
        client = db.connect()

        # 检查权限
        user_role = current_user.get("role")
        if user_role not in ['admin', 'ops']:
            raise HTTPException(status_code=403, detail="Only admin and ops users can update spend")

        # 验证日期格式
        try:
            datetime.strptime(request.date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        escaped_media = request.media.replace("'", "''")
        username = current_user.get("username", "unknown")

        # 检查记录是否存在
        check_query = f"""
            SELECT spend_manual, spend_original
            FROM ad_platform.dwd_daily_report
            WHERE reportDate = '{request.date}' AND Media = '{escaped_media}'
        """

        check_result = client.query(check_query)
        existing_rows = list(check_result.named_results())

        if not existing_rows:
            # 记录不存在，创建新记录
            # 用户填的是最终值 spend_final，spend_original 默认为 0
            insert_query = f"""
                INSERT INTO ad_platform.dwd_daily_report
                (reportDate, Media, spend_manual, spend_final, spend_original,
                 impressions, clicks, conversions, revenue, m_imp, m_clicks, m_conv,
                 created_at, updated_at, last_modified_by)
                VALUES
                ('{request.date}', '{escaped_media}',
                 {request.spend_value}, {request.spend_value}, 0,
                 0, 0, 0, 0, 0, 0, 0,
                 now(), now(), '{username}')
            """
            client.command(insert_query)
        else:
            # 更新现有记录
            # 用户填的是 spend_final（最终值），需要计算 spend_manual
            current_spend_original = float(existing_rows[0].get("spend_original", 0) or 0)
            new_spend_final = request.spend_value
            new_spend_manual = new_spend_final - current_spend_original

            update_query = f"""
                ALTER TABLE ad_platform.dwd_daily_report
                UPDATE
                    spend_manual = {new_spend_manual},
                    spend_final = {new_spend_final},
                    updated_at = now(),
                    last_modified_by = '{username}'
                WHERE reportDate = '{request.date}' AND Media = '{escaped_media}'
            """
            client.command(update_query)

        return {
            "success": True,
            "message": f"Spend updated to {request.spend_value} for {request.media} on {request.date}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating spend: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating spend: {str(e)}")


@router.get("/summary", response_model=DailyReportSummary)
async def get_daily_report_summary(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    media: Optional[str] = Query(None, description="媒体筛选"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取 Daily Report 汇总数据。
    """
    db = get_db()
    try:
        client = db.connect()

        conditions = [
            f"reportDate >= '{start_date}'",
            f"reportDate <= '{end_date}'"
        ]

        if media:
            escaped_media = media.replace("'", "''")
            conditions.append(f"Media = '{escaped_media}'")

        # 权限过滤
        user_role = current_user.get("role", "")
        user_keywords = current_user.get("keywords", [])
        permission_filter = _build_permission_filter(user_role, user_keywords)
        if permission_filter:
            conditions.append(permission_filter)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(revenue) as revenue,
                sum(spend_final) as spend,
                sum(m_imp) as m_imp,
                sum(m_clicks) as m_clicks,
                sum(m_conv) as m_conv
            FROM ad_platform.dwd_daily_report
            WHERE {where_clause}
        """

        result = client.query(query)
        rows = list(result.named_results())

        if rows and rows[0]:
            row = rows[0]
            impressions = int(row.get("impressions", 0) or 0)
            clicks = int(row.get("clicks", 0) or 0)
            conversions = int(row.get("conversions", 0) or 0)
            revenue = float(row.get("revenue", 0) or 0)
            spend = float(row.get("spend", 0) or 0)
            m_imp = int(row.get("m_imp", 0) or 0)
            m_clicks = int(row.get("m_clicks", 0) or 0)
            m_conv = int(row.get("m_conv", 0) or 0)

            # 计算衍生指标
            ctr = clicks / (impressions or 1)
            cvr = conversions / (clicks or 1)
            roi = (revenue - spend) / (spend or 1) if spend > 0 else 0
            cpa = spend / (conversions or 1)
            rpa = revenue / (conversions or 1)
            profit = revenue - spend
        else:
            impressions = clicks = conversions = 0
            revenue = spend = profit = 0.0
            m_imp = m_clicks = m_conv = 0
            ctr = cvr = roi = cpa = rpa = 0.0

        return DailyReportSummary(
            impressions=impressions,
            clicks=clicks,
            conversions=conversions,
            revenue=revenue,
            spend=spend,
            profit=profit,
            m_imp=m_imp,
            m_clicks=m_clicks,
            m_conv=m_conv,
            ctr=ctr,
            cvr=cvr,
            roi=roi,
            cpa=cpa,
            rpa=rpa,
        )

    except Exception as e:
        logger.error(f"Error fetching daily report summary: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")


@router.get("/media-list", response_model=MediaListResponse)
async def get_media_list(current_user: dict = Depends(get_current_user)):
    """
    获取可用的媒体列表（用于筛选器）。
    """
    db = get_db()
    try:
        client = db.connect()

        # 权限过滤
        user_role = current_user.get("role", "")
        user_keywords = current_user.get("keywords", [])
        permission_filter = _build_permission_filter(user_role, user_keywords)

        base_query = """
            SELECT DISTINCT Media as name
            FROM ad_platform.dwd_daily_report
            WHERE Media != '' AND length(Media) > 0
        """

        if permission_filter:
            query = f"{base_query} AND {permission_filter}"
        else:
            query = base_query

        query += " ORDER BY Media"

        result = client.query(query)
        media_list = [{"name": row.get("name", "")} for row in result.named_results()]

        return MediaListResponse(media=media_list)

    except Exception as e:
        logger.error(f"Error fetching media list: {e}")
        return MediaListResponse(media=[])


@router.post("/sync")
async def sync_data_from_performance(
    request: SyncDataRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    从 Performance 表同步数据到 Daily Report 表。

    只同步未锁定的日期的数据。
    如果某天已有数据且未锁定，则更新；如果不存在，则插入。
    """
    db = get_db()
    try:
        client = db.connect()

        # 检查权限
        user_role = current_user.get("role")
        if user_role not in ['admin', 'ops']:
            raise HTTPException(status_code=403, detail="Only admin and ops users can sync data")

        # 验证日期格式
        try:
            datetime.strptime(request.start_date, '%Y-%m-%d')
            datetime.strptime(request.end_date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        # 先删除未锁定的现有数据（避免重复）
        delete_query = f"""
            ALTER TABLE ad_platform.dwd_daily_report
            DELETE WHERE reportDate >= '{request.start_date}'
            AND reportDate <= '{request.end_date}'
            AND is_locked = 0
        """
        client.command(delete_query)

        # 从源表聚合数据并插入
        insert_query = f"""
            INSERT INTO ad_platform.dwd_daily_report
            (reportDate, Media, impressions, clicks, conversions, revenue, spend_original,
             spend_manual, spend_final, m_imp, m_clicks, m_conv, is_locked,
             created_at, updated_at, last_modified_by)
            SELECT
                reportDate,
                Media,
                sum(impressions) as impressions,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(revenue) as revenue,
                sum(spend) as spend_original,
                toDecimal64(0, 4) as spend_manual,
                sum(spend) as spend_final,
                sum(m_imp) as m_imp,
                sum(m_clicks) as m_clicks,
                sum(m_conv) as m_conv,
                0 as is_locked,
                now() as created_at,
                now() as updated_at,
                '{current_user.get("username", "system")}' as last_modified_by
            FROM ad_platform.dwd_marketing_report_daily
            WHERE reportDate >= '{request.start_date}'
            AND reportDate <= '{request.end_date}'
            GROUP BY reportDate, Media
            SETTINGS max_memory_usage=2000000000, max_threads=4
        """
        client.command(insert_query)

        # 获取同步的行数
        count_query = f"""
            SELECT count() as cnt
            FROM ad_platform.dwd_daily_report
            WHERE reportDate >= '{request.start_date}'
            AND reportDate <= '{request.end_date}'
        """
        count_result = client.query(count_query)
        row_count = count_result.first_row[0]

        return {
            "success": True,
            "message": f"Synced {row_count} rows from {request.start_date} to {request.end_date}",
            "row_count": row_count
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error syncing data: {e}")
        raise HTTPException(status_code=500, detail=f"Error syncing data: {str(e)}")


@router.post("/lock-date")
async def lock_date(
    request: LockDateRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    锁定/解锁指定日期的数据。

    锁定后，该日期的数据不再自动同步更新。
    只有 admin 和 ops 用户可以锁定。
    """
    db = get_db()
    try:
        client = db.connect()

        # 检查权限
        user_role = current_user.get("role")
        if user_role not in ['admin', 'ops']:
            raise HTTPException(status_code=403, detail="Only admin and ops users can lock dates")

        # 验证日期格式
        try:
            datetime.strptime(request.date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        lock_value = 1 if request.lock else 0
        action = "locked" if request.lock else "unlocked"

        update_query = f"""
            ALTER TABLE ad_platform.dwd_daily_report
            UPDATE is_locked = {lock_value},
                   updated_at = now(),
                   last_modified_by = '{current_user.get("username", "unknown")}'
            WHERE reportDate = '{request.date}'
        """
        client.command(update_query)

        return {
            "success": True,
            "message": f"Date {request.date} has been {action}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error locking date: {e}")
        raise HTTPException(status_code=500, detail=f"Error locking date: {str(e)}")


@router.get("/locked-dates")
async def get_locked_dates(current_user: dict = Depends(get_current_user)):
    """
    获取已锁定的日期列表。
    """
    db = get_db()
    try:
        client = db.connect()

        query = """
            SELECT DISTINCT formatDateTime(reportDate, '%Y-%m-%d') as date
            FROM ad_platform.dwd_daily_report
            WHERE is_locked = 1
            ORDER BY reportDate DESC
        """

        result = client.query(query)
        locked_dates = [row.get("date", "") for row in result.named_results()]

        return {
            "locked_dates": locked_dates
        }

    except Exception as e:
        logger.error(f"Error fetching locked dates: {e}")
        return {"locked_dates": []}


@router.get("/health", response_model=dict)
async def health_check():
    """健康检查端点。"""
    return {"status": "ok", "service": "daily-report"}
