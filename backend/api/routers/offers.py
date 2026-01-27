"""
Clickflare Offers API Router
提供 Offer 详情数据（URL、Notes 等）
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/offers",
    tags=["offers"]
)


def get_db():
    """Get ClickHouse database connection."""
    from api.database import get_db
    return get_db()


@router.get("/details")
async def get_offers_details(
    offer_ids: Optional[str] = None
):
    """
    获取 Offers 详情数据

    Args:
        offer_ids: 逗号分隔的 offer_id 列表，不传则返回所有

    Returns:
        Offer 详情列表，包含 offer_id, name, url, notes 等字段
    """
    try:
        db = get_db()

        if offer_ids:
            # 查询指定的 offers
            id_list = [id.strip() for id in offer_ids.split(',') if id.strip()]
            if not id_list:
                return {"data": []}

            # 构建 IN 子句
            id_placeholders = ', '.join([f"'{id}'" for id in id_list])
            query = f"""
                SELECT
                    offer_id,
                    name,
                    url,
                    notes,
                    payout_type,
                    payout_amount,
                    payout_currency,
                    affiliate_network_id,
                    static_url,
                    tags
                FROM ad_platform.clickflare_offers_details
                WHERE offer_id IN ({id_placeholders})
            """
        else:
            # 查询所有 offers
            query = """
                SELECT
                    offer_id,
                    name,
                    url,
                    notes,
                    payout_type,
                    payout_amount,
                    payout_currency,
                    affiliate_network_id,
                    static_url,
                    tags
                FROM ad_platform.clickflare_offers_details
            """

        with db.get_client() as client:
            result = client.query(query)

        # 转换结果为字典列表
        offers = []
        for row in result.named_results():
            offers.append({
                "offer_id": row.get("offer_id", ""),
                "name": row.get("name", ""),
                "url": row.get("url", ""),
                "notes": row.get("notes", ""),
                "payout_type": row.get("payout_type", ""),
                "payout_amount": float(row.get("payout_amount", 0)),
                "payout_currency": row.get("payout_currency", "USD"),
                "affiliate_network_id": row.get("affiliate_network_id", ""),
                "static_url": row.get("static_url", ""),
                "tags": row.get("tags", [])
            })

        return {"data": offers}

    except Exception as e:
        logger.error(f"Error fetching offers details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/details-map")
async def get_offers_details_map(
    offer_ids: Optional[str] = None
):
    """
    获取 Offers 详情映射（以 offer_id 为 key）

    Args:
        offer_ids: 逗号分隔的 offer_id 列表，不传则返回所有

    Returns:
        映射字典 {offer_id: {url, notes, ...}}
    """
    result = await get_offers_details(offer_ids)
    offers = result.get("data", [])

    # 构建映射
    offers_map = {}
    for offer in offers:
        offer_id = offer.pop("offer_id")
        offers_map[offer_id] = offer

    return {"data": offers_map}
