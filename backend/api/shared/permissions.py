"""
权限过滤器共享模块

提供统一的权限过滤 SQL 构建函数，消除 dashboard.py、daily_report.py、hourly.py 中的重复代码
"""

from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


def build_permission_filter(user_role: str, user_keywords: List[str]) -> Optional[str]:
    """
    构建权限过滤 SQL

    Args:
        user_role: 用户角色 (admin, ops, ops02, business)
        user_keywords: 用户的关键词列表

    Returns:
        SQL WHERE 子句片段，如果不需要过滤则返回 None
    """
    # Admin 或空关键词 = 无限制
    if user_role == 'admin' or not user_keywords:
        logger.debug(f"[Permission] No filtering: role={user_role}, keywords={user_keywords}")
        return None

    # 根据角色构建关键词过滤（使用实际的 ClickHouse 列名）
    if user_role == 'ops':
        # 按 Adset 列过滤
        keyword_conditions = [f"lower(Adset) LIKE lower('%{k}%')" for k in user_keywords]
        filter_sql = f"({' OR '.join(keyword_conditions)})"
        logger.debug(f"[Permission] ops role filter: {filter_sql}")
        return filter_sql
    elif user_role == 'ops02':
        # 按 Media 列（platform 维度）过滤
        keyword_conditions = [f"lower(Media) LIKE lower('%{k}%')" for k in user_keywords]
        filter_sql = f"({' OR '.join(keyword_conditions)})"
        logger.debug(f"[Permission] ops02 role filter: {filter_sql}")
        return filter_sql
    elif user_role == 'business':
        # 按 offer 列过滤
        keyword_conditions = [f"lower(offer) LIKE lower('%{k}%')" for k in user_keywords]
        filter_sql = f"({' OR '.join(keyword_conditions)})"
        logger.debug(f"[Permission] business role filter: {filter_sql}")
        return filter_sql

    return None
