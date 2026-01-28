"""
Config API router

系统配置工具，包括数据拉取和特殊媒体配置
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
import subprocess
import logging
import os
import json
from datetime import datetime, timedelta, date
from pathlib import Path

from api.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/config", tags=["config"])

# 配置文件路径 - 放在项目根目录的 config 文件夹
# 开发环境: E:\code\bicode\backend\config\special_media.json
# 生产环境: /app/config/special_media.json (容器) 或项目根目录/config/special_media.json
PROJECT_ROOT = Path(__file__).parent.parent.parent  # 从 api/routers 向上三级到 backend 根目录
CONFIG_DIR = PROJECT_ROOT / "config"
CONFIG_FILE = CONFIG_DIR / "special_media.json"


class SpecialMediaRequest(BaseModel):
    """特殊媒体配置请求"""
    type: str  # 'dates' or 'hourly'
    keywords: List[str]


class SpecialMediaResponse(BaseModel):
    """特殊媒体配置响应"""
    dates_special_media: List[str]
    hourly_special_media: List[str]


# ==================== 辅助函数 ====================

def get_default_config() -> dict:
    """获取默认配置

    dates_special_media: 这些媒体在 Dates Report 中 spend = revenue (来自 CF ETL exclude_spend_media)
    hourly_special_media: 这些媒体在 Hourly Report 中 spend = revenue
    """
    return {
        "dates_special_media": ["Mintegral", "Hastraffic", "JMmobi", "Brain"],
        "hourly_special_media": ["mintegral", "hastraffic", "jmmobi", "brainx"]
    }


def load_special_media_config() -> dict:
    """从文件加载特殊媒体配置，如果文件不存在则返回默认值"""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load config from file: {e}")
    return get_default_config()


def save_special_media_config(config: dict):
    """保存特殊媒体配置到文件"""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved special media config to file: {CONFIG_FILE}")


# ==================== API 端点 ====================

@router.get("/special-media", response_model=SpecialMediaResponse)
async def get_special_media(current_user: dict = Depends(get_current_user)):
    """获取特殊媒体配置"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    config = load_special_media_config()
    return SpecialMediaResponse(**config)


@router.post("/special-media")
async def update_special_media(request: SpecialMediaRequest, current_user: dict = Depends(get_current_user)):
    """更新特殊媒体配置"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    if request.type not in ["dates", "hourly"]:
        raise HTTPException(status_code=400, detail="Invalid type. Must be 'dates' or 'hourly'")

    config = load_special_media_config()

    if request.type == "dates":
        config["dates_special_media"] = request.keywords
    else:
        config["hourly_special_media"] = request.keywords

    save_special_media_config(config)

    logger.info(f"Special media config updated: {request.type} -> {request.keywords}")

    return {
        "message": f"Special media configuration updated for {request.type}",
        **config
    }


@router.post("/pull-data/{data_type}")
async def pull_data(data_type: str, current_user: dict = Depends(get_current_user)):
    """触发数据拉取

    Args:
        data_type: 'yesterday' 或 'hourly'
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    if data_type not in ["yesterday", "hourly"]:
        raise HTTPException(status_code=400, detail="Invalid data_type. Must be 'yesterday' or 'hourly'")

    try:
        # 获取脚本路径
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        if data_type == "hourly":
            # Hourly ETL
            script = os.path.join(backend_dir, "clickflare_etl", "cf_hourly_etl.py")
            cmd = ["python3", script]
            logger.info(f"Starting hourly ETL: {' '.join(cmd)}")
        else:
            # Yesterday data - 使用 daily report sync
            # 这里调用 sync_data_from_performance
            yesterday = date.today() - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
            end_date = start_date

            from api.routers.daily_report import SyncDataRequest, sync_data_from_performance

            request = SyncDataRequest(start_date=start_date, end_date=end_date)
            result = await sync_data_from_performance(request, current_user)

            logger.info(f"Yesterday data sync completed: {result}")
            return {"message": "Yesterday data sync initiated", "result": result}

        # 对于 hourly ETL，使用 subprocess 异步运行
        import asyncio
        import copy

        env = copy.copy(os.environ)
        env['PYTHONPATH'] = backend_dir + os.pathsep + env.get('PYTHONPATH', '')

        # 在后台运行
        async def run_etl():
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=os.path.join(backend_dir, "clickflare_etl"),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode == 0:
                logger.info(f"Hourly ETL completed successfully")
            else:
                logger.error(f"Hourly ETL failed: {stderr.decode()}")

        # 在后台启动任务
        asyncio.create_task(run_etl())

        return {
            "message": "Hourly data pull initiated. This may take a few minutes.",
            "data_type": data_type
        }

    except Exception as e:
        logger.error(f"Failed to pull data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_config_status(current_user: dict = Depends(get_current_user)):
    """获取配置状态"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    config = load_special_media_config()

    return {
        "special_media": config,
        "last_updated": datetime.now().isoformat()
    }
