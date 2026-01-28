"""
定时任务状态和日志 API Router
提供定时任务执行状态和日志查询功能
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, List, Optional
import logging
import os
import re
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/scheduler",
    tags=["scheduler"]
)


# 日志文件配置
LOG_FILES = {
    "offers": {
        "name": "Offers ETL",
        "schedule": "05:15 daily",
        "path": "clickflare_etl/logs/cf_offers_etl.log",
        "icon": "fa-box",
        "color": "bg-indigo-500"
    },
    "lander": {
        "name": "Lander URLs Sync",
        "schedule": "06:15 daily",
        "path": "logs/sync_lander_urls.log",
        "icon": "fa-link",
        "color": "bg-emerald-500"
    },
    "hourly": {
        "name": "Hourly ETL",
        "schedule": "Every 10 minutes",
        "path": "clickflare_etl/logs/cf_hourly_etl.log",
        "icon": "fa-clock",
        "color": "bg-cyan-500"
    }
}


def get_backend_dir() -> str:
    """获取 backend 目录的绝对路径"""
    # 尝试从 __file__ 获取，如果失败则使用固定路径
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # api/routers -> api -> backend
        backend_dir = os.path.dirname(os.path.dirname(current_dir))
        # 验证目录是否有效
        if os.path.exists(os.path.join(backend_dir, "api")):
            return backend_dir
    except:
        pass
    # 回退到硬编码路径
    return r"E:\code\bicode\backend"


def parse_log_status(log_content: str) -> Dict:
    """从日志内容中解析出状态信息"""
    status = {
        "last_run": None,
        "last_status": "unknown",  # success, failed, unknown
        "duration": None,
        "record_count": None,
        "error_message": None
    }

    if not log_content:
        return status

    lines = log_content.split('\n')

    # 查找最后一次运行记录
    last_success_time = None
    last_failed_time = None
    duration = None

    # 提取行首时间戳的辅助函数
    def extract_line_timestamp(line: str) -> Optional[str]:
        """从行首提取时间戳，支持多种格式"""
        # 格式: "2026-01-28 18:09:21" 或 "2026-01-27 23:21:18"
        match = re.match(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
        if match:
            return match.group(1)
        return None

    for i, line in enumerate(lines):
        line_clean = line.strip()
        # 检测完成标记（成功或失败） - 支持 Lander 和 Offers 格式
        if ("Completed Successfully" in line or
            "Sync completed successfully" in line.lower() or
            "ETL 执行成功" in line):
            # 先尝试从当前行提取时间戳（用于 Offers 格式）
            if not last_success_time:
                line_time = extract_line_timestamp(line_clean)
                if line_time:
                    last_success_time = line_time

            # 在后续行中查找 End time 和 Duration
            for j in range(i, min(i + 10, len(lines))):
                # End time 格式: "End time: 2026-01-28 18:09:21"
                end_match = re.search(r'End time:\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', lines[j])
                if end_match:
                    last_success_time = end_match.group(1)
                # Duration 格式: "Duration: 2.59 seconds" 或 "ETL 执行时长: 4.62 秒"
                dur_match = re.search(r'Duration:\s+([\d.]+)', lines[j])
                if not dur_match:
                    dur_match = re.search(r'ETL 执行时长:\s+([\d.]+)', lines[j])
                if dur_match:
                    duration = float(dur_match.group(1))

        # 检测失败
        if "ETL failed" in line or "Sync Failed" in line or "failed with Exception" in line or "ETL 执行失败" in line:
            # 先尝试从当前行提取时间戳（用于 Offers 格式）
            if not last_failed_time:
                line_time = extract_line_timestamp(line)
                if line_time:
                    last_failed_time = line_time

            # 在后续行中查找 End time
            for j in range(i, min(i + 10, len(lines))):
                end_match = re.search(r'End time:\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', lines[j])
                if end_match:
                    last_failed_time = end_match.group(1)
                    break

    # 提取记录数 - 支持多种格式
    for line in reversed(lines[-50:]):  # 只检查最后50行
        if "Total records in table:" in line or "Total records:" in line:
            count_match = re.search(r'(\d+)', line)
            if count_match:
                status["record_count"] = int(count_match.group(1))
                break
        elif "Fetched" in line and "landers" in line:
            count_match = re.search(r'Fetched\s+(\d+)', line)
            if count_match:
                status["record_count"] = int(count_match.group(1))
                break
        elif "ETL 执行成功" in line:
            count_match = re.search(r'共处理\s+(\d+)\s+条', line)
            if count_match:
                status["record_count"] = int(count_match.group(1))
                break
        elif "成功拉取" in line and "Offers" in line:
            count_match = re.search(r'成功拉取\s+(\d+)\s+条', line)
            if count_match:
                status["record_count"] = int(count_match.group(1))
                break

    # 判断最终状态
    if last_success_time and (not last_failed_time or last_success_time >= last_failed_time):
        status["last_run"] = last_success_time
        status["last_status"] = "success"
        status["duration"] = duration
    elif last_failed_time:
        status["last_run"] = last_failed_time
        status["last_status"] = "failed"
        # 提取错误信息
        for line in reversed(lines[-20:]):
            if "Error:" in line or "ERROR" in line:
                status["error_message"] = line.strip()[-100:]
                break

    return status


def read_log_file(task_key: str, lines: int = 100) -> Optional[str]:
    """读取日志文件的最后 N 行"""
    task_info = LOG_FILES.get(task_key)
    if not task_info:
        print(f"[SCHEDULER] Unknown task_key: {task_key}")
        return None

    backend_dir = get_backend_dir()
    log_path = os.path.join(backend_dir, task_info["path"])

    print(f"[SCHEDULER] Reading log for {task_key}: backend_dir={backend_dir}, log_path={log_path}, exists={os.path.exists(log_path)}")

    if not os.path.exists(log_path):
        print(f"[SCHEDULER] Log file not found: {log_path}")
        return None

    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            # 读取最后 N 行
            all_lines = f.readlines()
            content = ''.join(all_lines[-lines:])
            print(f"[SCHEDULER] Log file read successfully, content length: {len(content)}")
            return content
    except Exception as e:
        print(f"[SCHEDULER] Error reading log file {log_path}: {e}")
        return None


@router.get("/status")
async def get_scheduler_status():
    """
    获取所有定时任务的状态

    Returns:
        各定时任务的最新运行状态
    """
    result = {}

    for key, task_info in LOG_FILES.items():
        log_content = read_log_file(key, lines=200)
        logger.info(f"[SCHEDULER] Task {key}: log_content length = {len(log_content) if log_content else 0}")
        status = parse_log_status(log_content or "")
        logger.info(f"[SCHEDULER] Task {key}: status = {status}")

        result[key] = {
            "name": task_info["name"],
            "schedule": task_info["schedule"],
            "icon": task_info["icon"],
            "color": task_info["color"],
            **status
        }

    return {"data": result}


@router.get("/debug")
async def debug_info():
    """调试信息"""
    backend_dir = get_backend_dir()

    # 检查每个日志文件
    files_info = {}
    for key, task_info in LOG_FILES.items():
        log_path = os.path.join(backend_dir, task_info["path"])
        files_info[key] = {
            "path": log_path,
            "exists": os.path.exists(log_path),
            "size": os.path.getsize(log_path) if os.path.exists(log_path) else None
        }

    return {
        "backend_dir": backend_dir,
        "files": files_info
    }


@router.get("/log/{task_id}")
async def get_task_log(task_id: str, lines: int = 100):
    """
    获取指定任务的日志内容

    Args:
        task_id: 任务 ID (offers, lander, hourly)
        lines: 返回的日志行数，默认 100

    Returns:
        日志内容
    """
    if task_id not in LOG_FILES:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    log_content = read_log_file(task_id, lines)

    if log_content is None:
        return {
            "task": task_id,
            "log": None,
            "message": "Log file not found or empty"
        }

    return {
        "task": task_id,
        "name": LOG_FILES[task_id]["name"],
        "log": log_content
    }


@router.post("/trigger/{task_id}")
async def trigger_task(task_id: str):
    """
    手动触发定时任务

    Args:
        task_id: 任务 ID (offers, lander, hourly)

    Returns:
        触发结果
    """
    import subprocess
    import sys

    backend_dir = get_backend_dir()

    task_scripts = {
        "offers": ("clickflare_etl/cf_offers_etl.py", "clickflare_etl"),
        "lander": ("sync_lander_urls.py", ""),
        "hourly": ("clickflare_etl/cf_hourly_etl.py", "clickflare_etl")
    }

    if task_id not in task_scripts:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    script_path, work_dir = task_scripts[task_id]
    full_script_path = os.path.join(backend_dir, script_path)
    work_dir_path = os.path.join(backend_dir, work_dir) if work_dir else backend_dir

    try:
        # 设置环境变量
        import copy
        env = copy.copy(os.environ)
        env['PYTHONPATH'] = backend_dir + os.pathsep + env.get('PYTHONPATH', '')

        # 在后台运行
        subprocess.Popen(
            [sys.executable, full_script_path],
            cwd=work_dir_path,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        return {
            "success": True,
            "message": f"Task {LOG_FILES[task_id]['name']} triggered successfully",
            "task_id": task_id
        }

    except Exception as e:
        logger.error(f"Error triggering task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
