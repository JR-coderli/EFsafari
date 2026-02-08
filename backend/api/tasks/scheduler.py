"""
APScheduler configuration for Daily Report data synchronization.

Runs daily at 12:00 to sync data from dwd_marketing_report_daily to dwd_daily_report.
Also runs hourly ETL every 10 minutes to fetch today's data for UTC0 and UTC8.
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import date, timedelta, datetime
import logging
import subprocess
import sys
import os
import signal
import time
import traceback

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = AsyncIOScheduler()

# System user context for scheduled tasks
SYSTEM_USER = {
    "username": "system",
    "role": "admin"
}


def kill_old_hourly_etl_processes():
    """
    查找并杀死所有正在运行的 Hourly ETL 进程（cf_hourly_etl.py）

    不判断运行时间，直接全部杀死。因为每 10 分钟触发一次，
    每次启动前先清理旧进程，确保不会并发运行。

    Returns:
        int: 被杀死的进程数量
    """
    current_pid = os.getpid()
    killed_count = 0

    try:
        # 使用 pgrep 查找 Hourly ETL 相关进程
        result = subprocess.run(
            ['pgrep', '-f', 'cf_hourly_etl.py'],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            old_pids = result.stdout.strip().split('\n')
            for pid_str in old_pids:
                if not pid_str:
                    continue

                old_pid = int(pid_str)
                if old_pid == current_pid:
                    continue

                # 直接杀死，不判断运行时间
                try:
                    logger.warning(f"[KILL] Killing old Hourly ETL process PID={old_pid}")
                    os.kill(old_pid, signal.SIGKILL)
                    killed_count += 1
                except ProcessLookupError:
                    # 进程可能已经退出
                    pass

        if killed_count > 0:
            logger.info(f"[KILL] Killed {killed_count} old Hourly ETL process(es)")
            time.sleep(1)  # 等待进程完全退出
        else:
            logger.debug("[INFO] No old Hourly ETL processes found")

    except FileNotFoundError:
        logger.warning("[WARN] pgrep command not found, skipping old process check")
    except Exception as e:
        logger.warning(f"[WARN] Error checking old processes: {e}")

    return killed_count


async def run_hourly_etl(timezone: str = "UTC") -> bool:
    """
    Run the hourly ETL script for a specific timezone.

    Args:
        timezone: "UTC" or "Asia/Shanghai"

    Returns:
        True if successful, False otherwise
    """
    try:
        # Get paths
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        etl_dir = os.path.join(backend_dir, "clickflare_etl")
        etl_script = os.path.join(etl_dir, "cf_hourly_etl.py")
        config_path = os.path.join(etl_dir, "config.yaml")

        # Determine command line args
        args = [sys.executable, etl_script, "--config", config_path]
        if timezone == "Asia/Shanghai":
            args.append("--utc8")

        logger.info(f"Running hourly ETL for {timezone}: {' '.join(args)}")

        # Set up environment with correct PYTHONPATH
        import copy
        env = copy.copy(os.environ)
        # Add backend_dir to PYTHONPATH so 'api' module can be found
        current_pythonpath = env.get('PYTHONPATH', '')
        env['PYTHONPATH'] = backend_dir + os.pathsep + current_pythonpath

        # Run the ETL script from etl_dir (where config.yaml is)
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minutes timeout
            cwd=etl_dir,
            env=env
        )

        if result.returncode == 0:
            logger.info(f"Hourly ETL for {timezone} completed successfully")
            if result.stdout:
                logger.info(f"ETL output: {result.stdout[-500:]}")  # Last 500 chars
            return True
        else:
            logger.error(f"Hourly ETL for {timezone} failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"ETL errors: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"Hourly ETL for {timezone} timed out after 10 minutes")
        return False
    except Exception as e:
        logger.error(f"Hourly ETL for {timezone} failed: {e}", exc_info=True)
        return False


async def hourly_etl_task():
    """
    Scheduled task: Fetch past 24 hours of hourly data for both UTC0 and UTC8.

    Note: The ETL script now generates both UTC and Asia/Shanghai data in a single run
    by converting from the UTC+8 API data. So we only need to run it once.

    Concurrency protection: Kills any old hourly ETL processes before starting a new one.
    """
    try:
        logger.info("Starting scheduled hourly ETL task")

        # Step 1: 杀死旧的 Hourly ETL 进程（防止并发）
        killed = kill_old_hourly_etl_processes()
        if killed > 0:
            logger.info(f"Killed {killed} old Hourly ETL process(es) before starting new one")

        # Step 2: 运行 ETL - 生成 UTC 和 Asia/Shanghai 两个时区的数据
        success = await run_hourly_etl("UTC")

        if success:
            # 清除 hourly 相关缓存，确保下次请求获取最新数据
            from api.cache import delete_cache
            try:
                deleted = delete_cache("hourly:*")
                logger.info(f"Hourly ETL task completed successfully (both timezones), cleared {deleted} hourly cache entries")
            except Exception as cache_err:
                logger.warning(f"Hourly ETL completed but cache clearing failed: {cache_err}")
        else:
            logger.warning("Hourly ETL task completed with warnings")

    except Exception as e:
        logger.error(f"Hourly ETL task failed: {e}", exc_info=True)


async def daily_sync_task():
    """
    Scheduled task: Sync yesterday's data to Daily Report table.

    Runs daily at 12:00 to sync the previous day's data from
    dwd_marketing_report_daily to dwd_daily_report.
    """
    try:
        # Import here to avoid circular imports
        from api.routers.daily_report import SyncDataRequest, sync_data_from_performance

        # Calculate yesterday's date
        yesterday = date.today() - timedelta(days=1)
        start_date = yesterday.strftime('%Y-%m-%d')
        end_date = start_date  # Single day

        logger.info(f"Starting daily sync for {start_date}")

        # Create sync request
        request = SyncDataRequest(start_date=start_date, end_date=end_date)

        # Perform sync
        result = await sync_data_from_performance(request, SYSTEM_USER)

        logger.info(f"Daily sync completed: {result.get('message', 'Unknown')}")
    except Exception as e:
        logger.error(f"Daily sync task failed: {e}", exc_info=True)


async def sync_offers_task():
    """
    Scheduled task: Sync Offers from ClickFlare API.

    Runs daily at 05:15 to fetch all offers and sync to
    ad_platform.clickflare_offers_details table.
    """
    start_time = datetime.now()
    logger.info("="*60)
    logger.info(f"[SCHEDULED TASK] Offers Sync Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)

    try:
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        etl_dir = os.path.join(backend_dir, "clickflare_etl")
        etl_script = os.path.join(etl_dir, "cf_offers_etl.py")

        # Set up environment with correct PYTHONPATH
        import copy
        env = copy.copy(os.environ)
        current_pythonpath = env.get('PYTHONPATH', '')
        env['PYTHONPATH'] = backend_dir + os.pathsep + current_pythonpath

        result = subprocess.run(
            [sys.executable, etl_script],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minutes timeout
            cwd=etl_dir,
            env=env
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if result.returncode == 0:
            logger.info("="*60)
            logger.info(f"[SCHEDULED TASK] Offers Sync Completed Successfully!")
            logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Duration: {duration:.2f} seconds")
            if result.stdout:
                # 从输出中提取统计信息
                for line in result.stdout.split('\n'):
                    if 'Fetched' in line or 'inserted' in line or 'ETL 执行' in line:
                        logger.info(f"  {line.strip()}")
            logger.info("="*60)
        else:
            logger.error("="*60)
            logger.error(f"[SCHEDULED TASK] Offers Sync Failed!")
            logger.error(f"Return code: {result.returncode}")
            logger.error(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if result.stderr:
                logger.error(f"Errors: {result.stderr}")
            logger.error("="*60)

    except subprocess.TimeoutExpired:
        logger.error(f"[SCHEDULED TASK] Offers Sync timed out after 5 minutes")
    except Exception as e:
        end_time = datetime.now()
        logger.error("="*60)
        logger.error(f"[SCHEDULED TASK] Offers Sync Failed with Exception!")
        logger.error(f"Error: {e}")
        logger.error(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error("="*60)
        logger.error(traceback.format_exc())


async def sync_lander_urls_task():
    """
    Scheduled task: Sync Lander URLs from ClickFlare API.

    Runs daily at 06:15 to fetch all lander data and sync to
    ad_platform.dim_lander_url_mapping table.
    """
    start_time = datetime.now()
    logger.info("="*60)
    logger.info(f"[SCHEDULED TASK] Lander URLs Sync Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)

    try:
        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        sync_script = os.path.join(backend_dir, "sync_lander_urls.py")

        # Set up environment with correct PYTHONPATH
        import copy
        env = copy.copy(os.environ)
        current_pythonpath = env.get('PYTHONPATH', '')
        env['PYTHONPATH'] = backend_dir + os.pathsep + current_pythonpath

        result = subprocess.run(
            [sys.executable, sync_script],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minutes timeout
            cwd=backend_dir,
            env=env
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if result.returncode == 0:
            logger.info("="*60)
            logger.info(f"[SCHEDULED TASK] Lander URLs Sync Completed Successfully!")
            logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Duration: {duration:.2f} seconds")
            if result.stdout:
                # 从输出中提取统计信息
                for line in result.stdout.split('\n'):
                    if 'Fetched' in line or 'Inserting' in line or 'Total records' in line or 'Sync complete' in line:
                        logger.info(f"  {line.strip()}")
            logger.info("="*60)
        else:
            logger.error("="*60)
            logger.error(f"[SCHEDULED TASK] Lander URLs Sync Failed!")
            logger.error(f"Return code: {result.returncode}")
            logger.error(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if result.stderr:
                logger.error(f"Errors: {result.stderr}")
            logger.error("="*60)

    except subprocess.TimeoutExpired:
        logger.error(f"[SCHEDULED TASK] Lander URLs Sync timed out after 5 minutes")
    except Exception as e:
        end_time = datetime.now()
        logger.error("="*60)
        logger.error(f"[SCHEDULED TASK] Lander URLs Sync Failed with Exception!")
        logger.error(f"Error: {e}")
        logger.error(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error("="*60)
        logger.error(traceback.format_exc())


# Scheduler lock file to prevent multiple workers from starting it
_SCHEDULER_LOCK_FILE = "/tmp/bicode_scheduler.lock"
_scheduler_lock_fd = None


def _acquire_scheduler_lock() -> bool:
    """Acquire an exclusive lock to ensure only one worker runs the scheduler."""
    global _scheduler_lock_fd
    import os
    import fcntl

    try:
        _scheduler_lock_fd = open(_SCHEDULER_LOCK_FILE, 'w')
        # Try to acquire exclusive lock (non-blocking)
        fcntl.lockf(_scheduler_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Write PID to lock file
        _scheduler_lock_fd.write(str(os.getpid()))
        _scheduler_lock_fd.flush()
        logger.info(f"[SCHEDULER] Acquired exclusive lock (PID={os.getpid()})")
        return True
    except (IOError, OSError):
        # Lock is held by another process
        _scheduler_lock_fd.close()
        _scheduler_lock_fd = None
        logger.debug("[SCHEDULER] Another worker already holds the scheduler lock")
        return False


def _release_scheduler_lock():
    """Release the scheduler lock."""
    global _scheduler_lock_fd
    if _scheduler_lock_fd:
        try:
            import fcntl
            fcntl.lockf(_scheduler_lock_fd, fcntl.LOCK_UN)
            _scheduler_lock_fd.close()
        except Exception:
            pass
        _scheduler_lock_fd = None


def start_scheduler():
    """Start the APScheduler with daily sync and hourly ETL jobs.

    Uses file locking to ensure only one worker starts the scheduler
    in a multi-worker environment (gunicorn).
    """
    # Try to acquire lock - only one worker will succeed
    if not _acquire_scheduler_lock():
        logger.info("[SCHEDULER] Scheduler already running in another worker, skipping")
        return

    try:
        # Schedule Offers sync at 05:15 daily
        scheduler.add_job(
            sync_offers_task,
            'cron',
            hour=5,
            minute=15,
            id='offers_sync',
            replace_existing=True
        )

        # Schedule Lander URLs sync at 06:15 daily
        scheduler.add_job(
            sync_lander_urls_task,
            'cron',
            hour=6,
            minute=15,
            id='lander_urls_sync',
            replace_existing=True
        )

        # Schedule daily sync at 12:00
        scheduler.add_job(
            daily_sync_task,
            'cron',
            hour=12,
            minute=0,
            id='daily_report_sync',
            replace_existing=True
        )

        # Schedule hourly ETL every 10 minutes
        scheduler.add_job(
            hourly_etl_task,
            'interval',
            minutes=10,
            id='hourly_etl_sync',
            replace_existing=True
        )

        scheduler.start()
        logger.info("Scheduler started - offers sync at 05:15, lander URLs sync at 06:15, daily sync at 12:00, hourly ETL every 10 minutes (both timezones)")
    except Exception as e:
        _release_scheduler_lock()
        raise


def stop_scheduler():
    """Stop the APScheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
    _release_scheduler_lock()
