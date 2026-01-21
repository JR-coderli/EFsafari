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

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = AsyncIOScheduler()

# System user context for scheduled tasks
SYSTEM_USER = {
    "username": "system",
    "role": "admin"
}


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
    Scheduled task: Fetch today's hourly data for both UTC0 and UTC8.

    Note: The ETL script now generates both UTC and Asia/Shanghai data in a single run
    by converting from the UTC+8 API data. So we only need to run it once.
    """
    try:
        logger.info("Starting scheduled hourly ETL task")

        # Run ETL once - it generates both UTC and Asia/Shanghai data
        success = await run_hourly_etl("UTC")

        if success:
            logger.info("Hourly ETL task completed successfully (both timezones)")
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


def start_scheduler():
    """Start the APScheduler with daily sync and hourly ETL jobs."""
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
    logger.info("Scheduler started - daily sync at 12:00, hourly ETL every 10 minutes (both timezones)")


def stop_scheduler():
    """Stop the APScheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
