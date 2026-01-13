"""
APScheduler configuration for Daily Report data synchronization.

Runs daily at 12:00 to sync data from dwd_marketing_report_daily to dwd_daily_report.
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import date, timedelta
import logging

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = AsyncIOScheduler()

# System user context for scheduled tasks
SYSTEM_USER = {
    "username": "system",
    "role": "admin"
}


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
    """Start the APScheduler with daily sync job."""
    # Schedule daily sync at 12:00
    scheduler.add_job(
        daily_sync_task,
        'cron',
        hour=12,
        minute=0,
        id='daily_report_sync',
        replace_existing=True
    )

    scheduler.start()
    logger.info("Scheduler started - daily sync scheduled for 12:00")


def stop_scheduler():
    """Stop the APScheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
