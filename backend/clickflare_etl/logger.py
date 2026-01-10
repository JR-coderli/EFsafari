"""
Logging module for Clickflare ETL
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime


def setup_logger(config):
    """
    Setup logger with file and console handlers

    Args:
        config: Dict with logging configuration

    Returns:
        logging.Logger: Configured logger instance
    """
    log_level = getattr(logging, config.get("level", "INFO"))
    log_dir = config.get("log_dir", "logs")
    log_file = config.get("log_file", "cf_etl.log")
    max_bytes = config.get("max_bytes", 10485760)
    backup_count = config.get("backup_count", 5)

    # Create logs directory if not exists
    os.makedirs(log_dir, exist_ok=True)

    # Create logger
    logger = logging.getLogger("CF_ETL")
    logger.setLevel(log_level)

    # Clear existing handlers
    logger.handlers = []

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, log_file),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


class ETLLogger:
    """
    ETL Logger wrapper for convenience
    """

    def __init__(self, config):
        self.logger = setup_logger(config)
        self.errors = []

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)
        self.errors.append({
            "timestamp": datetime.now().isoformat(),
            "message": message
        })

    def debug(self, message):
        self.logger.debug(message)

    def critical(self, message):
        self.logger.critical(message)
        self.errors.append({
            "timestamp": datetime.now().isoformat(),
            "message": message
        })

    def has_errors(self):
        return len(self.errors) > 0

    def get_errors(self):
        return self.errors

    def log_etl_start(self, report_date):
        self.info(f"{'='*60}")
        self.info(f"ETL Job Started for report_date: {report_date}")
        self.info(f"{'='*60}")

    def log_etl_complete(self, report_date, rows_inserted):
        self.info(f"{'='*60}")
        self.info(f"ETL Job Completed for report_date: {report_date}")
        self.info(f"Rows inserted: {rows_inserted}")
        self.info(f"{'='*60}")

    def log_etl_failed(self, report_date, error):
        self.error(f"{'='*60}")
        self.error(f"ETL Job FAILED for report_date: {report_date}")
        self.error(f"Error: {error}")
        self.error(f"{'='*60}")
