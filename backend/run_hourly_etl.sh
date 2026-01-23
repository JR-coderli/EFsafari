#!/bin/bash
#
# Hourly ETL Runner
# Clickflare Hourly Report ETL - 只运行 UTC+0 时区的数据拉取
#
# Usage:
#   ./run_hourly_etl.sh
#
# Crontab 配置 (每小时运行一次):
#   30 * * * * cd /opt/bicode/backend && ./run_hourly_etl.sh >> logs/hourly_etl.log 2>&1
#

set -e

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETL_DIR="${SCRIPT_DIR}/clickflare_etl"
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/hourly_etl.log"

# 创建日志目录
mkdir -p "${LOG_DIR}"

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

log "=========================================="
log "Hourly ETL Started (UTC+0 only)"
log "=========================================="

# 切换到 ETL 目录
cd "${ETL_DIR}"

# 运行 UTC+0 时区的 ETL
log ""
log "[Step 1/1] Running UTC+0 ETL..."
if python3 cf_hourly_etl.py --config config.yaml 2>&1 | tee -a "${LOG_FILE}"; then
    log "[Step 1/1] UTC+0 ETL completed successfully"
else
    log "[ERROR] UTC+0 ETL failed!"
    exit 1
fi

log ""
log "=========================================="
log "Hourly ETL Completed Successfully"
log "=========================================="

exit 0
