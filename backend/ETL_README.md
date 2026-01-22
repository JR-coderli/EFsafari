# ETL 说明文档

## 概述

本项目包含两个 ETL 流程：

| ETL 类型 | 文件 | 触发方式 | 数据源 |
|----------|------|----------|--------|
| **主 ETL** | `run_etl.py` | 每小时 crontab | Clickflare + MTG API |
| **Hourly ETL** | `clickflare_etl/cf_hourly_etl.py` | 每 10 分钟 (scheduler) | Clickflare API |

---

## 防止并发机制

### 主 ETL (`run_etl.py`)

- **超时阈值**: 20 分钟 (1200 秒)
- **触发频率**: 每小时 crontab
- **查找进程**: `pgrep -f 'run_etl.py|cf_etl.py'`
- **逻辑**: 运行超过 20 分钟的进程视为卡住，会被杀死

### Hourly ETL (`scheduler.py`)

- **触发频率**: 每 10 分钟
- **查找进程**: `pgrep -f 'cf_hourly_etl.py'`
- **逻辑**: 每次启动前直接杀死所有旧进程（不判断运行时间），确保不会并发运行

### 为什么逻辑不同？

- **主 ETL**: 涉及多个 API 端点拉取，数据量大，可能确实需要较长时间，给予 20 分钟缓冲
- **Hourly ETL**: 只拉取当天数据，应该很快完成。每次启动前直接清理旧进程更简单可靠

---

## 时区转换说明

### Hourly Report 数据存储

- **存储时区**: UTC
- **API 返回**: UTC+8 (Asia/Shanghai)
- **转换逻辑**: API 数据减 8 小时后存储为 UTC

```
UTC+8 时间 → UTC 存储时间
────────────────────────────────
00:00 - 07:59  →  前一天 16:00 - 23:59
08:00 - 23:59  →  当天 00:00 - 15:59
```

### 查询时转换

前端查询时指定 `timezone` 参数，API 会自动转换：

- `timezone=UTC`: 显示 UTC 时间
- `timezone=Asia/Shanghai`: 显示 UTC+8 时间

---

## 数据表

### hourly_report

```sql
CREATE TABLE IF NOT EXISTS ad_platform.hourly_report (
    reportDate Date,
    reportHour UInt8,
    timezone String,           -- 固定为 "UTC"
    Media String,
    MediaID String,
    offer String,
    offerID String,
    advertiser String,
    advertiserID String,
    Campaign String,
    CampaignID String,
    Adset String,
    AdsetID String,
    impressions UInt64,
    clicks UInt64,
    conversions UInt64,
    spend Float64,
    revenue Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(reportDate)
ORDER BY (reportDate, reportHour, Media, AdsetID)
TTL reportDate + INTERVAL 1 MONTH
```

---

## 修改记录

### 2026-01-22

- 为 `scheduler.py` 添加防止并发机制
  - Hourly ETL: 每次启动前直接杀死所有旧进程（不判断运行时间）
- 主 ETL 超时阈值从 10 分钟改为 20 分钟
