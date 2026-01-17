# Bicode - AdData Dashboard

营销数据管理与可视化平台，整合多个广告平台（Clickflare、MTG）的数据，提供多维度的营销数据分析仪表板。

## 项目结构

```
bicode/
├── backend/                    # 后端服务 (FastAPI)
│   ├── api/                   # API 应用
│   │   ├── main.py           # 入口
│   │   ├── config.yaml       # 配置
│   │   ├── database.py       # ClickHouse 连接
│   │   ├── routers/          # API 路由
│   │   └── models/           # 数据模型
│   ├── clickflare_etl/       # Clickflare ETL
│   │   ├── cf_etl.py         # 主 ETL (两次拉取)
│   │   ├── cf_api.py         # API 客户端
│   │   └── config.yaml       # 配置
│   └── mtg_etl/             # MTG ETL
│       ├── mtg_etl.py        # 主 ETL (DELETE + INSERT)
│       ├── mtg_api.py        # API 客户端
│       └── config.yaml       # 配置
├── EflowJRbi/                # 前端应用 (React + Vite)
└── deploy/                   # 部署脚本
```

## 技术栈

| 层级 | 技术 |
|------|------|
| 前端 | React 19.2 + TypeScript + Vite |
| 后端 | FastAPI + Uvicorn |
| 数据库 | ClickHouse |
| 缓存 | Redis |

## 快速开始

### 后端启动
```bash
cd backend
python -m uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload
```

### 前端启动
```bash
cd EflowJRbi
npm run dev
```

---

## ClickHouse 优化方案

### 服务器配置
- CPU: 2 核心
- 内存: 3.6 GB
- 系统: CentOS 7

### 问题诊断

| 问题 | 原因 |
|------|------|
| CPU 负载高 (8+) | 后台 merge 操作 |
| 内存紧张 | ClickHouse 无内存限制 |
| 数据碎片 | MTG ETL 大量 UPDATE |

### 已执行优化

#### 1. 手动优化表 (清理碎片)
```bash
clickhouse-client --user=default --password=admin123 --query="
OPTIMIZE TABLE ad_platform.dwd_marketing_report_daily FINAL"
```

#### 2. 内存限制配置
文件: `/etc/clickhouse-server/config.d/memory.xml`
```xml
<clickhouse>
    <max_server_memory_usage>2500000000</max_server_memory_usage>
    <max_memory_usage_for_all_queries>2000000000</max_memory_usage_for_all_queries>
</clickhouse>
```

#### 3. 后台 Merge 线程限制
文件: `/etc/clickhouse-server/config.d/merge.xml`
```xml
<clickhouse>
    <background_pool>
        <max_threads>1</max_threads>
        <max_memory_usage>1000000000</max_memory_usage>
    </background_pool>
</clickhouse>
```

### 表结构

```sql
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(reportDate)    -- 按月分区
ORDER BY (Media, reportDate, CampaignID, AdsetID, AdsID)
SETTINGS index_granularity = 8192
```

### ETL 流程优化

#### Clickflare ETL (两次拉取)
```
PASS 1: 拉取 advertiser 信息 (10 维)
PASS 2: 拉取 landing 信息 (10 维)
合并: 内存中合并 9 维 key
写入: DELETE + INSERT
```

#### MTG ETL (DELETE + INSERT)
```
原方案: ~10,000 次 UPDATE → 大量碎片
新方案: 1 次 DELETE + 1 次 INSERT → 碎片少
```

### 监控命令

```bash
# 检查系统负载
ssh houtai01 "uptime"

# 检查内存使用
ssh houtai01 "free -h"

# 检查 ClickHouse 进程
ssh houtai01 "ps aux | grep clickhouse"

# 检查后台 merge 状态
clickhouse-client --user=default --password=admin123 --query="
SELECT * FROM system.merges"
```

### 优化效果

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| 可用内存 | 574 MB | 1.7 GB |
| ClickHouse 内存 | 2.1 GB (58%) | 1.0 GB (28%) |
| 活跃 parts | 4 个 | 2 个 |
| SQL 操作 (MTG) | ~10,000 UPDATE | 1 DELETE + 1 INSERT |
