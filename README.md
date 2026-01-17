# Bicode - AdData Dashboard

营销数据管理与可视化平台，整合多个广告平台（Clickflare、MTG/Mintegral）的数据，提供多维度的营销数据分析仪表板。

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
│   ├── clickflare_etl/       # Clickflare ETL (已集成 MTG 数据)
│   │   ├── cf_etl.py         # 主 ETL
│   │   ├── cf_api.py         # API 客户端
│   │   ├── logger.py         # 日志模块
│   │   └── config.yaml       # 配置
│   └── run_etl.py            # ETL 运行器 (统一入口)
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

#### 当前 ETL 流程 (一体化设计)

```
Clickflare ETL (已集成 MTG 数据):
├── PASS 1: 拉取 advertiser 信息 (10 维)
├── PASS 2: 拉取 landing 信息 (10 维)
├── 内存合并 CF 两轮数据
├── 拉取 MTG API (3个账户)
├── 按 AdsetID 匹配合并 MTG spend
└── 一次性 DELETE + INSERT
```

#### MTG Spend 合并策略

| 问题 | 解决方案 |
|------|----------|
| MTG 追踪所有 impressions，CF 只追踪有转化的 | MTG 数据按 CF impressions 比例分配 spend |
| 一个 AdsetID 对应多行 CF 数据 | 按 CF impressions 权重分配，保证 spend 总数不变 |
| 无 CF 匹配的 MTG 数据 | 创建新的 MTG-only 行 |

#### 超时保护机制

| 检查点 | 说明 |
|--------|------|
| CF PASS 1 完成后 | 检查是否超过 30 分钟 |
| CF PASS 2 完成后 | 检查是否超过 30 分钟 |
| 每个 MTG 账户拉取后 | 检查是否超过 30 分钟 |
| 超时处理 | 插入已拉取的部分数据，等待下次任务继续 |

#### 旧任务清理

新任务启动时，自动杀死运行超过 10 分钟的旧 ETL 进程，防止并发运行。

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
| SQL 操作 | CF INSERT + MTG ~10,000 UPDATE | 1 次 DELETE + 1 次 INSERT |
| MTG spend 保留率 | ~0.4% (丢失 99.6%) | 100% 完整保留 |
| ETL 步骤 | 2 个独立进程 | 1 个一体化进程 |
