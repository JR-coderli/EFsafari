# MTG Report API ETL 文档

## 概述

从 Mintegral (MTG) Report API 拉取多个账户的广告投放数据，经过 ETL 处理后存入 ClickHouse 宽表。

## API 信息

| 项目 | 值 |
|------|-----|
| Base URL | `https://ss-api.mintegral.com` |
| Endpoint | `/api/v2/reports/data` |
| 文档 | [广告投放报表_进阶版 \| Mintegral Docs](https://adv.mintegral.com/doc/cn/guide/report/advancedPerformanceReport.html) |
| Token文档 | [Token \| Mintegral Docs](https://adv.mintegral.com/doc/cn/guide/introduction/token.html) |
| 认证方式 | AccessKey + Token (MD5) |

### 认证参数

| 参数 | 说明 |
|------|------|
| Token生成 | `MD5(API_KEY + MD5(timestamp))` |
| Header字段 | `access-key` (小写), `Token`, `Timestamp` |

### 多账户配置

支持配置多个 MTG 账户，自动循环拉取所有账户数据并合并。

## 目标表结构

**表名**: `ad_platform.dwd_marketing_report_daily`

**字段映射**:

| API返回字段 | 目标表字段 | 数据类型 | 说明 |
|------------|-----------|---------|------|
| Date | reportDate | Date | 格式转换: 20240601 → datetime.date 对象 |
| - | Media | String | 留空，由 Clickflare UPDATE (trafficsourceName) |
| - | MediaID | String | 留空，由 Clickflare UPDATE (trafficsourceID) |
| - | Campaign | String | 暂留空，API无此字段 |
| Campaign Id | CampaignID | String | 广告ID (用于DELETE识别账户) |
| Offer Name | Adset | String | 广告单元名称 |
| Offer Id | AdsetID | String | 广告单元ID |
| Creative Name | Ads | String | 素材名称 |
| Creative Id | AdsID | String | 素材ID |
| Spend | spend | Float64 | 花费 |
| Impression | m_imp | UInt64 | 展示数 |
| Click | m_clicks | UInt64 | 点击数 |
| Conversion | m_conv | UInt64 | 转化数 |

**表引擎**: `SummingMergeTree`
**分区**: `PARTITION BY toYYYYMM(reportDate)`
**排序**: `ORDER BY (Media, reportDate, CampaignID, AdsetID, AdsID)`

## ETL 流程

```
1. 计算报告日期 (默认昨天)
   ↓
2. 连接 ClickHouse
   ↓
3. 循环处理每个账户:
   ├─ type=1 发起数据生成请求
   ├─ 轮询状态直至 code=200
   ├─ type=2 下载TSV数据
   └─ 解析并转换数据
   ↓
4. 收集所有账户的数据
   ↓
5. 删除历史数据 (按 CampaignID 列表)
   ↓ DELETE WHERE reportDate=? AND CampaignID IN (...)
   ↓
6. 批量插入 ClickHouse
```

## 请求参数

| 参数 | 值 | 说明 |
|------|-----|------|
| timezone | `0` | UTC 0 时区（与数据库其他媒体统一） |
| start_time | `YYYY-MM-DD` | 开始日期 |
| end_time | `YYYY-MM-DD` | 结束日期 |
| dimension_option | `Offer,Campaign,Creative` | 按广告单元、广告、素材维度 |
| time_granularity | `daily` | 按天聚合 |
| type | `1` (生成) / `2` (下载) | 请求类型 |

## 重试与容错策略

| 场景 | 处理方式 |
|------|---------|
| API请求失败 | 重试3次，指数退避 (2s, 4s, 8s) |
| 单账户失败 | 记录错误，继续处理其他账户 |
| 状态轮询超时 | 5分钟后失败，记录错误日志 |
| 数据插入失败 | 记录详细错误日志+堆栈信息 |
| 任务失败 | 返回 exit code 1 |

## ClickHouse 连接

| 参数 | 值 |
|------|-----|
| Host | `43.160.248.9` |
| Port | `8123` |
| Database | `ad_platform` |
| Username | `default` |
| Password | `admin123` |
| connect_timeout | `10s` |
| send_receive_timeout | `30s` |

## 项目结构

```
mtg_etl/
├── config.yaml           # 配置文件（包含多账户配置）
├── logger.py             # 日志模块
├── mtg_api.py            # API客户端
├── mtg_etl.py            # ETL主流程（支持多账户）
├── test_api.py           # API测试脚本
├── requirements.txt      # 依赖
├── run.sh                # 启动脚本
├── crontab.example       # 定时任务示例
└── mtgpull.md            # 本文档
```

## 运行命令

```bash
# 安装依赖
pip install -r requirements.txt

# 手动运行（拉取昨天数据，所有账户）
python mtg_etl.py

# 指定日期运行
python mtg_etl.py -d 2026-01-09

# 测试API（不连接数据库）
python test_api.py

# 使用启动脚本
./run.sh
```

## 定时任务

```bash
# 编辑 crontab
crontab -e

# 每小时运行（每小时的第30分钟）
30 * * * * cd /path/to/mtg_etl && ./run.sh >> logs/cron.log 2>&1

# 每天凌晨2点运行（推荐）
0 2 * * * cd /path/to/mtg_etl && ./run.sh >> logs/cron.log 2>&1
```

## 重要说明

1. **多账户支持**: 配置文件支持多个账户，自动循环拉取
2. **Media/MediaID**: 由 Clickflare ETL 模块负责填充，MTG ETL 留空
3. **关联方式**: 通过 CampaignID + AdsetID + AdsID 三个 ID 联合匹配
4. **时区**: 使用 UTC 0 时区，与数据库其他媒体数据统一
5. **数据可用时间**: 当天数据需在第二天凌晨 1:30 后才能拉取（按API服务器时区）
6. **数据保留期限**: API 生成的数据保留 1 个月
7. **查询时间范围**: 只支持查询最近半年的数据
8. **单次查询跨度**: 不超过 7 天
9. **幂等处理**: 每次运行会先删除同账户的历史数据
10. **日志**: 日志文件位于 `logs/mtg_etl.log`，支持日志轮转

## 更新日志

| 日期 | 更新内容 | 操作人 |
|------|---------|--------|
| 2026-01-10 | 初始版本，完成基础ETL流程；时区改为UTC 0 | Claude |
| 2026-01-10 | Media/MediaID 改为留空，由 Clickflare 填充 | Claude |
| 2026-01-10 | 支持多账户配置，自动循环拉取 | Claude |
