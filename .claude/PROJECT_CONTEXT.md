# AdData Dashboard - 项目上下文

## 项目概述
这是一个广告数据分析平台，包含 Python FastAPI 后端和 React 前端。

## 目录结构

```
bicode/
├── backend/                    # Python FastAPI 后端
│   ├── api/
│   │   ├── main.py            # FastAPI 入口，监听 8000 端口
│   │   ├── config.yaml        # 配置文件（ClickHouse 连接等）
│   │   ├── database.py        # ClickHouse 数据库连接
│   │   ├── auth.py            # JWT 认证
│   │   ├── models/            # 数据模型
│   │   ├── routers/           # API 路由
│   │   │   ├── dashboard.py   # 数据面板 API
│   │   │   ├── auth.py        # 认证 API
│   │   │   ├── daily_report.py # 日报 API
│   │   │   └── views.py       # 视图管理 API
│   │   ├── users/             # 用户管理
│   │   └── services/          # 业务逻辑层
│   ├── clickflare_etl/        # ETL 处理模块
│   └── sql/                   # SQL 脚本
├── EflowJRbi/                 # React 前端
│   ├── src/
│   │   ├── api/               # API 客户端
│   │   │   ├── client.ts      # 核心 API 客户端
│   │   │   ├── auth.ts        # 认证 API
│   │   │   └── hooks.ts       # React Hooks
│   │   ├── components/        # React 组件
│   │   └── services/          # 前端业务逻辑
│   ├── App.tsx                # 主应用
│   └── vite.config.ts         # Vite 配置（代理到 8000）
└── deploy/                     # 部署脚本
```

## 技术栈

### 后端
- **框架**: FastAPI 0.115.0
- **数据库**: ClickHouse (43.160.248.9:8123)
- **认证**: JWT (python-jose)
- **服务器**: Uvicorn (端口 8000)

### 前端
- **框架**: React 19.2.3 + TypeScript 5.8.2
- **构建**: Vite 6.2.0
- **样式**: Tailwind CSS

## 核心功能模块

| 模块 | 说明 | 关键文件 |
|------|------|----------|
| 数据面板 | 多维度透视表、下钻展开 | `routers/dashboard.py` |
| 用户认证 | JWT + 数据权限过滤 | `auth.py`, `routers/auth.py` |
| 日报管理 | 每日业绩统计、数据锁定 | `routers/daily_report.py` |
| 视图管理 | 保存/分享自定义视图 | `routers/views.py` |
| ETL | 自动拉取外部数据 | `clickflare_etl/` |

## 数据库信息

```yaml
# ClickHouse
host: 43.160.248.9
port: 8123
database: ad_platform
table: dwd_marketing_report_daily
```

**可用维度**: platform, advertiser, offer, campaign_name, sub_campaign_name, creative_name

**指标**: impressions, clicks, conversions, spend, revenue, ctr, cvr, roi, cpa

## API 路由

| 路由 | 方法 | 说明 |
|------|------|------|
| `/api/dashboard/data` | GET | 获取聚合数据 |
| `/api/dashboard/daily` | GET | 获取每日数据 |
| `/api/auth/login` | POST | 用户登录 |
| `/api/auth/verify` | POST | Token 验证 |
| `/api/users/` | GET/POST/PUT/DELETE | 用户管理 |
| `/api/daily-report/` | - | 日报相关 |
| `/api/views/` | - | 视图管理 |

## 启动命令

```bash
# 后端 (端口 8000)
cd backend
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# 前端 (端口 3000)
cd EflowJRbi
npm run dev
```

## 默认登录

- 用户名: `admin`
- 密码: `admin123`

## 重要约定

1. 每次启动项目检查端口占用，有则 kill 掉
2. 用中文回复
3. 避免过度工程化，保持简单
4. 数据权限基于 `allowedKeywords` 过滤

## 缓存策略

### 多层缓存架构

| 层级 | 时间 | 环境差异 | 说明 |
|------|------|----------|------|
| 前端缓存 | 5 分钟 | 开发环境禁用 | 浏览器内存，刷新页面清空 |
| 后端缓存 | 10 分钟 | 开发环境禁用 | Redis 共享，刷新页面保留 |
| 异步刷新 | TTL 前 2 分钟 | 仅生产环境 | 后台预热，用户无感知 |

### 缓存效果

- 首次查询：直接查 ClickHouse（7天约22秒，14天约40秒）
- 缓存命中：秒级响应
- 异步刷新：用户永远不等待慢查询

### 环境自动判断

- **开发环境**: `ENVIRONMENT` 未设置或为 `development` → 缓存自动禁用
- **生产环境**: `ENVIRONMENT=production` → 缓存自动启用

### 配置文件

- **后端**: `backend/api/config.yaml` → `redis.data_ttl: 600`
- **前端**: `EflowJRbi/src/api/hooks.ts` → 自动判断环境

## 服务器信息

### SSH 登接

```yaml
IP: 43.160.248.9
用户: root
密码: y?FJYqmp4*h@5
```

### 路径

```yaml
项目根目录: /opt/bicode/
后端代码: /opt/bicode/backend/
前端代码: /opt/bicode/EflowJRbi/
```

### 环境变量

```bash
export ENVIRONMENT=production
```

### 服务管理

```bash
# 重启后端
cd /opt/bicode/backend
pkill -f "uvicorn.*8001"
/opt/bicode/backend/start.sh &

# 查看日志
tail -f server.log
```

### 部署流程

**本地开发流程：**
1. 修改代码（本地）
2. 提交到 Git: `git commit -am "描述"`
3. 推送到 GitHub: `git push`

**服务器部署流程：**
```bash
# 登录服务器
ssh root@43.160.248.9

# 拉取最新代码并自动部署
cd /opt/bicode
./deploy/update.sh
```

**update.sh 自动执行：**
- 拉取最新代码
- 更新后端依赖
- 重新构建前端
- 清除 Redis 缓存
- 清除 Nginx 缓存
- 重启服务

---
## 使用说明

- 当前任务上下文使用 `/remember` 命令保存
- 架构变更时手动更新此文件
- 最近修改查看 `git log --oneline -10`
