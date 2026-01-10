# 前后端集成完成说明

## 项目概述

- **前端**: React 19 + TypeScript + Vite (EflowJRbi)
- **后端**: Python FastAPI (backend/api)
- **数据库**: ClickHouse (43.160.248.9:8123)

## 功能实现

### 1. 数据面板集成

#### 后端 API (`backend/api/`)

**文件结构:**
```
backend/api/
├── main.py              # FastAPI 应用入口
├── config.yaml          # 配置文件
├── database.py          # ClickHouse 连接管理
├── auth.py              # JWT 认证
├── models/
│   ├── schemas.py       # Pydantic 数据模型
│   └── user.py          # 用户模型
└── routers/
    ├── dashboard.py     # 数据面板 API
    ├── auth.py          # 认证 API
    └── users.py         # 用户管理 API
```

**API 端点:**

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/dashboard/health` | GET | 健康检查 |
| `/api/dashboard/data` | GET | 获取聚合数据 |
| `/api/dashboard/daily` | GET | 获取每日数据 |
| `/api/dashboard/platforms` | GET | 获取媒体平台列表 |
| `/api/dashboard/aggregate` | GET | 获取汇总指标 |
| `/api/dashboard/dimensions` | GET | 获取可用维度 |
| `/api/dashboard/metrics` | GET | 获取可用指标 |
| `/api/auth/login` | POST | 用户登录 |
| `/api/auth/verify` | POST | Token 验证 |
| `/api/users` | GET/POST | 用户列表/创建 |
| `/api/users/{id}` | PUT/DELETE | 更新/删除用户 |

#### 前端集成 (`EflowJRbi/src/api/`)

**文件:**
- `client.ts` - API 客户端，自动添加 Bearer Token
- `auth.ts` - 认证 API，Token 管理
- `hooks.ts` - 数据获取 hooks

### 2. 用户认证与权限

- JWT Token 认证
- SHA256 + 盐 密码哈希
- JSON 文件存储用户数据 (`backend/api/users.json`)
- Admin 权限控制
- 基于 `allowedKeywords` 的数据权限过滤

### 3. 动态维度系统

**可用维度:**
- `platform` (Media) - 媒体平台
- `advertiser` (Advertiser) - 广告主
- `offer` (Offer) - 广告
- `campaign_name` (Campaign) - 广告系列
- `sub_campaign_name` (Adset) - 广告组
- `creative_name` (Ads) - 创意

**功能:**
- 拖拽排序维度
- 点击添加/删除维度
- 自动重新加载数据
- 下钻层级按维度顺序展开

## 启动方式

### 后端

```bash
cd E:/code/bicode/backend
python -m uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload
```

### 前端

```bash
cd E:/code/bicode/EflowJRbi
npm run dev
```

访问: `http://localhost:3000`

## 配置

### Vite 代理 (`vite.config.ts`)

```typescript
proxy: {
  '/api': {
    target: 'http://localhost:8001',
    changeOrigin: true,
    secure: false,
  },
}
```

### ClickHouse 配置 (`backend/api/config.yaml`)

```yaml
clickhouse:
  host: "43.160.248.9"
  port: 8123
  database: "ad_platform"
  table: "dwd_marketing_report_daily"
  username: "default"
  password: "admin123"
```

## 数据流程

```
ClickHouse → FastAPI → React Frontend
                    ↓
                  JWT Auth
```

## 默认登录凭据

- 用户名: `admin`
- 密码: `admin123`

## 已修复的问题

1. ClickHouse 列名冲突 (`revenue` → `total_revenue`)
2. Datetime JSON 序列化
3. Pydantic v2 Config 冲突
4. ClickHouse 参数绑定（不支持 `?` 占位符）
5. 下钻层级边界条件 (`loadChildData`)
6. 日期格式返回为 `datetime.date` 对象

## 数据说明

当前数据库中的数据日期为 2026-01-09（测试数据），前端默认使用 "2026 Data" 日期范围。
