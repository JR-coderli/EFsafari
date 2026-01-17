# MTG (Mintegral) Report API 接入说明

## 1. 基本信息

| 项目 | 值 |
|------|-----|
| API 文档 | 内部文档（请联系 MTG 技术支持） |
| API 端点 | `GET https://ss-api.mintegral.com/api/v2/reports/data` |
| 认证方式 | Header: `access-key`, `Token`, `Timestamp` |
| 时区 | UTC+0 |

---

## 2. 账户配置

| 账户 | Access Key | API Key |
|------|-----------|---------|
| Account1 | `28e3b671a1c06171448e85233b65e0e0` | `25e8f524265bb39fa0521ea7f72924e4` |
| Account2 | `00c6e1387b44dc94d5d7df4256ed07eb` | `ea70b2e328b7c11b1ecec66dea2d1a49` |
| Account3 | `e0c55f0bc62515cfbb08c21d98ca7776` | `1c23a2044c0dac36cf7a0671e075c3d9` |

---

## 3. Token 生成算法

MTG API 使用 MD5 签名进行认证：

```python
import hashlib
import time

def generate_token(api_key: str) -> tuple[str, str]:
    """
    生成 MTG API 认证所需的 Timestamp 和 Token

    Args:
        api_key: 账户的 API Key

    Returns:
        (timestamp, token) 元组
    """
    timestamp = str(int(time.time()))

    # Token = MD5(API_KEY + MD5(timestamp))
    ts_md5 = hashlib.md5(timestamp.encode("utf-8")).hexdigest()
    raw = api_key + ts_md5
    token = hashlib.md5(raw.encode("utf-8")).hexdigest()

    return timestamp, token
```

**示例：**
```
Timestamp: "1737132000"
Token: "a1b2c3d4e5f6..." (MD5 哈希值)
```

---

## 4. 请求参数

### 请求流程

MTG Report API 采用**两步请求模式**：

1. **Initiate (type=1)**: 发起报告请求，获取报告状态
2. **Poll (type=2)**: 下载报告数据（TSV 格式）

### 参数说明

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `start_time` | string | 开始日期，格式：`YYYY-MM-DD` |
| `end_time` | string | 结束日期，格式：`YYYY-MM-DD` |
| `type` | integer | 请求类型：`1`=发起报告，`2`=下载数据 |
| `timezone` | string | 时区偏移，如 `0` (UTC+0)、`8` (UTC+8) |
| `dimension_option` | string | 分组维度（逗号分隔） |
| `time_granularity` | string | 时间粒度：`daily` / `hourly` |

### 响应码 (Response Code)

| Code | 说明 | 处理方式 |
|------|------|---------|
| 200 | 成功，报告已就绪 | 直接 type=2 下载 |
| 201 | 已接收，正在生成 | 轮询等待 |
| 202 | 生成中 | 轮询等待 |
| 203 | 无该请求 | 重新发起 |
| 204 | 未就绪 | 轮询等待 |
| 205 | 已过期 | 重新发起 |
| 10000 | 错误 | 检查参数 |

---

## 5. dimension_option 可用维度

| dimension_option 值 | 说明 |
|---------------------|------|
| `Offer` | Offer (对应 Clickflare 的 AdsetID) |
| `Campaign` | Campaign |
| `Creative` | Creative (对应 Clickflare 的 AdsID) |
| `Country` | 国家 |
| `App` | 应用 |
| `Placement` | 广告位 |

**当前使用配置：** `Offer,Campaign,Creative`

---

## 6. 响应数据格式 (TSV)

### 下载方式

```
GET https://ss-api.mintegral.com/api/v2/reports/data?type=2&...
```

响应为 **TSV (Tab-Separated Values)** 格式，需要解析：

```python
def parse_tsv_data(tsv_content: str) -> list[dict]:
    """解析 MTG TSV 数据"""
    lines = tsv_content.strip().split("\n")
    headers = [h.strip() for h in lines[0].split("\t")]

    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        values = line.split("\t")
        row = {}
        for i, header in enumerate(headers):
            if i < len(values):
                row[header] = values[i].strip()
        rows.append(row)  # 注意：append 必须在 for 循环外！

    return rows
```

### TSV 列说明

| 列名 | 说明 | 示例值 |
|------|------|--------|
| `Date` | 日期 | `20260115` |
| `Offer Id` | Offer ID | `381801` |
| `Offer Uuid` | Offer UUID | - |
| `Offer Name` | Offer 名称 | - |
| `Campaign Id` | Campaign ID | `123845` |
| `Creative Id` | Creative ID | `2196157011` |
| `Creative Name` | Creative 名称 | - |
| `Currency` | 货币 | `USD` |
| `Impression` | 展示次数 | `75285` |
| `Click` | 点击次数 | `1234` |
| `Conversion` | 转化次数 | `56` |
| `Ecpm` | 千次展示收益 | `1.23` |
| `Cpc` | 点击成本 | `0.45` |
| `Ctr` | 点击率 | `1.64` |
| `Cvr` | 转化率 | `4.54` |
| `Ivr` | 展示转化率 | `0.07` |
| `Spend` | 消费金额 | `123.45` |

---

## 7. 请求示例

### Step 1: 发起报告 (type=1)

```bash
curl --request GET \
  --url 'https://ss-api.mintegral.com/api/v2/reports/data?start_time=2026-01-15&end_time=2026-01-15&type=1&timezone=0&dimension_option=Offer,Campaign,Creative&time_granularity=daily' \
  --header 'access-key: YOUR_ACCESS_KEY' \
  --header 'Token: YOUR_TOKEN' \
  --header 'Timestamp: YOUR_TIMESTAMP'
```

**响应：**
```json
{
  "code": 200,
  "msg": "Generate success, please use type = 2 to get data"
}
```

### Step 2: 下载数据 (type=2)

```bash
curl --request GET \
  --url 'https://ss-api.mintegral.com/api/v2/reports/data?start_time=2026-01-15&end_time=2026-01-15&type=2&timezone=0&dimension_option=Offer,Campaign,Creative&time_granularity=daily' \
  --header 'access-key: YOUR_ACCESS_KEY' \
  --header 'Token: YOUR_TOKEN' \
  --header 'Timestamp: YOUR_TIMESTAMP'
```

**响应 (TSV 格式)：**
```
Date	Offer Id	Offer Uuid	Offer Name	Campaign Id	Creative Id	Creative Name	Currency	Impression	Click	Conversion	Ecpm	Cpc	Ctr	Cvr	Ivr	Spend
20260115	381801			123845	2196157011		USD	75285	1234	56	1.64	0.45	1.64	4.54	0.07	123.45
...
```

---

## 8. 字段映射关系

| 本地字段 | MTG API 字段 | 说明 |
|----------|-------------|------|
| reportDate | `Date` | 报告日期 (YYYYMMDD → Date) |
| CampaignID | `Campaign Id` | Campaign ID |
| AdsetID | `Offer Id` | Offer ID（匹配 Clickflare AdsetID） |
| AdsID | `Creative Id` | Creative ID |
| Adset | `Offer Name` | Offer 名称 |
| Ads | `Creative Name` | Creative 名称 |
| spend | `Spend` | MTG 消费金额 |
| m_imp | `Impression` | MTG 展示次数 |
| m_clicks | `Click` | MTG 点击次数 |
| m_conv | `Conversion` | MTG 转化次数 |

**注意：** MTG 数据用于补充 Clickflare 中 Mintegral/Hastraffic 媒体的 spend，按 AdsetID 匹配合并。

---

## 9. 重试与轮询策略

### 重试配置 (config.yaml)

```yaml
retry:
  max_attempts: 3        # 最大重试次数
  backoff_factor: 2      # 退避因子 (2^n 秒)

poll:
  max_attempts: 20       # 最大轮询次数
  interval_seconds: 15   # 轮询间隔
  timeout_seconds: 300   # 轮询超时
```

### 轮询流程

```
发起请求 (type=1)
    ↓
响应码 = 200?
    ↓ No
等待 15 秒
    ↓
重新轮询 (最多 20 次)
    ↓
超时或成功
```

---

## 10. 注意事项

1. **认证方式**：需同时发送 `access-key`、`Token`、`Timestamp` 三个 Header
2. **Token 算法**：`Token = MD5(API_KEY + MD5(timestamp))`
3. **两步请求**：先 type=1 发起，再 type=2 下载
4. **轮询等待**：报告生成需要时间，需轮询直到 code=200
5. **TSV 解析**：响应是 TSV 格式，需要正确解析（注意 append 位置！）
6. **时区设置**：所有 report API 使用 UTC+0
7. **多账户**：需循环调用 3 个账户的数据并合并
8. **数据合并**：MTG 数据按 AdsetID (Offer Id) 与 Clickflare 数据匹配

---

## 11. 已验证 ✅

- API 连通性：已验证通过
- 测试时间：2026-01-17
- 所有字段已验证：
  - ✅ dimension: `Offer`, `Campaign`, `Creative`
  - ✅ metrics: `Impression`, `Click`, `Conversion`, `Spend`
- 3 个账户均已验证：Account1, Account2, Account3

---

## 12. Bug 修复记录

| 日期 | Bug | 修复 |
|------|-----|------|
| 2026-01-17 | `parse_tsv_data` 中 `rows.append(row)` 在 for 循环内，导致每行被添加 17 次（列数） | 将 `rows.append(row)` 移到 for 循环外 |
