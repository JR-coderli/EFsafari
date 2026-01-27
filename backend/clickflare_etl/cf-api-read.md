# Clickflare Report API 接入说明

## 1. 基本信息

| 项目 | 值 |
|------|-----|
| API 文档 | https://developers.clickflare.io/#/paths/api-report/post |
| API 端点 | `POST https://public-api.clickflare.io/api/report` |
| 认证方式 | Header: `api-key` |
| API Key | `406561a67ff45389757647c936537da98f6c89a11776566dbe6efc8241c357f9.da59c8abbd8fbf4af7c3a5c72612d871a30273fa` |

---

## 2. 请求参数

### 必填参数 (Required)

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `startDate` | string | 开始日期，格式：`YYYY-MM-DD HH:mm:ss` |
| `endDate` | string | 结束日期，格式：`YYYY-MM-DD HH:mm:ss` |
| `groupBy` | array[string] | 分组维度（不能重复使用同一维度） |
| `timezone` | string | 时区，如 `UTC`、`Asia/Shanghai` |
| `sortBy` | string | 排序字段（必须是 metrics 中的字段） |
| `orderType` | string | 排序方式：`asc` / `desc` |
| `includeAll` | boolean | `true` 包含全部数据；`false` 仅返回有统计的数据 |

### 可选参数

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `metrics` | array[string] | 需要返回的指标 |
| `currency` | string | 货币类型 |
| `page` | integer | 页码（1-1000） |
| `pageSize` | number | 每页数量（1-1000） |
| `search` | string | 全文搜索 |
| `metricsFilters` | array[object] | 指标过滤器，格式：`{name, operator, value}` |
| `conversionTimestamp` | string | 转化时间戳类型：`visit`（点击时间）/ `postback`（回传时间） |
| `workspace_ids` | array[string] | 工作区 ID |

---

## 3. groupBy 可用维度

| 需求字段 | groupBy 值 | 说明 |
|----------|-----------|------|
| 日期 | `date` | 按天聚合（YYYY-MM-DD） |
| 日期时间（小时级） | `dateTime` | 精确到小时（YYYY-MM-DD HH:mm:ss） |
| 小时 | `hourOfDay` | 一天中的小时（0-23），跨多天会聚合同一小时 |
| 流量来源 | `trafficSourceID` | |
| Offer | `offerID` | |
| 广告网络 | `affiliateNetworkID` | |
| Lander | `landingID` | |
| trackingField1 | `trackingField1` | |
| trackingField2 | `trackingField2` | |
| trackingField5 | `trackingField5` | |
| trackingField6 | `trackingField6` | |

**完整 groupBy 可选值：**
```
locationRegion, visitID, externalID, clickID, connectionReferrer, connectionReferrerDomain, 
deviceOS, deviceOsVersion, deviceModel, deviceMainLanguage, offerID, landingID, connectionISP, 
connectionIP, deviceType, locationCountry, connectionType, locationCity, deviceBrowser, 
deviceBrand, deviceBrowserVersion, campaignID, trafficSourceID, flowID, nodeID, nodesPath, 
affiliateNetworkID, deviceUserAgent, botScore, queryStringRotation, queryStringRotationUnreplaced, 
postalCode, pathName, date, dateTime, hourOfDay, dayOfWeek, startOfMonth, 
trackingField1-20, param1-20
```

---

## 4. metrics 可用指标

| 需求字段 | metrics 值 |
|----------|-----------|
| impressions | `uniqueVisits` |
| clicks | `uniqueClicks` |
| conversions | `conversions` |
| revenue | `revenue` |
| 流量来源名称 | `trafficSourceName` |
| Offer 名称 | `offerName` |
| 广告网络名称 | `affiliateNetworkName` |
| Lander 名称 | `landingName` |

---

## 5. 请求示例

```bash
curl --request POST \
  --url 'https://public-api.clickflare.io/api/report' \
  --header 'api-key: YOUR_API_KEY' \
  --header 'Content-Type: application/json' \
  --data '{
    "startDate": "2026-01-09 00:00:00",
    "endDate": "2026-01-09 23:59:59",
    "groupBy": ["date", "trafficSourceID", "offerID", "affiliateNetworkID", "trackingField1", "trackingField2", "trackingField5", "trackingField6"],
    "metrics": ["uniqueVisits", "uniqueClicks", "conversions", "revenue", "trafficSourceName", "offerName", "affiliateNetworkName"],
    "timezone": "UTC",
    "sortBy": "uniqueVisits",
    "orderType": "desc",
    "includeAll": false,
    "page": 1,
    "pageSize": 1000
  }'
```

---

## 6. 响应示例

```json
{
  "items": [
    {
      "counter": 1,
      "uniqueVisits": 56115,
      "uniqueClicks": 6595,
      "conversions": 113,
      "revenue": 10735,
      "date": "2026-01-09",
      "trafficSourceID": "67fcdaffe4f5ce0012c5b4b3",
      "trafficSourceName": "Mintegral 2Events",
      "offerID": "683a65459a402b0012f131e2",
      "offerName": "TTL Adblock Base (Push/DSP)",
      "affiliateNetworkID": "67f644e65c657a0012e1a4bd",
      "affiliateNetworkName": "TTL",
      "trackingField1": "2196157011",
      "trackingField2": "123845",
      "trackingField5": "381801",
      "trackingField6": "AC01_Adblock_CPE_02"
    }
  ],
  "totals": {
    "counter": 10572,
    "uniqueVisits": 896241,
    "uniqueClicks": 146634,
    "conversions": 7618,
    "revenue": 68747.22393021
  }
}
```

---

## 7. 字段映射关系

| 本地字段 | API groupBy | API metrics/响应字段 | 说明 |
|----------|-------------|---------------------|------|
| reportDate | `date` | `date` | 报告日期（天级） |
| reportDateTime | `dateTime` | `dateTime` | 报告日期时间（小时级） |
| reportHour | `hourOfDay` | `hourOfDay` | 小时（0-23） |
| Media | `trafficSourceID` | `trafficSourceName` | 流量来源名称 |
| MediaID | `trafficSourceID` | `trafficSourceID` | 流量来源 ID |
| offer | `offerID` | `offerName` | Offer 名称 |
| offerID | `offerID` | `offerID` | Offer ID |
| advertiser | `affiliateNetworkID` | `affiliateNetworkName` | 广告网络名称 |
| advertiserID | `affiliateNetworkID` | `affiliateNetworkID` | 广告网络 ID |
| lander | `landingID` | `landingName` | Lander 名称 |
| landerID | `landingID` | `landingID` | Lander ID |
| Campaign | - | `trackingField2` | 活动名称 |
| CampaignID | `trackingField2` | `trackingField2` | 活动 ID |
| Adset | - | `trackingField6` | 广告组 |
| AdsetID | `trackingField5` | `trackingField5` | 广告组 ID |
| Ads | - | `trackingField1` | 广告 |
| AdsID | `trackingField1` | `trackingField1` | 广告 ID |
| impressions | - | `uniqueVisits` | 唯一访问数 |
| clicks | - | `uniqueClicks` | 唯一点击数 |
| conversions | - | `conversions` | 转化数 |
| revenue | - | `revenue` | 收入 |

---

## 8. 注意事项

1. **认证方式**：Header 使用 `api-key`（不是 `Authorization: Bearer`）
2. **日期格式**：必须使用 `YYYY-MM-DD HH:mm:ss` 格式
3. **必填参数**：`startDate`, `endDate`, `groupBy`, `timezone`, `sortBy`, `orderType`, `includeAll` 都是必填的
4. **sortBy 限制**：`sortBy` 的值必须是 `metrics` 数组中的一个字段
5. **分组限制**：`groupBy` 中不能重复使用相同的维度
6. **分页**：如数据量大，需要通过 `page` 和 `pageSize` 进行分页拉取
7. **转化时间**：`conversionTimestamp` 可选择按 `visit`（点击时间）或 `postback`（回传时间）统计
8. **Name 字段**：`trafficSourceName`、`offerName`、`affiliateNetworkName` 需要放在 `metrics` 中，不是 `groupBy`

---

## 9. 已验证 ✅

- API 连通性：已验证通过
- 测试时间：2026-01-10
- 所有字段已验证：
  - ✅ groupBy: `date`, `trafficSourceID`, `offerID`, `affiliateNetworkID`, `trackingField1`, `trackingField2`, `trackingField5`, `trackingField6`
  - ✅ metrics: `uniqueVisits`, `uniqueClicks`, `conversions`, `revenue`, `trafficSourceName`, `offerName`, `affiliateNetworkName`

---

## 10. Lander 列表 API（获取 Lander URL）

Report API 不支持直接获取 Lander URL，需要通过独立的 Lander 列表 API 获取。

### 基本信息

| 项目 | 值 |
|------|-----|
| API 文档 | https://developers.clickflare.io/#/paths/api-landings/get |
| API 端点 | `GET https://public-api.clickflare.io/api/landings` |
| 认证方式 | Header: `api-key` |

### 请求示例

```bash
curl --request GET \
  --url 'https://public-api.clickflare.io/api/landings' \
  --header 'Accept: application/json' \
  --header 'api-key: YOUR_API_KEY'
```

### 响应字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `workspace_id` | any | 工作区 ID |
| `user_id` | number | 用户 ID |
| `domain_id` | string | 域名 ID |
| `name` | string | Lander 名称 |
| `url` | string | **Lander URL 地址** |
| `cta_count` | number | CTA 数量 |
| `notes` | string | 备注 |
| `is_prelander` | boolean | 是否是 Pre-lander |
| `tracking_info` | object | 追踪信息（含 `tracking_domain_id`） |

### 响应示例

```json
[
  {
    "workspace_id": null,
    "user_id": 12345,
    "domain_id": "abc123",
    "name": "My Landing Page",
    "url": "https://example.com/landing",
    "cta_count": 1,
    "notes": "test lander",
    "is_prelander": false,
    "tracking_info": {
      "tracking_domain_id": "domain_xxx"
    }
  }
]
```

### 使用场景

1. 调用 `GET /api/landings` 获取所有 Lander 列表及 URL
2. 使用返回的 Lander ID 与 Report API 中的 `landingID` 进行关联
3. 可在本地建立 Lander ID → URL 的映射表

---

## 11. Offer 列表 API（获取 Offer 详情）

Report API 不支持直接获取 Offer URL、Payout 等详细信息，需要通过独立的 Offer 列表 API 获取。

### 基本信息

| 项目 | 值 |
|------|-----|
| API 文档 | https://developers.clickflare.io/#/paths/api-offers/get |
| API 端点 | `GET https://public-api.clickflare.io/api/offers` |
| 认证方式 | Header: `api-key` |

### 请求参数

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `fields[]` | array[string] | 可选，指定返回的字段 |
| `page` | number | 页码（>= 1） |
| `pageSize` | number | 每页数量（>= 1） |
| `search` | string | 搜索关键词 |

### 请求示例

```bash
curl --request GET \
  --url 'https://public-api.clickflare.io/api/offers' \
  --header 'Accept: application/json' \
  --header 'api-key: YOUR_API_KEY'
```

### 响应字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `workspace_id` | any | 工作区 ID |
| `user_id` | number | 用户 ID |
| `name` | string | **Offer 名称** |
| `url` | string | **Offer URL 地址** |
| `notes` | string | 备注 |
| `keywords` | array[object] | 关键词列表 |
| `payout` | object | **Payout 配置**（见下方详情） |
| `direct` | boolean | 是否直链 |
| `affiliateNetworkID` | string | **关联的广告网络 ID** |
| `conversionTracking` | object | 转化追踪配置 |
| `staticUrl` | string/null | **静态 URL**（固定链接） |
| `keywordBuilderMode` | string | 关键词模式：`free_form` / `keyword_builder` |
| `tags` | array[string] | **标签列表** |

### payout 对象详情

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | string | **Payout 类型**：`manual`（手动）/ `auto`（自动） |
| `payout` | number | **Payout 金额** |
| `currency` | string | **货币类型**：`USD`、`EUR`、`CNY` 等 |
| `geo` | array[object] | 按地区的 Payout 配置 |

### 响应示例

```json
[
  {
    "workspace_id": null,
    "user_id": 12345,
    "name": "My Offer",
    "url": "https://offer.example.com/click?id={clickid}",
    "notes": "High converting offer",
    "keywords": [
      {
        "id": "kw_001",
        "name": "clickid",
        "weight": 1,
        "collapsed": true,
        "encodedQueryString": "clickid={clickid}"
      }
    ],
    "payout": {
      "type": "manual",
      "payout": 0.5,
      "currency": "USD",
      "geo": [
        {
          "location": "US",
          "payout": 0.8,
          "currency": "USD"
        }
      ]
    },
    "direct": false,
    "affiliateNetworkID": "67f644e65c657a0012e1a4bd",
    "conversionTracking": {
      "trackingDomainID": "domain_xxx",
      "trackingMethod": "S2S",
      "includeAdditionalParams": false
    },
    "staticUrl": null,
    "keywordBuilderMode": "free_form",
    "tags": ["CPA", "Mobile"]
  }
]
```

### 重点字段说明

| 需求 | 字段 | 说明 |
|------|------|------|
| Offer URL | `url` | Offer 的目标链接 |
| 静态 URL | `staticUrl` | 固定的静态链接（如有） |
| Payout 金额 | `payout.payout` | 单次转化的收益金额 |
| Payout 类型 | `payout.type` | `manual` 手动设置 / `auto` 自动从回传获取 |
| 货币 | `payout.currency` | 如 `USD`、`EUR` |
| 广告网络 | `affiliateNetworkID` | 可与 Report API 的 `affiliateNetworkID` 关联 |
| 标签 | `tags` | Offer 的分类标签 |

### 使用场景

1. 调用 `GET /api/offers` 获取所有 Offer 的详情（URL、Payout、标签等）
2. 使用返回的 Offer ID 与 Report API 中的 `offerID` 进行关联
3. 获取 Payout 配置用于成本/收益计算
