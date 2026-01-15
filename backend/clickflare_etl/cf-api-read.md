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

| 需求字段 | groupBy 值 |
|----------|-----------|
| 日期 | `date` |
| 流量来源 | `trafficSourceID` |
| Offer | `offerID` |
| 广告网络 | `affiliateNetworkID` |
| Lander | `landingID` |
| trackingField1 | `trackingField1` |
| trackingField2 | `trackingField2` |
| trackingField5 | `trackingField5` |
| trackingField6 | `trackingField6` |

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
| reportDate | `date` | `date` | 报告日期 |
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
