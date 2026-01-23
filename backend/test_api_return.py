"""测试后端 API 返回的数据"""
import requests

# 使用从浏览器获取的 token（需要从 F12 -> Application -> Local Storage 复制）
# 或者直接查询数据库看看实际返回的数据

# 直接查询 ClickHouse 看看 landerUrl 是否存在
import clickhouse_connect
import yaml

config_path = "E:/code/bicode/backend/api/config.yaml"
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

ch_config = config.get("clickhouse", {})
client = clickhouse_connect.get_client(
    host=ch_config.get("host", "localhost"),
    port=ch_config.get("port", 8123),
    database=ch_config.get("database", "ad_platform"),
    username=ch_config.get("username", "default"),
    password=ch_config.get("password", ""),
)

# 模拟后端查询
query = """
SELECT
    t.lander as group_lander,
    m.landerUrl as landerUrl,
    sum(t.impressions) as impressions
FROM ad_platform.dwd_marketing_report_daily t
LEFT JOIN ad_platform.dim_lander_url_mapping m ON t.landerID = m.landerID
WHERE t.reportDate >= '2026-01-20' AND t.reportDate <= '2026-01-20'
GROUP BY t.lander, m.landerUrl
ORDER BY impressions DESC
LIMIT 5
"""

result = client.query(query)
print("ClickHouse 查询结果:")
for row in result.named_results():
    url = row.get('landerUrl', 'NULL')
    print(f"  lander: {row['group_lander']}")
    print(f"  landerUrl: {url}")
    print(f"  landerUrl type: {type(url)}")
    print()
