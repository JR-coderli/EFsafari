"""检查 landerID 与映射表的匹配情况"""
import clickhouse_connect
import yaml

# 读取配置
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

print("=== 检查主表中的 landerID ===")
query1 = """
SELECT DISTINCT landerID, lander
FROM ad_platform.dwd_marketing_report_daily
WHERE landerID != ''
LIMIT 5
"""
result1 = client.query(query1)
for row in result1.named_results():
    print(f"ID: {row['landerID']}, Name: {row['lander']}")

print("\n=== 检查映射表中的数据 ===")
query2 = """
SELECT landerID, landerName, landerUrl
FROM ad_platform.dim_lander_url_mapping
LIMIT 5
"""
result2 = client.query(query2)
for row in result2.named_results():
    print(f"ID: {row['landerID']}, Name: {row['landerName']}, URL: {row['landerUrl']}")

print("\n=== 检查主表 landerID 是否在映射表中 ===")
query3 = """
SELECT
    t.landerID,
    t.lander,
    m.landerUrl
FROM ad_platform.dwd_marketing_report_daily t
LEFT JOIN ad_platform.dim_lander_url_mapping m ON t.landerID = m.landerID
WHERE t.landerID != ''
LIMIT 10
"""
result3 = client.query(query3)
matched = 0
for row in result3.named_results():
    has_url = "YES" if row['landerUrl'] else "NO"
    if row['landerUrl']:
        matched += 1
    print(f"ID: {row['landerID']}, Name: {row['lander']}, URL: [{has_url}]")

print(f"\n匹配率: {matched}/10")

print("\n=== 检查数据类型是否匹配 ===")
query4 = """
SELECT
    toTypeName(t.landerID) as main_id_type,
    toTypeName(m.landerID) as mapping_id_type
FROM ad_platform.dwd_marketing_report_daily t
CROSS JOIN ad_platform.dim_lander_url_mapping m
LIMIT 1
"""
result4 = client.query(query4)
for row in result4.named_results():
    print(f"主表 landerID 类型: {row['main_id_type']}")
    print(f"映射表 landerID 类型: {row['mapping_id_type']}")
