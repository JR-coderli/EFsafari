"""
添加 platform MATERIALIZED 列到 dwd_marketing_report_daily 表
platform 列会自动从 Media 列提取第一个单词
"""
import clickhouse_connect
import yaml
import os

# 读取配置
config_path = os.path.join(os.path.dirname(__file__), "api/config.yaml")
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

ch_config = config.get("clickhouse", {})
host = ch_config.get("host", "localhost")
port = ch_config.get("port", 8123)
database = ch_config.get("database", "ad_platform")

print(f"连接 ClickHouse: {host}:{port}/{database}")

client = clickhouse_connect.get_client(
    host=host,
    port=port,
    database=database,
    username=ch_config.get("username", "default"),
    password=ch_config.get("password", ""),
)

# 检查 platform 列是否已存在
check_sql = f"SELECT count() FROM system.columns WHERE database = '{database}' AND table = 'dwd_marketing_report_daily' AND name = 'platform'"
result = client.query(check_sql)
exists = result.first_row[0] > 0

if exists:
    print("platform 列已存在，跳过创建")
else:
    # 添加 MATERIALIZED 列
    alter_sql = f"""
    ALTER TABLE {database}.dwd_marketing_report_daily
    ADD COLUMN IF NOT EXISTS platform String MATERIALIZED splitByString(' ', Media)[1]
    """
    print(f"执行 SQL: {alter_sql}")
    client.command(alter_sql)
    print("platform 列添加成功！")

# 验证列是否添加成功
describe_sql = f"DESCRIBE {database}.dwd_marketing_report_daily"
result = client.query(describe_sql)
print("\n表结构中的 platform 列:")
for row in result.named_results():
    if row['name'] == 'platform':
        print(f"  {row['name']}: {row['type']} (default: {row.get('default', 'N/A')})")

# 测试查询，验证 platform 值是否正确计算
test_sql = f"""
SELECT
    Media,
    platform,
    splitByString(' ', Media)[1] as expected_platform
FROM {database}.dwd_marketing_report_daily
LIMIT 5
"""
print("\n测试查询结果:")
result = client.query(test_sql)
for row in result.named_results():
    print(f"  Media='{row['Media']}' -> platform='{row['platform']}' (expected='{row['expected_platform']}')")

print("\n完成！")
