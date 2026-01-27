"""
创建 Clickflare Offers 详情表的脚本
"""
import clickhouse_connect

# 连接配置
client = clickhouse_connect.get_client(
    host='43.160.248.9',
    port=8123,
    database='ad_platform',
    username='default',
    password='admin123'
)

print("连接 ClickHouse 成功")

# 创建主表
create_table_sql = """
CREATE TABLE IF NOT EXISTS ad_platform.clickflare_offers_details (
  -- Offer 唯一标识
  offer_id String COMMENT 'Offer ID',
  workspace_id String COMMENT '工作区 ID',

  -- 基本信息
  user_id UInt64 COMMENT '用户 ID',
  name String COMMENT 'Offer 名称',
  url String COMMENT 'Offer URL 地址',
  notes String COMMENT '备注',

  -- Payout 配置
  payout_type String COMMENT 'Payout 类型: manual/auto',
  payout_amount Decimal(18,4) DEFAULT 0.0000 COMMENT 'Payout 金额',
  payout_currency String DEFAULT 'USD' COMMENT '货币类型: USD/EUR/CNY 等',

  -- 关联信息
  affiliate_network_id String COMMENT '关联的广告网络 ID',

  -- URL 信息
  direct_url String COMMENT '直接链接 URL',
  static_url String COMMENT '静态 URL（固定链接）',

  -- 其他配置
  is_direct UInt8 DEFAULT 0 COMMENT '是否直链: 0=否, 1=是',
  keyword_builder_mode String COMMENT '关键词模式: free_form/keyword_builder',

  -- 标签（数组存储为字符串，用逗号分隔）
  tags Array(String) DEFAULT [] COMMENT '标签列表',

  -- 元数据
  created_at DateTime DEFAULT now() COMMENT '创建时间',
  updated_at DateTime DEFAULT now() COMMENT '更新时间'
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (offer_id, workspace_id)
SETTINGS index_granularity = 8192
"""

print("创建 clickflare_offers_details 表...")
client.command(create_table_sql)
print("表创建成功！")

# 查看表结构
print("\n表结构:")
result = client.query("DESCRIBE ad_platform.clickflare_offers_details")
for row in result.named_results():
    print(f"  {row['name']}: {row['type']}")

# 检查是否有数据
count_result = client.query("SELECT count() as cnt FROM ad_platform.clickflare_offers_details")
count = count_result.first_row[0]
print(f"\n当前数据行数: {count}")

print("\n完成！")
