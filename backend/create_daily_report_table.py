"""
创建 Daily Report 表的脚本
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
CREATE TABLE IF NOT EXISTS ad_platform.dwd_daily_report (
  reportDate Date COMMENT '报告日期',
  Media String COMMENT '媒体来源',

  -- 基础指标（只读，从源表同步）
  impressions UInt64 DEFAULT 0 COMMENT '展示次数',
  clicks UInt64 DEFAULT 0 COMMENT '点击次数',
  conversions UInt64 DEFAULT 0 COMMENT '转化次数',
  revenue Decimal(18,4) DEFAULT 0.0000 COMMENT '收入',

  -- 可修正指标
  spend_original Decimal(18,4) DEFAULT 0.0000 COMMENT '原始花费',
  spend_manual Decimal(18,4) DEFAULT 0.0000 COMMENT '手动修正花费',
  spend_final Decimal(18,4) DEFAULT 0.0000 COMMENT '最终花费 = spend_original + spend_manual',

  -- 移动端指标（只读）
  m_imp UInt64 DEFAULT 0 COMMENT '移动端展示',
  m_clicks UInt64 DEFAULT 0 COMMENT '移动端点击',
  m_conv UInt64 DEFAULT 0 COMMENT '移动端转化',

  -- 锁定状态（按日期锁定）
  is_locked UInt8 DEFAULT 0 COMMENT '是否锁定: 0=未锁定(自动同步), 1=已锁定(不同步)',

  -- 元数据
  created_at DateTime DEFAULT now() COMMENT '创建时间',
  updated_at DateTime DEFAULT now() COMMENT '更新时间',
  last_modified_by String DEFAULT '' COMMENT '最后修改人'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(reportDate)
ORDER BY (reportDate, Media)
SETTINGS index_granularity = 8192
"""

print("创建 dwd_daily_report 表...")
client.command(create_table_sql)
print("表创建成功！")

# 查看表结构
print("\n表结构:")
result = client.query("DESCRIBE ad_platform.dwd_daily_report")
for row in result.named_results():
    print(f"  {row['name']}: {row['type']}")

# 检查是否有数据
count_result = client.query("SELECT count() as cnt FROM ad_platform.dwd_daily_report")
count = count_result.first_row[0]
print(f"\n当前数据行数: {count}")

print("\n完成！")
