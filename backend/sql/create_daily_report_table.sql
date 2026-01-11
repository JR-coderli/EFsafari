-- Daily Report 表创建脚本
-- 独立的日报数据表，支持 spend 手动修正

-- 创建主表
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

  -- 元数据
  created_at DateTime DEFAULT now() COMMENT '创建时间',
  updated_at DateTime DEFAULT now() COMMENT '更新时间',
  last_modified_by String DEFAULT '' COMMENT '最后修改人'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(reportDate)
ORDER BY (reportDate, Media)
SETTINGS index_granularity = 8192;

-- 创建物化视图：自动从主表同步数据（每日聚合到 media 维度）
-- 这个物化视图会自动将 dwd_marketing_report_daily 的数据按 reportDate 和 Media 聚合
-- 注意：首次创建时会用 POPULATE 填充历史数据
DROP TABLE IF EXISTS ad_platform.mv_daily_report_sync;

CREATE MATERIALIZED VIEW IF NOT EXISTS ad_platform.mv_daily_report_sync
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(reportDate)
ORDER BY (reportDate, Media)
POPULATE
AS SELECT
  reportDate,
  Media,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  sum(conversions) as conversions,
  sum(revenue) as revenue,
  sum(spend) as spend_original,
  toDecimal64(0, 4) as spend_manual,
  sum(spend) as spend_final,
  sum(m_imp) as m_imp,
  sum(m_clicks) as m_clicks,
  sum(m_conv) as m_conv,
  now() as created_at,
  now() as updated_at,
  '' as last_modified_by
FROM ad_platform.dwd_marketing_report_daily
GROUP BY reportDate, Media;

-- 创建触发器视图用于增量更新（可选）
-- 当源表有新数据时自动同步
