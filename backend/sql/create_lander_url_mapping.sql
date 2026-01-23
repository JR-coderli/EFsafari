-- Lander URL 映射表
-- 用于存储 ClickFlare Lander ID 到 URL 的映射关系
-- 支持前端跳转功能

CREATE TABLE IF NOT EXISTS ad_platform.dim_lander_url_mapping (
    landerID String COMMENT 'Lander ID (对应 ClickFlare landingID/_id)',
    landerName String COMMENT 'Lander 名称',
    landerUrl String COMMENT 'Lander URL 地址',
    created_at DateTime DEFAULT now() COMMENT '创建时间',
    updated_at DateTime DEFAULT now() COMMENT '更新时间'
)
ENGINE = MergeTree()
ORDER BY (landerID)
SETTINGS index_granularity = 8192;

-- 创建索引以加速按 landerName 查询
-- ClickHouse 使用 skip indexes
ALTER TABLE ad_platform.dim_lander_url_mapping
ADD INDEX idx_lander_name_bloom (landerName) TYPE bloom_filter GRANULARITY 1;
