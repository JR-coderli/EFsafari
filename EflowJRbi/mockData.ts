
import { AdRow, Dimension, DailyBreakdown } from './types';

const MOCK_VALUES: Record<Dimension, string[]> = {
  platform: ['Facebook', 'Google', 'TikTok', 'Mintegral'],
  advertiser: ['Nike', 'Adidas', 'Coca-Cola', 'Samsung', 'Amazon'],
  offer: ['Black_Friday_Special', 'New_User_Bonus', 'Spring_Collection_50', 'Flash_Sale_Electronics'],
  campaign_name: ['Summer_Sale_2026', 'Brand_Awareness_Global', 'Retargeting_Q1', 'Influencer_Campaign'],
  sub_campaign_name: ['AdSet_Interest_Tech', 'AdGroup_Broad_US', 'Lookalike_Purchasers', 'Video_Placements'],
  creative_name: ['Video_15s_Square', 'Static_Banner_Blue', 'Carousel_Product_A', 'Playable_Ad_V2']
};

const calcMetrics = (data: { impressions: number; clicks: number; conversions: number; spend: number; revenue: number; m_imp: number; m_clicks: number }) => {
  return {
    ctr: data.clicks / (data.impressions || 1),
    cvr: data.conversions / (data.clicks || 1),
    roi: (data.revenue - data.spend) / (data.spend || 1),
    cpa: data.spend / (data.conversions || 1),
    rpa: data.revenue / (data.conversions || 1),
    epc: data.revenue / (data.clicks || 1),
    epv: data.revenue / (data.impressions || 1),
    m_epc: data.revenue / (data.m_clicks || 1),
    m_epv: data.revenue / (data.m_imp || 1),
    m_cpc: data.spend / (data.m_clicks || 1),
    m_cpv: data.spend / (data.m_imp || 1)
  };
};

const generateDailyData = (baseImpressions: number): DailyBreakdown[] => {
  const data: DailyBreakdown[] = [];
  const baseDate = new Date('2026-01-01T00:00:00');
  for (let i = 0; i < 30; i++) {
    const date = new Date(baseDate);
    date.setDate(baseDate.getDate() + i);
    const dailyImps = Math.floor((baseImpressions / 30) * (0.8 + Math.random() * 0.4)) + 10;
    const clicks = Math.floor(dailyImps * (0.02 + Math.random() * 0.05)) + 1;
    const spend = clicks * (0.5 + Math.random()) + 0.5;
    const conversions = Math.floor(clicks * (0.05 + Math.random() * 0.1));
    data.push({
      date: date.toISOString().split('T')[0],
      impressions: dailyImps,
      clicks: clicks,
      conversions: conversions,
      spend: spend,
      revenue: conversions * (15 + Math.random() * 10),
      m_imp: Math.floor(dailyImps * (0.95 + Math.random() * 0.1)),
      m_clicks: Math.floor(clicks * (0.98 + Math.random() * 0.04)),
      m_conv: Math.floor(conversions * (0.9 + Math.random() * 0.2))
    });
  }
  return data.reverse();
};

export const generateMockReport = (
  dimension: Dimension,
  level: number,
  parentId: string = '',
  remainingDimensions: Dimension[],
  selectedRange: string = 'Last 7 Days'
): AdRow[] => {
  const values = MOCK_VALUES[dimension] || [`Item_${Math.floor(Math.random() * 100)}`];
  const todayStr = '2026-01-09';
  return values.map((val) => {
    const baseImpressions = Math.random() * 200000 + 50000;
    const dailyData = generateDailyData(baseImpressions);
    let filtered = dailyData;
    if (selectedRange === 'Today') filtered = dailyData.filter(d => d.date === todayStr);
    else if (selectedRange === 'Yesterday') filtered = dailyData.filter(d => d.date === '2026-01-08');
    else if (selectedRange === 'Last 7 Days') filtered = dailyData.slice(0, 7);
    const totals = filtered.reduce((acc, curr) => ({
      impressions: acc.impressions + curr.impressions,
      clicks: acc.clicks + curr.clicks,
      conversions: acc.conversions + curr.conversions,
      spend: acc.spend + curr.spend,
      revenue: acc.revenue + curr.revenue,
      m_imp: acc.m_imp + curr.m_imp,
      m_clicks: acc.m_clicks + curr.m_clicks,
      m_conv: acc.m_conv + curr.m_conv,
    }), { impressions: 0, clicks: 0, conversions: 0, spend: 0, revenue: 0, m_imp: 0, m_clicks: 0, m_conv: 0 });
    return {
      id: parentId ? `${parentId}|${val}` : val,
      name: val,
      level,
      dimensionType: dimension,
      ...totals,
      ...calcMetrics(totals),
      hasChild: remainingDimensions.length > 0,
      dailyData: dailyData,
      campaign_id: `cmp_${Math.floor(Math.random() * 1000000)}`,
      adset_id: `set_${Math.floor(Math.random() * 1000000)}`,
      ads_id: `ad_${Math.floor(Math.random() * 1000000)}`
    };
  });
};
