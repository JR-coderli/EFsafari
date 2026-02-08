import { Dimension, MetricConfig, AdRow } from '../types';

export const ALL_DIMENSIONS: { value: Dimension; label: string }[] = [
  { value: 'platform', label: 'Media' },
  { value: 'advertiser', label: 'Advertiser' },
  { value: 'offer', label: 'Offer' },
  { value: 'lander', label: 'Lander' },
  { value: 'campaign_name', label: 'Campaign' },
  { value: 'sub_campaign_name', label: 'Adset' },
  { value: 'creative_name', label: 'Ads' },
  { value: 'date', label: 'Date' },
];

export const DEFAULT_METRICS: MetricConfig[] = [
  { key: 'spend', label: 'Spend', visible: true, type: 'money', group: 'Basic' },
  { key: 'conversions', label: 'Conversions', visible: true, type: 'number', group: 'Basic' },
  { key: 'revenue', label: 'Revenue', visible: true, type: 'money', group: 'Basic' },
  { key: 'profit', label: 'Profit', visible: true, type: 'profit' as const, group: 'Basic' },
  { key: 'impressions', label: 'Impressions', visible: true, type: 'number', group: 'Basic' },
  { key: 'clicks', label: 'Clicks', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_imp', label: 'm_imp', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_clicks', label: 'm_clicks', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_conv', label: 'm_conv', visible: true, type: 'number', group: 'Basic' },
  { key: 'ctr', label: 'CTR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'cvr', label: 'CVR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'roi', label: 'ROI', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'cpa', label: 'CPA', visible: true, type: 'money', group: 'Calculated' },
  { key: 'epa', label: 'EPA', visible: true, type: 'money', group: 'Calculated' },
  { key: 'epc', label: 'EPC', visible: true, type: 'money', group: 'Calculated' },
  { key: 'epv', label: 'EPV', visible: true, type: 'money', group: 'Calculated' },
  { key: 'm_epc', label: 'm_EPC', visible: false, type: 'money', group: 'Calculated' },
  { key: 'm_epv', label: 'm_EPV', visible: false, type: 'money', group: 'Calculated' },
  { key: 'm_cpa', label: 'm_CPA', visible: false, type: 'money', group: 'Calculated' },
  { key: 'm_epa', label: 'm_EPA', visible: false, type: 'money', group: 'Calculated' },
];

export const calculateMetrics = (data: {
  impressions: number;
  clicks: number;
  conversions: number;
  spend: number;
  revenue: number;
  m_imp: number;
  m_clicks: number;
  m_conv: number;
}) => {
  return {
    ctr: data.clicks / (data.impressions || 1),
    cvr: data.conversions / (data.clicks || 1),
    roi: data.spend > 0 ? (data.revenue - data.spend) / data.spend : 0,
    cpa: data.spend / (data.conversions || 1),
    rpa: data.revenue / (data.conversions || 1),
    epc: data.revenue / (data.clicks || 1),
    epv: data.revenue / (data.impressions || 1),
    m_epc: data.revenue / (data.m_clicks || 1),
    m_epv: data.revenue / (data.m_imp || 1),
    m_cpc: data.spend / (data.m_clicks || 1),
    m_cpv: data.spend / (data.m_imp || 1),
    m_cpa: data.spend / (data.m_conv || 1),
    m_epa: data.revenue / (data.m_conv || 1)
  };
};
