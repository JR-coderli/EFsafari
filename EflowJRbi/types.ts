
export type UserRole = 'admin' | 'ops' | 'ops02' | 'business';

export type Dimension =
  | 'platform'
  | 'advertiser'
  | 'offer'
  | 'lander'
  | 'campaign_name'
  | 'sub_campaign_name'
  | 'creative_name'
  | 'date';

export interface DailyBreakdown {
  date: string;
  impressions: number;
  clicks: number;
  conversions: number;
  spend: number;
  revenue: number;
  profit: number;
  m_imp: number;
  m_clicks: number;
  m_conv: number;
  // Calculated metrics
  ctr: number;
  cvr: number;
  roi: number;
  cpa: number;
  epa: number;  // Revenue per action (conversion)
  epc: number;  // Earnings per click
  epv: number;  // Earnings per view
  m_epc: number;
  m_epv: number;
  m_cpc: number;
  m_cpv: number;
  m_cpa: number;  // Mobile CPA
  m_epa: number;  // Mobile EPA
}

export interface AdRow {
  id: string;
  name: string;
  level: number;
  dimensionType: Dimension;
  impressions: number;
  clicks: number;
  conversions: number;
  spend: number;
  revenue: number;
  profit: number;
  m_imp: number;
  m_clicks: number;
  m_conv: number;
  // Computed Metrics
  ctr: number;
  cvr: number;
  roi: number;
  cpa: number;
  rpa: number;
  epa: number;  // Revenue per action (same as rpa)
  epc: number;
  epv: number;
  m_epc: number;
  m_epv: number;
  m_cpc: number;
  m_cpv: number;
  m_cpa: number;  // Mobile CPA
  m_epa: number;  // Mobile EPA

  hasChild: boolean;
  isExpanded?: boolean;
  children?: AdRow[];
  dailyData?: DailyBreakdown[];
  campaign_id?: string;
  adset_id?: string;
  ads_id?: string;
  // Filter path for this row (used to load daily breakdown data)
  filterPath?: Array<{ dimension: Dimension; value: string }>;
  // Spend manual adjustment (non-zero if manually edited)
  spend_manual?: number;
  // Lander URL (for lander dimension)
  landerUrl?: string;
  // Offer details (for offer dimension)
  offerUrl?: string;
  offerNotes?: string;
  offerId?: string;
}

export interface MetricConfig {
  key: keyof AdRow;
  label: string;
  visible: boolean;
  type: 'money' | 'percent' | 'number' | 'profit';
  group?: 'Dimension' | 'Basic' | 'Calculated';
}

export interface SavedView {
  id: string;
  name: string;
  dimensions: Dimension[];
  visibleMetrics: string[];
  colorMode?: boolean;
  userId?: string;
  createdAt?: string;
  isDefault?: boolean;
}

export interface UserPermission {
  id: string;
  name: string;
  username: string;
  password?: string;
  email: string;
  role: UserRole;
  keywords: string[];
  showRevenue?: boolean;  // 是否显示收入相关列 (revenue, profit, epa, epc 等)
}
