
export type UserRole = 'admin' | 'ops' | 'business';

export type Dimension = 
  | 'platform' 
  | 'advertiser'
  | 'offer'
  | 'campaign_name' 
  | 'sub_campaign_name' 
  | 'creative_name';

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
  epc: number;
  epv: number;
  m_epc: number;
  m_epv: number;
  m_cpc: number;
  m_cpv: number;
  
  hasChild: boolean;
  isExpanded?: boolean;
  children?: AdRow[];
  dailyData?: DailyBreakdown[];
  campaign_id?: string;
  adset_id?: string;
  ads_id?: string;
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
  userId?: string;
  createdAt?: string;
}

export interface UserPermission {
  id: string;
  name: string;
  username: string;
  password?: string;
  email: string;
  role: UserRole;
  keywords: string[];
}
