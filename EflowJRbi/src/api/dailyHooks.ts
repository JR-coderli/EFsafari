/**
 * Data fetching hooks for Daily Report
 * Supports media -> date hierarchy structure
 */

import { dailyReportApi } from './client';
import type { AdRow } from '../types';

/**
 * In-memory cache for Daily Report API responses
 * Cache is disabled in development mode, enabled in production
 */
class DailyDataCache {
  private cache = new Map<string, { data: any; timestamp: number }>();
  private defaultTTL = 5 * 60 * 1000; // 5 minutes

  // Check if we're in development mode - caching disabled
  private isDev = import.meta.env.DEV;

  set(key: string, data: any, ttl: number = this.defaultTTL) {
    if (this.isDev) return;
    this.cache.set(key, { data, timestamp: Date.now() + ttl });
  }

  get(key: string): any | null {
    if (this.isDev) return null;
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.timestamp) {
      this.cache.delete(key);
      return null;
    }
    return entry.data;
  }

  clear() {
    this.cache.clear();
  }

  clearPattern(pattern: string) {
    for (const key of this.cache.keys()) {
      if (key.includes(pattern)) {
        this.cache.delete(key);
      }
    }
  }
}

const dailyDataCache = new DailyDataCache();

/**
 * Generate cache key from parameters
 */
function cacheKey(prefix: string, ...args: any[]): string {
  return args.length > 0
    ? `daily:${prefix}:${args.map(a => JSON.stringify(a)).join(':')}`
    : `daily:${prefix}`;
}

/**
 * Hierarchy data structure for Daily Report
 */
interface DailyHierarchyNode {
  _dimension: string;
  _metrics: {
    impressions: number;
    clicks: number;
    conversions: number;
    spend: number;
    revenue: number;
    profit: number;
    m_imp: number;
    m_clicks: number;
    m_conv: number;
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
    m_cpa: number;
    m_epa: number;
  };
  _children?: Record<string, DailyHierarchyNode>;
}

interface DailyHierarchyResponse {
  dimensions: string[];
  hierarchy: Record<string, DailyHierarchyNode>;
  startDate: string;
  endDate: string;
}

/**
 * Load hierarchy data for Daily Report (media -> date structure)
 */
export async function loadDailyHierarchy(
  startDate: string,
  endDate: string
): Promise<DailyHierarchyResponse | null> {
  const cacheKeyVal = cacheKey('hierarchy', startDate, endDate);

  // Check cache first
  const cached = dailyDataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  try {
    const response = await dailyReportApi.getHierarchy({ startDate, endDate });
    dailyDataCache.set(cacheKeyVal, response);
    return response;
  } catch (error) {
    console.warn('Daily Report hierarchy loading failed:', error);
    return null;
  }
}

/**
 * Convert hierarchy node to AdRow format
 */
function hierarchyNodeToAdRow(
  name: string,
  node: DailyHierarchyNode,
  level: number,
  maxLevel: number,
  parentFilters: Array<{ dimension: string; value: string }>
): AdRow {
  const metrics = node._metrics;

  // Build unique ID using filterPath values (hierarchical path)
  const filterPath = [...parentFilters, { dimension: node._dimension, value: name }];
  const uniqueId = filterPath.map(f => f.value).join('|');

  return {
    id: uniqueId,
    name,
    level,
    dimensionType: node._dimension as any,
    impressions: metrics.impressions,
    clicks: metrics.clicks,
    conversions: metrics.conversions,
    spend: metrics.spend,
    revenue: metrics.revenue,
    profit: metrics.profit,
    m_imp: metrics.m_imp,
    m_clicks: metrics.m_clicks,
    m_conv: metrics.m_conv,
    ctr: metrics.ctr,
    cvr: metrics.cvr,
    roi: metrics.roi,
    cpa: metrics.cpa,
    rpa: metrics.rpa,
    epa: metrics.rpa,  // Earnings Per Action
    epc: metrics.epc,
    epv: metrics.epv,
    m_epc: metrics.m_epc,
    m_epv: metrics.m_epv,
    m_cpc: metrics.m_cpc,
    m_cpv: metrics.m_cpv,
    m_cpa: metrics.spend / (metrics.m_conv || 1),
    m_epa: metrics.revenue / (metrics.m_conv || 1),
    hasChild: level < maxLevel,
    isExpanded: false,
    children: [],
    filterPath,
  };
}

/**
 * Get data from hierarchy at a specific level
 */
function getDataFromHierarchy(
  hierarchy: Record<string, DailyHierarchyNode>,
  activeFilters: Array<{ dimension: string; value: string }>,
  startLevel: number = 0
): AdRow[] {
  // Navigate through hierarchy based on filters
  let currentLevel = hierarchy;
  let actualLevel = startLevel;

  for (let i = 0; i < activeFilters.length; i++) {
    const filter = activeFilters[i];
    const node = currentLevel[filter.value] as DailyHierarchyNode;
    if (!node || !node._children) {
      return [];
    }
    currentLevel = node._children as Record<string, DailyHierarchyNode>;
    actualLevel++; // Increment level as we go deeper
  }

  // Convert current level to AdRow format
  const maxLevel = 1; // Daily Report has 2 levels: media (0) -> date (1)
  const result = Object.entries(currentLevel).map(([name, node]) =>
    hierarchyNodeToAdRow(name, node as DailyHierarchyNode, actualLevel, maxLevel, activeFilters)
  );
  return result;
}

/**
 * Load root level data (Media level) from API
 */
export async function loadDailyRootData(
  startDate: string,
  endDate: string
): Promise<AdRow[]> {
  const cacheKeyVal = cacheKey('data', startDate, endDate);

  // Check cache first
  const cached = dailyDataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  // Check if hierarchy is already cached
  const hierarchyCacheKey = cacheKey('hierarchy', startDate, endDate);
  const cachedHierarchy = dailyDataCache.get(hierarchyCacheKey);
  if (cachedHierarchy) {
    const result = getDataFromHierarchy(cachedHierarchy.hierarchy, [], 0);
    dailyDataCache.set(cacheKeyVal, result);
    return result;
  }

  // Load Media level data directly
  try {
    const response = await dailyReportApi.getData({ startDate, endDate });

    const result = response.map(row => {
      return {
        ...row,
        dimensionType: row.dimensionType as any,
        profit: row.revenue - row.spend,
        epa: row.rpa,
        isExpanded: false,
        children: [],
      } as AdRow;
    });

    dailyDataCache.set(cacheKeyVal, result);

    // Trigger background hierarchy preloading
    preloadDailyHierarchyInBackground(startDate, endDate);

    return result;
  } catch (error) {
    console.error('Error loading Daily Report data:', error);
    throw error;
  }
}

/**
 * Load child data for a specific Media (Date level)
 */
export async function loadDailyChildData(
  media: string,
  startDate: string,
  endDate: string
): Promise<AdRow[]> {
  // Try hierarchy first
  const hierarchy = await loadDailyHierarchy(startDate, endDate);
  if (hierarchy) {
    return getDataFromHierarchy(hierarchy.hierarchy, [{ dimension: 'media', value: media }], 0);
  }

  // Fallback to regular API
  const cacheKeyVal = cacheKey('data', startDate, endDate, media);

  // Check cache
  const cached = dailyDataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  try {
    const response = await dailyReportApi.getData({ startDate, endDate, media });

    const result = response.map(row => {
      return {
        ...row,
        dimensionType: row.dimensionType as any,
        profit: row.revenue - row.spend,
        epa: row.rpa,
        isExpanded: false,
        children: [],
      } as AdRow;
    });

    dailyDataCache.set(cacheKeyVal, result);
    return result;
  } catch (error) {
    console.error('Error loading Daily Report child data:', error);
    throw error;
  }
}

/**
 * Preload hierarchy in background without blocking UI
 */
function preloadDailyHierarchyInBackground(
  startDate: string,
  endDate: string
) {
  const preloadFn = async () => {
    try {
      await loadDailyHierarchy(startDate, endDate);
    } catch (error) {
      // Silent fail - this is just optimization
    }
  };

  // Use setTimeout(0) to run after current render
  if (typeof requestIdleCallback !== 'undefined') {
    requestIdleCallback(() => preloadFn());
  } else {
    setTimeout(preloadFn, 0);
  }
}

/**
 * Update spend for a specific date and media
 */
export async function updateDailySpend(
  date: string,
  media: string,
  spendValue: number
): Promise<{ success: boolean; message: string }> {
  try {
    const result = await dailyReportApi.updateSpend({
      date,
      media,
      spend_value: spendValue,
    });

    // Clear relevant caches
    dailyDataCache.clearPattern('data');
    dailyDataCache.clearPattern('hierarchy');

    return result;
  } catch (error) {
    console.error('Error updating spend:', error);
    throw error;
  }
}

/**
 * Get summary data for Daily Report
 */
export async function loadDailySummary(
  startDate: string,
  endDate: string,
  media?: string
): Promise<{
  impressions: number;
  clicks: number;
  conversions: number;
  spend: number;
  revenue: number;
  profit: number;
  m_imp: number;
  m_clicks: number;
  m_conv: number;
  ctr: number;
  cvr: number;
  roi: number;
  cpa: number;
  rpa: number;
}> {
  try {
    const response = await dailyReportApi.getSummary({ startDate, endDate, media });

    return {
      impressions: response.impressions,
      clicks: response.clicks,
      conversions: response.conversions,
      spend: response.spend,
      revenue: response.revenue,
      profit: response.revenue - response.spend,
      m_imp: response.m_imp,
      m_clicks: response.m_clicks,
      m_conv: response.m_conv,
      ctr: response.ctr,
      cvr: response.cvr,
      roi: response.roi,
      cpa: response.cpa,
      rpa: response.rpa || (response.revenue / (response.conversions || 1)),
    };
  } catch (error) {
    console.error('Error loading Daily Report summary:', error);
    throw error;
  }
}

/**
 * Clear all Daily Report data cache
 */
export function clearDailyDataCache() {
  dailyDataCache.clear();
}
