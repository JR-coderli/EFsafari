/**
 * Data fetching hooks for dashboard data
 * Includes in-memory cache and hierarchy preloading
 */

import { AdRow, DailyBreakdown, Dimension } from '../types';
import dashboardApi from './client';

/**
 * In-memory cache for API responses
 */
class DataCache {
  private cache = new Map<string, { data: any; timestamp: number }>();
  private defaultTTL = 30000; // 30 seconds

  set(key: string, data: any, ttl: number = this.defaultTTL) {
    this.cache.set(key, { data, timestamp: Date.now() + ttl });
  }

  get(key: string): any | null {
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

const dataCache = new DataCache();

/**
 * Generate cache key from parameters
 */
function cacheKey(prefix: string, ...args: any[]): string {
  const keyParts = [prefix, ...args.map(a => JSON.stringify(a))];
  return keyParts.join(':');
}

/**
 * Convert date range string to actual dates
 * For custom ranges, pass start/end dates directly
 * Uses LOCAL date format (not UTC) to match database dates
 */
function getDateRange(range: string, customStart?: Date, customEnd?: Date): { start: string; end: string } {
  // Format date as YYYY-MM-DD using LOCAL time (not UTC)
  const formatDate = (date: Date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };

  // If custom dates are provided, use them
  if (range === 'Custom' && customStart && customEnd) {
    return { start: formatDate(customStart), end: formatDate(customEnd) };
  }

  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);

  switch (range) {
    case 'Today':
      return { start: formatDate(today), end: formatDate(today) };
    case 'Yesterday':
      return { start: formatDate(yesterday), end: formatDate(yesterday) };
    case 'Last 7 Days':
      const sevenDaysAgo = new Date(today);
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
      return { start: formatDate(sevenDaysAgo), end: formatDate(today) };
    case 'Last 14 Days':
      const fourteenDaysAgo = new Date(today);
      fourteenDaysAgo.setDate(fourteenDaysAgo.getDate() - 13);
      return { start: formatDate(fourteenDaysAgo), end: formatDate(today) };
    case 'Last 30 Days':
      const thirtyDaysAgo = new Date(today);
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 29);
      return { start: formatDate(thirtyDaysAgo), end: formatDate(today) };
    default:
      return { start: formatDate(yesterday), end: formatDate(today) };
  }
}

/**
 * Build filters for API from active filters state
 */
function buildApiFilters(activeFilters: Array<{ dimension: Dimension; value: string }>) {
  return activeFilters.map(f => ({
    dimension: f.dimension,
    value: f.value
  }));
}

/**
 * Hierarchy data structure for preloaded data
 */
interface HierarchyNode {
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
  };
  _dimension: Dimension;
  _children?: Record<string, HierarchyNode>;
}

interface HierarchyResponse {
  dimensions: Dimension[];
  hierarchy: Record<string, HierarchyNode>;
  startDate: string;
  endDate: string;
}

/**
 * Load hierarchy data (all levels at once) with caching
 */
export async function loadHierarchy(
  activeDims: Dimension[],
  selectedRange: string,
  customStart?: Date,
  customEnd?: Date
): Promise<HierarchyResponse | null> {
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);
  const cacheKeyVal = cacheKey('hierarchy', start, end, activeDims);

  // Check cache first
  const cached = dataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  try {
    const response = await fetch(`/api/dashboard/hierarchy?start_date=${start}&end_date=${end}&dimensions=${activeDims.join(',')}`, {
      headers: {
        'Authorization': `Bearer ${localStorage.getItem('addata_access_token')}`
      }
    });

    if (!response.ok) {
      // If hierarchy endpoint fails, return null to fall back to regular loading
      console.warn('Hierarchy endpoint failed, falling back to regular loading');
      return null;
    }

    const data = await response.json();
    dataCache.set(cacheKeyVal, data);
    return data;
  } catch (error) {
    console.warn('Hierarchy loading failed:', error);
    return null;
  }
}

/**
 * Convert hierarchy node to AdRow format
 */
function hierarchyNodeToAdRow(
  name: string,
  node: HierarchyNode,
  level: number,
  maxLevel: number,
  parentFilters: Array<{ dimension: Dimension; value: string }>
): AdRow {
  return {
    id: `row_${name}_${level}`,
    name,
    level,
    dimensionType: node._dimension,
    impressions: node._metrics.impressions,
    clicks: node._metrics.clicks,
    conversions: node._metrics.conversions,
    spend: node._metrics.spend,
    revenue: node._metrics.revenue,
    profit: node._metrics.profit,
    m_imp: node._metrics.m_imp,
    m_clicks: node._metrics.m_clicks,
    m_conv: node._metrics.m_conv,
    ctr: node._metrics.ctr,
    cvr: node._metrics.cvr,
    roi: node._metrics.roi,
    cpa: node._metrics.cpa,
    rpa: node._metrics.revenue / (node._metrics.conversions || 1),
    epc: node._metrics.revenue / (node._metrics.clicks || 1),
    epv: node._metrics.revenue / (node._metrics.impressions || 1),
    m_epc: (node._metrics.revenue * 0.4) / (node._metrics.m_clicks || 1),
    m_epv: (node._metrics.revenue * 0.4) / (node._metrics.m_imp || 1),
    m_cpc: (node._metrics.spend * 0.4) / (node._metrics.m_clicks || 1),
    m_cpv: (node._metrics.spend * 0.4) / (node._metrics.m_imp || 1),
    hasChild: level < maxLevel,
    isExpanded: false,
    children: [],
  };
}

/**
 * Get data from hierarchy at a specific level
 */
function getDataFromHierarchy(
  hierarchy: Record<string, HierarchyNode>,
  activeDims: Dimension[],
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  level: number = 0
): AdRow[] {
  // Navigate through hierarchy based on filters
  let currentLevel = hierarchy;

  for (let i = 0; i < activeFilters.length && i < activeDims.length; i++) {
    const filter = activeFilters[i];
    const node = currentLevel[filter.value];
    if (!node || !node._children) {
      return [];
    }
    currentLevel = node._children;
  }

  // Convert current level to AdRow format
  const maxLevel = activeDims.length - 1;
  return Object.entries(currentLevel).map(([name, node]) =>
    hierarchyNodeToAdRow(name, node, level, maxLevel, activeFilters)
  );
}

/**
 * Load root level data from API (with hierarchy fallback)
 */
export async function loadRootData(
  activeDims: Dimension[],
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  selectedRange: string,
  customStart?: Date,
  customEnd?: Date
): Promise<AdRow[]> {
  // Try to load from hierarchy first (faster, one request)
  const hierarchy = await loadHierarchy(activeDims, selectedRange, customStart, customEnd);
  if (hierarchy) {
    return getDataFromHierarchy(hierarchy.hierarchy, activeDims, activeFilters, activeFilters.length);
  }

  // Fallback to regular API
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);
  const currentLevel = activeFilters.length;
  if (currentLevel >= activeDims.length) {
    return [];
  }

  const primaryDim = activeDims[currentLevel];
  const cacheKeyVal = cacheKey('data', start, end, [primaryDim], activeFilters);

  // Check cache
  const cached = dataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  try {
    const response = await dashboardApi.getData({
      startDate: start,
      endDate: end,
      groupBy: [primaryDim],
      filters: activeFilters.map(f => ({ dimension: f.dimension, value: f.value })),
      limit: 1000,
    });

    const result = response.data.map(row => ({
      ...row,
      dimensionType: row.dimensionType as Dimension,
      profit: (row.revenue || 0) - (row.spend || 0),
      hasChild: currentLevel < activeDims.length - 1,
    }));

    dataCache.set(cacheKeyVal, result);
    return result;
  } catch (error) {
    console.error('Error loading data:', error);
    throw error;
  }
}

/**
 * Load child data for a specific row (with hierarchy fallback)
 */
export async function loadChildData(
  activeDims: Dimension[],
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  selectedRange: string,
  rowId: string,
  customStart?: Date,
  customEnd?: Date
): Promise<AdRow[]> {
  // Try hierarchy first
  const hierarchy = await loadHierarchy(activeDims, selectedRange, customStart, customEnd);
  if (hierarchy) {
    return getDataFromHierarchy(hierarchy.hierarchy, activeDims, activeFilters, activeFilters.length);
  }

  // Fallback to regular API
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);
  const currentLevel = activeFilters.length;
  if (currentLevel >= activeDims.length) {
    return [];
  }

  const nextDim = activeDims[currentLevel];
  const cacheKeyVal = cacheKey('data', start, end, [nextDim], activeFilters);

  // Check cache
  const cached = dataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  try {
    const response = await dashboardApi.getData({
      startDate: start,
      endDate: end,
      groupBy: [nextDim],
      filters: activeFilters.map(f => ({ dimension: f.dimension, value: f.value })),
      limit: 1000,
    });

    const result = response.data.map(row => ({
      ...row,
      dimensionType: row.dimensionType as Dimension,
      profit: (row.revenue || 0) - (row.spend || 0),
      hasChild: currentLevel + 1 < activeDims.length,
    }));

    dataCache.set(cacheKeyVal, result);
    return result;
  } catch (error) {
    console.error('Error loading child data:', error);
    throw error;
  }
}

/**
 * Load daily breakdown data for a row (with caching)
 */
export async function loadDailyData(
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  selectedRange: string,
  limit: number = 7,
  customStart?: Date,
  customEnd?: Date
): Promise<DailyBreakdown[]> {
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);
  const cacheKeyVal = cacheKey('daily', start, end, activeFilters, limit);

  // Check cache
  const cached = dataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  try {
    const dailyData = await dashboardApi.getDailyData({
      startDate: start,
      endDate: end,
      filters: activeFilters.map(f => ({ dimension: f.dimension, value: f.value })),
      limit,
    });

    const result = dailyData.map(day => ({
      date: day.date,
      impressions: day.impressions,
      clicks: day.clicks,
      conversions: day.conversions,
      spend: day.spend,
      revenue: day.revenue,
      profit: (day.revenue || 0) - (day.spend || 0),
      m_imp: day.m_imp,
      m_clicks: day.m_clicks,
      m_conv: day.m_conv,
    }));

    dataCache.set(cacheKeyVal, result, 15000); // 15 seconds cache for daily data
    return result;
  } catch (error) {
    console.error('Error loading daily data:', error);
    return [];
  }
}

/**
 * Clear all data cache (call when date range changes)
 */
export function clearDataCache() {
  dataCache.clear();
}

/**
 * Get API health status
 */
export async function checkApiHealth(): Promise<boolean> {
  try {
    await dashboardApi.health();
    return true;
  } catch {
    return false;
  }
}
