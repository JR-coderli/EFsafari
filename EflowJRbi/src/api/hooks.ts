/**
 * Data fetching hooks for dashboard data
 * Includes in-memory cache and hierarchy preloading
 */

import { AdRow, DailyBreakdown, Dimension } from '../types';
import dashboardApi from './client';

/**
 * In-memory cache for API responses
 * Cache is disabled in development mode, enabled in production
 */
class DataCache {
  private cache = new Map<string, { data: any; timestamp: number }>();
  private defaultTTL = 5 * 60 * 1000; // 5 minutes

  // Check if we're in development mode - caching disabled
  private isDev = import.meta.env.DEV;

  set(key: string, data: any, ttl: number = this.defaultTTL) {
    // Skip caching in development mode
    if (this.isDev) return;
    this.cache.set(key, { data, timestamp: Date.now() + ttl });
  }

  get(key: string): any | null {
    // Skip cache in development mode
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
  const revenue = Number(node._metrics.revenue) || 0;
  const spend = Number(node._metrics.spend) || 0;
  const impressions = Number(node._metrics.impressions) || 0;
  const clicks = Number(node._metrics.clicks) || 0;
  const conversions = Number(node._metrics.conversions) || 0;
  const m_imp = Number(node._metrics.m_imp) || 0;
  const m_clicks = Number(node._metrics.m_clicks) || 0;
  const m_conv = Number(node._metrics.m_conv) || 0;

  return {
    id: `row_${name}_${level}`,
    name,
    level,
    dimensionType: node._dimension,
    impressions,
    clicks,
    conversions,
    spend,
    revenue,
    profit: revenue - spend,
    m_imp,
    m_clicks,
    m_conv,
    ctr: Number(node._metrics.ctr) || 0,
    cvr: Number(node._metrics.cvr) || 0,
    roi: Number(node._metrics.roi) || 0,
    cpa: Number(node._metrics.cpa) || 0,
    rpa: revenue / (conversions || 1),
    epa: revenue / (conversions || 1),  // Earnings Per Action
    epc: revenue / (clicks || 1),
    epv: revenue / (impressions || 1),
    m_epc: (revenue * 0.4) / (m_clicks || 1),
    m_epv: (revenue * 0.4) / (m_imp || 1),
    m_cpc: (spend * 0.4) / (m_clicks || 1),
    m_cpv: (spend * 0.4) / (m_imp || 1),
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
  const result = Object.entries(currentLevel).map(([name, node]) =>
    hierarchyNodeToAdRow(name, node, level, maxLevel, activeFilters)
  );
  return result;
}

/**
 * Load root level data from API (optimized for first screen)
 * Strategy:
 * 1. Check hierarchy cache - if available, return immediately
 * 2. Otherwise, load and return first level data immediately
 * 3. Trigger background hierarchy preloading for future interactions
 */
export async function loadRootData(
  activeDims: Dimension[],
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  selectedRange: string,
  customStart?: Date,
  customEnd?: Date
): Promise<AdRow[]> {
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);
  const currentLevel = activeFilters.length;
  if (currentLevel >= activeDims.length) {
    return [];
  }

  const primaryDim = activeDims[currentLevel];
  const cacheKeyVal = cacheKey('data', start, end, [primaryDim], activeFilters);

  // Check cache first (could be from previous hierarchy preload)
  const cached = dataCache.get(cacheKeyVal);
  if (cached) {
    return cached;
  }

  // Check if hierarchy is already cached
  const hierarchyCacheKey = cacheKey('hierarchy', start, end, activeDims);
  const cachedHierarchy = dataCache.get(hierarchyCacheKey);
  if (cachedHierarchy) {
    const result = getDataFromHierarchy(cachedHierarchy.hierarchy, activeDims, activeFilters, activeFilters.length);
    // Also cache this level for faster access
    dataCache.set(cacheKeyVal, result);
    return result;
  }

  // Load first level data immediately (fastest first screen)
  try {
    const response = await dashboardApi.getData({
      startDate: start,
      endDate: end,
      groupBy: [primaryDim],
      filters: activeFilters.map(f => ({ dimension: f.dimension, value: f.value })),
      limit: 1000,
    });

    const result = response.data.map(row => {
      const revenue = Number(row.revenue) || 0;
      const spend = Number(row.spend) || 0;
      const impressions = Number(row.impressions) || 0;
      const clicks = Number(row.clicks) || 0;
      const conversions = Number(row.conversions) || 0;
      const m_imp = Number(row.m_imp) || 0;
      const m_clicks = Number(row.m_clicks) || 0;
      const m_conv = Number(row.m_conv) || 0;

      return {
        ...row,
        dimensionType: row.dimensionType as Dimension,
        impressions,
        clicks,
        conversions,
        spend,
        revenue,
        profit: revenue - spend,
        m_imp,
        m_clicks,
        m_conv,
        ctr: Number(row.ctr) || 0,
        cvr: Number(row.cvr) || 0,
        roi: Number(row.roi) || 0,
        cpa: Number(row.cpa) || 0,
        rpa: Number(row.rpa) || 0,
        epa: Number(row.rpa) || 0,  // Earnings Per Action (same as rpa)
        epc: Number(row.epc) || 0,
        epv: Number(row.epv) || 0,
        m_epc: Number(row.m_epc) || 0,
        m_epv: Number(row.m_epv) || 0,
        m_cpc: Number(row.m_cpc) || 0,
        m_cpv: Number(row.m_cpv) || 0,
        hasChild: currentLevel < activeDims.length - 1,
      };
    });

    dataCache.set(cacheKeyVal, result);

    // Trigger background hierarchy preloading (don't wait for it)
    preloadHierarchyInBackground(activeDims, selectedRange, customStart, customEnd);

    return result;
  } catch (error) {
    console.error('Error loading data:', error);
    throw error;
  }
}

/**
 * Preload hierarchy in background without blocking UI
 */
function preloadHierarchyInBackground(
  activeDims: Dimension[],
  selectedRange: string,
  customStart?: Date,
  customEnd?: Date
) {
  // Use requestIdleCallback or setTimeout to not block the main thread
  const preloadFn = async () => {
    try {
      await loadHierarchy(activeDims, selectedRange, customStart, customEnd);
    } catch (error) {
      // Silent fail - this is just optimization
      console.debug('Background hierarchy preload failed:', error);
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

    const result = response.data.map(row => {
      const revenue = Number(row.revenue) || 0;
      const spend = Number(row.spend) || 0;
      const impressions = Number(row.impressions) || 0;
      const clicks = Number(row.clicks) || 0;
      const conversions = Number(row.conversions) || 0;
      const m_imp = Number(row.m_imp) || 0;
      const m_clicks = Number(row.m_clicks) || 0;
      const m_conv = Number(row.m_conv) || 0;

      return {
        ...row,
        dimensionType: row.dimensionType as Dimension,
        impressions,
        clicks,
        conversions,
        spend,
        revenue,
        profit: revenue - spend,
        m_imp,
        m_clicks,
        m_conv,
        ctr: Number(row.ctr) || 0,
        cvr: Number(row.cvr) || 0,
        roi: Number(row.roi) || 0,
        cpa: Number(row.cpa) || 0,
        rpa: Number(row.rpa) || 0,
        epa: Number(row.rpa) || 0,  // Earnings Per Action (same as rpa)
        epc: Number(row.epc) || 0,
        epv: Number(row.epv) || 0,
        m_epc: Number(row.m_epc) || 0,
        m_epv: Number(row.m_epv) || 0,
        m_cpc: Number(row.m_cpc) || 0,
        m_cpv: Number(row.m_cpv) || 0,
        hasChild: currentLevel + 1 < activeDims.length,
      };
    });

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

    const result = dailyData.map(day => {
      const impressions = Number(day.impressions) || 0;
      const clicks = Number(day.clicks) || 0;
      const conversions = Number(day.conversions) || 0;
      const spend = Number(day.spend) || 0;
      const revenue = Number(day.revenue) || 0;
      const m_imp = Number(day.m_imp) || 0;
      const m_clicks = Number(day.m_clicks) || 0;
      const m_conv = Number(day.m_conv) || 0;

      return {
        date: day.date,
        impressions,
        clicks,
        conversions,
        spend,
        revenue,
        profit: revenue - spend,
        m_imp,
        m_clicks,
        m_conv,
        // Calculated metrics
        ctr: clicks / (impressions || 1),
        cvr: conversions / (clicks || 1),
        roi: (revenue - spend) / (spend || 1),
        cpa: spend / (conversions || 1),
        epa: revenue / (conversions || 1),  // Earnings Per Action
        epc: revenue / (clicks || 1),       // Earnings Per Click
        epv: revenue / (impressions || 1),  // Earnings Per View
        m_epc: revenue / (m_clicks || 1),
        m_epv: revenue / (m_imp || 1),
        m_cpc: spend / (m_clicks || 1),
        m_cpv: spend / (m_imp || 1),
      };
    });

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
