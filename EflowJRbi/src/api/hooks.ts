/**
 * Data fetching hooks for dashboard data
 */

import { AdRow, DailyBreakdown, Dimension } from '../types';
import dashboardApi from './client';

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
 * Load root level data from API
 */
export async function loadRootData(
  activeDims: Dimension[],
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  selectedRange: string,
  customStart?: Date,
  customEnd?: Date
): Promise<AdRow[]> {
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);

  // Get the primary dimension for current level
  const currentLevel = activeFilters.length;
  if (currentLevel >= activeDims.length) {
    return [];
  }

  const primaryDim = activeDims[currentLevel];

  try {
    const response = await dashboardApi.getData({
      startDate: start,
      endDate: end,
      groupBy: [primaryDim],
      filters: buildApiFilters(activeFilters),
      limit: 1000,
    });

    return response.data.map(row => ({
      ...row,
      dimensionType: row.dimensionType as Dimension,
      profit: (row.revenue || 0) - (row.spend || 0),
      hasChild: currentLevel < activeDims.length - 1,
    }));
  } catch (error) {
    console.error('Error loading data:', error);
    throw error;
  }
}

/**
 * Load child data for a specific row
 */
export async function loadChildData(
  activeDims: Dimension[],
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  selectedRange: string,
  rowId: string,
  customStart?: Date,
  customEnd?: Date
): Promise<AdRow[]> {
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);

  // Get the next dimension (currentLevel is the index of next dimension to query)
  const currentLevel = activeFilters.length;
  if (currentLevel >= activeDims.length) {
    return [];
  }

  const nextDim = activeDims[currentLevel];

  try {
    const response = await dashboardApi.getData({
      startDate: start,
      endDate: end,
      groupBy: [nextDim],
      filters: buildApiFilters(activeFilters),
      limit: 1000,
    });

    return response.data.map(row => ({
      ...row,
      dimensionType: row.dimensionType as Dimension,
      profit: (row.revenue || 0) - (row.spend || 0),
      hasChild: currentLevel + 1 < activeDims.length,
    }));
  } catch (error) {
    console.error('Error loading child data:', error);
    throw error;
  }
}

/**
 * Load daily breakdown data for a row
 */
export async function loadDailyData(
  activeFilters: Array<{ dimension: Dimension; value: string }>,
  selectedRange: string,
  limit: number = 7,
  customStart?: Date,
  customEnd?: Date
): Promise<DailyBreakdown[]> {
  const { start, end } = getDateRange(selectedRange, customStart, customEnd);

  try {
    const dailyData = await dashboardApi.getDailyData({
      startDate: start,
      endDate: end,
      filters: buildApiFilters(activeFilters),
      limit,
    });

    return dailyData.map(day => ({
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
  } catch (error) {
    console.error('Error loading daily data:', error);
    return [];
  }
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
