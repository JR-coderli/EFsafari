import { useState, useCallback, useEffect, useRef } from 'react';
import type { MouseEvent } from 'react';
import { AdRow, Dimension, DailyBreakdown } from '../types';
import { loadRootData as apiLoadRootData, loadChildData, loadDailyData as apiLoadDailyData, clearDataCache } from '../src/api/hooks';
import { generateMockReport } from '../mockData';

export interface Filter {
  dimension: Dimension;
  value: string;
}

export interface UseDashboardDataOptions {
  activeDims: Dimension[];
  activeFilters: Filter[];
  selectedRange: string;
  customDateStart?: Date;
  customDateEnd?: Date;
  currentPage: string;
  isInitialized: boolean;
  useMock: boolean;
  currentUser: { id: string; keywords?: string[] };
}

export const useDashboardData = (options: UseDashboardDataOptions) => {
  const {
    activeDims,
    activeFilters,
    selectedRange,
    customDateStart,
    customDateEnd,
    currentPage,
    isInitialized,
    useMock,
    currentUser
  } = options;

  const [data, setData] = useState<AdRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedDimRows, setExpandedDimRows] = useState<Set<string>>(new Set());
  const [loadingDimRows, setLoadingDimRows] = useState<Set<string>>(new Set());
  const [expandedDailyRows, setExpandedDailyRows] = useState<Set<string>>(new Set());
  const [loadingDailyRows, setLoadingDailyRows] = useState<Set<string>>(new Set());
  const [dailyDataMap, setDailyDataMap] = useState<Map<string, DailyBreakdown[]>>(new Map());

  const loadRootData = useCallback(async () => {
    if (currentPage !== 'performance') return;
    if (!isInitialized) return;
    if (activeDims.length === 0) return;

    setLoading(true);
    setError(null);
    setExpandedDailyRows(new Set());
    setExpandedDimRows(new Set());

    try {
      const currentLevel = activeFilters.length;
      if (currentLevel >= activeDims.length) {
        setData([]);
        setLoading(false);
        return;
      }

      let rawData: AdRow[];

      if (useMock) {
        await new Promise(resolve => setTimeout(resolve, 300));
        rawData = generateMockReport(
          activeDims[currentLevel],
          currentLevel,
          activeFilters.map(f => f.value).join('|'),
          activeDims.slice(currentLevel + 1),
          selectedRange
        );
      } else {
        rawData = await apiLoadRootData(activeDims, activeFilters, selectedRange, customDateStart, customDateEnd);
      }

      setData(rawData);
    } catch (err) {
      console.error('Error loading data:', err);
      const errorMsg = err instanceof Error ? err.message : 'Failed to load data';

      if (useMock) {
        setError(errorMsg);
        setLoading(false);
        return;
      }

      if (errorMsg.includes('Not authenticated') || errorMsg.includes('401')) {
        setError('Please login to access data');
        setLoading(false);
        return;
      }

      setError(errorMsg);
      setLoading(false);
      return;
    } finally {
      if (!useMock) {
        setLoading(false);
      }
    }
  }, [activeDims, activeFilters, selectedRange, customDateStart, customDateEnd, currentUser, useMock, currentPage, isInitialized]);

  const handleRefreshData = useCallback(async () => {
    if (currentPage !== 'performance' || activeDims.length === 0) return;

    setIsRefreshing(true);
    setError(null);

    const startTime = Date.now();

    try {
      const currentLevel = activeFilters.length;
      if (currentLevel >= activeDims.length) {
        setData([]);
        setIsRefreshing(false);
        return;
      }

      const rawData = await apiLoadRootData(activeDims, activeFilters, selectedRange, customDateStart, customDateEnd);
      setData(rawData);
    } catch (err) {
      console.error('Error refreshing data:', err);
      const errorMsg = err instanceof Error ? err.message : 'Failed to refresh data';
      setError(errorMsg);
    } finally {
      const elapsed = Date.now() - startTime;
      const minDelay = Math.max(0, 500 - elapsed);
      setTimeout(() => setIsRefreshing(false), minDelay);
    }
  }, [activeDims, activeFilters, selectedRange, customDateStart, customDateEnd, currentPage]);

  const toggleDimExpansion = async (e: MouseEvent, row: AdRow) => {
    e.preventDefault();
    e.stopPropagation();
    if (!row.hasChild) {
      return;
    }
    const nextExpanded = new Set(expandedDimRows);
    if (nextExpanded.has(row.id)) {
      nextExpanded.delete(row.id);
      setExpandedDimRows(nextExpanded);
    } else {
      nextExpanded.add(row.id);
      setExpandedDimRows(nextExpanded);
      if (!row.children || row.children.length === 0) {
        setLoadingDimRows(prev => new Set(prev).add(row.id));

        const nextLevel = row.level + 1;
        let children: AdRow[];

        const rowFilters = row.filterPath || row.id.split('|').map((v, i) => ({
          dimension: activeDims[i],
          value: v
        }));

        if (useMock) {
          children = generateMockReport(
            activeDims[nextLevel],
            nextLevel,
            row.id,
            activeDims.slice(nextLevel + 1),
            selectedRange
          );
          setLoadingDimRows(prev => {
            const nextLoading = new Set(prev);
            nextLoading.delete(row.id);
            return nextLoading;
          });
        } else {
          try {
            children = await loadChildData(activeDims, rowFilters, selectedRange, row.id, customDateStart, customDateEnd);
          } catch (err) {
            console.error('Error loading child data:', err);
            children = [];
          } finally {
            setLoadingDimRows(prev => {
              const nextLoading = new Set(prev);
              nextLoading.delete(row.id);
              return nextLoading;
            });
          }
        }

        if (currentUser.keywords && currentUser.keywords.length > 0) {
          children = children.filter(c => {
            if (c.dimensionType === 'sub_campaign_name') {
              return currentUser.keywords.some(kw =>
                c.name.toLowerCase().includes(kw.toLowerCase())
              );
            }
            return true;
          });
        }

        setData(prev => {
          const update = (rows: AdRow[], depth = 0): AdRow[] => {
            return rows.map(r => {
              const match = r.id === row.id;
              return match
                ? { ...r, children }
                : (r.children ? { ...r, children: update(r.children, depth + 1) } : r);
            });
          };
          const updated = update(prev);
          return updated;
        });
      }
    }
  };

  const toggleDailyBreakdown = async (e: MouseEvent, row: AdRow) => {
    e.preventDefault();
    e.stopPropagation();
    const next = new Set(expandedDailyRows);
    if (next.has(row.id)) {
      next.delete(row.id);
      setExpandedDailyRows(next);
    } else {
      next.add(row.id);
      setExpandedDailyRows(next);
      if (!dailyDataMap.has(row.id) && row.filterPath) {
        setLoadingDailyRows(prev => new Set(prev).add(row.id));
        apiLoadDailyData(row.filterPath, 'Last 7 Days', 7).then(dailyData => {
          setDailyDataMap(prev => new Map(prev).set(row.id, dailyData));
        }).catch(err => {
          console.error('Error loading daily data:', err);
        }).finally(() => {
          setLoadingDailyRows(prev => {
            const nextLoading = new Set(prev);
            nextLoading.delete(row.id);
            return nextLoading;
          });
        });
      }
    }
  };

  return {
    data,
    setData,
    loading,
    isRefreshing,
    error,
    setError,
    setUseMock: (useMock: boolean) => { /* Handled in parent */ },
    expandedDimRows,
    loadingDimRows,
    expandedDailyRows,
    loadingDailyRows,
    dailyDataMap,
    loadRootData,
    handleRefreshData,
    toggleDimExpansion,
    toggleDailyBreakdown,
    clearDataCache
  };
};
