import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { createPortal } from 'react-dom';
import { AdRow, Dimension, MetricConfig, SavedView, UserPermission, DailyBreakdown } from './types';
import { generateMockReport } from './mockData';
import { loadRootData as apiLoadRootData, loadChildData, loadDailyData as apiLoadDailyData, clearDataCache } from './src/api/hooks';
import { authApi, usersApi, tokenManager } from './src/api/auth';
import { dailyReportApi, dashboardApi, offersApi, onConnectionStatusChange, type ConnectionStatus } from './src/api/client';
import { viewsApi } from './src/api/views';
import { getPageFromHash, navigateTo, setHashForPage, type PageType } from './src/utils/router';

// Import extracted components and hooks
import { ALL_DIMENSIONS, DEFAULT_METRICS } from './constants';
import MetricValue from './components/MetricValue';
import DatePicker from './components/DatePicker';
import LoginPage from './pages/LoginPage';
import PermissionsPage from './pages/PermissionsPage';
import DailyReport from './components/DailyReport';
import HourlyReport from './components/HourlyReport';
import Config from './components/Config';
import { useContextMenu } from './hooks/useContextMenu';
import { useColumnResize } from './hooks/useColumnResize';

interface Filter {
  dimension: Dimension;
  value: string;
}

interface ETLStatus {
  last_update: string | null;
  report_date: string | null;
  all_success: boolean;
}

const App: React.FC = () => {
  const [user, setUser] = useState<UserPermission | null>(null);
  const handleLogout = () => {
    authApi.logout();
    setUser(null);
  };

  if (!user) return <LoginPage onLogin={setUser} />;

  return <Dashboard key={user.id} currentUser={user} onLogout={handleLogout} />;
};

const Dashboard: React.FC<{ currentUser: UserPermission; onLogout: () => void }> = ({ currentUser, onLogout }) => {
  const [loading, setLoading] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [data, setData] = useState<AdRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [useMock, setUseMock] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('connected');
  const [activeDims, setActiveDims] = useState<Dimension[]>([]);
  const [editingDimIndex, setEditingDimIndex] = useState<number | null>(null);
  const [dropdownPosition, setDropdownPosition] = useState<{ top: number; left: number } | null>(null);
  const [metrics, setMetrics] = useState<MetricConfig[]>(DEFAULT_METRICS);
  const [currentPage, setCurrentPage] = useState<PageType>('performance');
  const [performanceSubPage, setPerformanceSubPage] = useState<'dates' | 'hourly'>('dates');
  const [performanceMenuOpen, setPerformanceMenuOpen] = useState(true);
  const [activeFilters, setActiveFilters] = useState<Filter[]>([]);
  const [expandedDailyRows, setExpandedDailyRows] = useState<Set<string>>(new Set());
  const [loadingDailyRows, setLoadingDailyRows] = useState<Set<string>>(new Set());
  const [loadingDimRows, setLoadingDimRows] = useState<Set<string>>(new Set());
  const [expandedDimRows, setExpandedDimRows] = useState<Set<string>>(new Set());
  const [dailyDataMap, setDailyDataMap] = useState<Map<string, DailyBreakdown[]>>(new Map());
  const [selectedRange, setSelectedRange] = useState('Yesterday');
  const [customDateStart, setCustomDateStart] = useState<Date | undefined>(undefined);
  const [customDateEnd, setCustomDateEnd] = useState<Date | undefined>(undefined);
  const [pendingRangeRefresh, setPendingRangeRefresh] = useState(false);
  const [hourlyRange, setHourlyRange] = useState('Today');
  const [hourlyDateStart, setHourlyDateStart] = useState<Date | undefined>(undefined);
  const [hourlyDateEnd, setHourlyDateEnd] = useState<Date | undefined>(undefined);
  const [dailyReportRange, setDailyReportRange] = useState('This Month');
  const [dailyReportStart, setDailyReportStart] = useState<Date | undefined>(undefined);
  const [dailyReportEnd, setDailyReportEnd] = useState<Date | undefined>(undefined);
  const [quickFilterText, setQuickFilterText] = useState('');
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [showColumnEditor, setShowColumnEditor] = useState(false);
  const [showViewList, setShowViewList] = useState(false);
  const viewsDropdownRef = useRef<HTMLDivElement>(null);
  const tableRef = useRef<HTMLTableElement>(null);

  const { contextMenu, contextMenuRef, handleContextMenu, handleCopyText, closeContextMenu } = useContextMenu();
  const { columnWidths, setColumnWidths, resizingColumn, handleResizeStart } = useColumnResize({
    hierarchy: 300,
    ...Object.fromEntries(DEFAULT_METRICS.map(m => [m.key, 120]))
  });

  // State for offer details
  const [offerDetailsMap, setOfferDetailsMap] = useState<Record<string, {
    url: string;
    notes: string;
    name: string;
  }>>({});
  const [copiedOfferUrl, setCopiedOfferUrl] = useState<string | null>(null);
  const [notesTooltip, setNotesTooltip] = useState<{ content: string; x: number; y: number } | null>(null);
  const notesTooltipTimerRef = useRef<NodeJS.Timeout | null>(null);
  const [notesModal, setNotesModal] = useState<{ offerName: string; content: string } | null>(null);

  const [paginationPage, setPaginationPage] = useState(1);
  const [rowsPerPage] = useState(20);
  type SortColumn = 'revenue' | 'spend' | 'impressions' | 'clicks' | 'conversions' | 'ctr' | 'cvr' | 'roi' | 'cpa' | 'rpa' | 'epc' | 'epv' | 'm_epc' | 'm_epv' | 'm_cpc' | 'm_cpv' | 'm_cpa' | 'm_epa' | 'm_imp' | 'm_clicks' | 'm_conv';
  type SortOrder = 'asc' | 'desc' | null;
  const [sortColumn, setSortColumn] = useState<SortColumn>('revenue');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [hideZeroImpressions, setHideZeroImpressions] = useState(true);
  const [colorMode, setColorMode] = useState(false);
  const [etlStatus, setEtlStatus] = useState<ETLStatus | null>(null);
  const [isInitialized, setIsInitialized] = useState(false);
  const hasLoadedDataAfterInit = useRef(false);

  // Saved views state
  const getUserStorageKey = (userId: string) => `ad_tech_saved_views_user_${userId}`;
  const STORAGE_KEY = getUserStorageKey(currentUser.id);
  const [savedViews, setSavedViews] = useState<SavedView[]>(() => {
    try {
      const userKey = getUserStorageKey(currentUser.id);
      const saved = localStorage.getItem(userKey);
      return saved ? JSON.parse(saved) : [];
    } catch (e) {
      return [];
    }
  });

  // Cleanup tooltip timer
  useEffect(() => {
    return () => {
      if (notesTooltipTimerRef.current) {
        clearTimeout(notesTooltipTimerRef.current);
      }
    };
  }, []);

  // Hash routing
  useEffect(() => {
    const page = getPageFromHash();
    setCurrentPage(page);
    if (page === 'performance') {
      setPerformanceSubPage('dates');
    } else if (page === 'hourly') {
      setPerformanceSubPage('hourly');
    }
  }, []);

  useEffect(() => {
    const handleHashChange = () => {
      const page = getPageFromHash();
      setCurrentPage(page);
      if (page === 'performance') {
        setPerformanceSubPage('dates');
      } else if (page === 'hourly') {
        setPerformanceSubPage('hourly');
      }
    };

    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  useEffect(() => {
    setHashForPage(currentPage);
  }, [currentPage]);

  // Date display string
  const dateDisplayString = useMemo(() => {
    const getRangeInfo = (range: string, start?: Date, end?: Date) => {
      const today = new Date();
      const yesterday = new Date(today);
      yesterday.setDate(yesterday.getDate() - 1);
      const sevenDaysAgo = new Date(today);
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
      const fourteenDaysAgo = new Date(today);
      fourteenDaysAgo.setDate(fourteenDaysAgo.getDate() - 13);
      const thirtyDaysAgo = new Date(today);
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 29);
      const thisMonthStart = new Date(today.getFullYear(), today.getMonth(), 1);
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const formatDate = (d: Date) => `${months[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
      const formatRange = (s: Date, e: Date) => {
        if (s.toDateString() === e.toDateString()) return formatDate(s);
        if (s.getFullYear() === e.getFullYear()) {
          if (s.getMonth() === e.getMonth()) return `${months[s.getMonth()]} ${s.getDate()} - ${e.getDate()}, ${s.getFullYear()}`;
          return `${months[s.getMonth()]} ${s.getDate()} - ${months[e.getMonth()]} ${e.getDate()}, ${s.getFullYear()}`;
        }
        return `${months[s.getMonth()]} ${s.getDate()}, ${s.getFullYear()} - ${months[e.getMonth()]} ${e.getDate()}, ${e.getFullYear()}`;
      };

      switch (range) {
        case 'Today': return { label: 'Today', dateString: formatDate(today), start: today, end: today };
        case 'Yesterday': return { label: 'Yesterday', dateString: formatDate(yesterday), start: yesterday, end: yesterday };
        case 'Last 7 Days': return { label: 'Last 7 Days', dateString: formatRange(sevenDaysAgo, today), start: sevenDaysAgo, end: today };
        case 'Last 14 Days': return { label: 'Last 14 Days', dateString: formatRange(fourteenDaysAgo, today), start: fourteenDaysAgo, end: today };
        case 'Last 30 Days': return { label: 'Last 30 Days', dateString: formatRange(thirtyDaysAgo, today), start: thirtyDaysAgo, end: today };
        case 'This Month': return { label: 'This Month', dateString: formatRange(thisMonthStart, today), start: thisMonthStart, end: today };
        case 'Custom':
          if (start && end) return { label: 'Custom', dateString: formatRange(start, end), start, end };
          return { label: 'Custom', dateString: 'Select dates...', start: today, end: today };
        default: return { label: 'Yesterday', dateString: formatDate(yesterday), start: yesterday, end: yesterday };
      }
    };

    if (currentPage === 'daily_report') {
      return getRangeInfo(dailyReportRange, dailyReportStart, dailyReportEnd).dateString;
    }
    if (currentPage === 'hourly') {
      return getRangeInfo(hourlyRange, hourlyDateStart, hourlyDateEnd).dateString;
    }
    return getRangeInfo(selectedRange, customDateStart, customDateEnd).dateString;
  }, [currentPage, selectedRange, customDateStart, customDateEnd, dailyReportRange, dailyReportStart, dailyReportEnd, hourlyRange, hourlyDateStart, hourlyDateEnd]);

  useEffect(() => {
    setPaginationPage(1);
  }, [activeFilters, quickFilterText, selectedRange, customDateStart, customDateEnd]);

  useEffect(() => {
    if (useMock) {
      setUseMock(false);
      setError(null);
    }
  }, [currentUser.id]);

  useEffect(() => {
    const unsubscribe = onConnectionStatusChange((status) => {
      setConnectionStatus(status);
    });
    return unsubscribe;
  }, []);

  useEffect(() => {
    const loadEtlStatus = async () => {
      if (import.meta.env.DEV) {
        setEtlStatus({
          last_update: '2026.01.13 09:00',
          report_date: new Date().toISOString().split('T')[0],
          all_success: true
        });
        return;
      }

      try {
        const status = await dashboardApi.getEtlStatus();
        setEtlStatus(status);
      } catch (err) {
        console.error('Failed to load ETL status:', err);
      }
    };

    loadEtlStatus();
    const interval = setInterval(loadEtlStatus, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const loadOfferDetails = async () => {
      if (currentPage !== 'performance') return;

      try {
        const result = await offersApi.getDetailsMap();
        setOfferDetailsMap(result.data);
      } catch (err) {
        console.error('Failed to load offer details:', err);
      }
    };

    loadOfferDetails();
  }, [currentPage]);

  // Data loading functions
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

  useEffect(() => {
    if (currentPage !== 'performance') return;
    if (!hasLoadedDataAfterInit.current) return;
    if (!pendingRangeRefresh) return;
    setPendingRangeRefresh(false);
    clearDataCache();
    handleRefreshData();
  }, [pendingRangeRefresh, currentPage, handleRefreshData]);

  useEffect(() => {
    if (currentPage !== 'performance') return;
    if (!hasLoadedDataAfterInit.current) return;
    loadRootData();
  }, [loadRootData, currentPage]);

  // Load saved views
  useEffect(() => {
    const loadViews = async () => {
      try {
        const views = await viewsApi.getAllViews();
        setSavedViews(views);
        const userKey = getUserStorageKey(currentUser.id);
        localStorage.setItem(userKey, JSON.stringify(views));
      } catch (error) {
        console.error('Failed to load views from backend:', error);
        const userKey = getUserStorageKey(currentUser.id);
        const saved = localStorage.getItem(userKey);
        if (saved) {
          setSavedViews(JSON.parse(saved));
        }
      }
    };
    loadViews();
  }, [currentUser.id]);

  // Load default view
  const hasLoadedDefaultView = useRef(false);
  const loadRootDataRef = useRef(loadRootData);

  useEffect(() => {
    loadRootDataRef.current = loadRootData;
  }, [loadRootData]);

  useEffect(() => {
    const loadDefaultView = async () => {
      if (hasLoadedDefaultView.current) return;
      hasLoadedDefaultView.current = true;

      try {
        const defaultView = await viewsApi.getDefaultView();
        if (defaultView) {
          applyView(defaultView);
        } else {
          setActiveDims(['platform', 'sub_campaign_name']);
        }
      } catch (error) {
        console.error('Failed to load default view:', error);
        setActiveDims(['platform', 'sub_campaign_name']);
      }
      setIsInitialized(true);
      hasLoadedDataAfterInit.current = true;
      loadRootDataRef.current();
    };
    loadDefaultView();
  }, []);

  useEffect(() => {
    const clickOutside = (e: MouseEvent) => {
      if (viewsDropdownRef.current && !viewsDropdownRef.current.contains(e.target as Node)) setShowViewList(false);
    };
    document.addEventListener('mousedown', clickOutside);
    return () => document.removeEventListener('mousedown', clickOutside);
  }, []);

  // Helper functions
  const getRowText = (row: AdRow): string => row.name;

  const replaceDimension = (index: number, newDim: Dimension) => {
    if (activeDims.includes(newDim)) return;
    const newDims = [...activeDims];
    newDims[index] = newDim;

    const validFilters = activeFilters.filter((filter, filterIndex) => {
      return newDims[filterIndex] === filter.dimension;
    });

    setActiveDims(newDims);
    setActiveFilters(validFilters);
    setEditingDimIndex(null);
    setDropdownPosition(null);
  };

  const openDimDropdown = (idx: number, e: React.MouseEvent) => {
    const target = e.currentTarget as HTMLElement;
    const rect = target.getBoundingClientRect();
    const tableHeader = document.querySelector('thead');
    const headerHeight = tableHeader ? tableHeader.getBoundingClientRect().height : 0;
    setDropdownPosition({
      top: Math.max(rect.bottom + 4, headerHeight + 4),
      left: rect.left
    });
    setEditingDimIndex(idx);
  };

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      const dropdowns = document.querySelectorAll('[data-dim-dropdown]');
      let clickedInside = false;
      dropdowns.forEach(dropdown => {
        if (dropdown.contains(target)) clickedInside = true;
      });
      if (!clickedInside) {
        setEditingDimIndex(null);
        setDropdownPosition(null);
      }
    };
    if (editingDimIndex !== null) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [editingDimIndex]);

  // View management
  const handleSaveView = async (e: React.MouseEvent) => {
    e.stopPropagation();
    const viewName = prompt("Save Current View As:");
    if (!viewName || viewName.trim() === "") return;
    const newView: SavedView = {
      id: "view_" + Date.now(),
      name: viewName.trim(),
      dimensions: [...activeDims],
      visibleMetrics: metrics.filter(m => m.visible).map(m => m.key as string),
      colorMode: colorMode,
      userId: currentUser.id,
      createdAt: new Date().toISOString(),
      isDefault: false
    };

    try {
      const savedView = await viewsApi.createView(newView);
      setSavedViews(prev => [...prev, savedView]);
      const userKey = getUserStorageKey(currentUser.id);
      localStorage.setItem(userKey, JSON.stringify([...savedViews, savedView]));
    } catch (error) {
      console.error('Failed to save view to backend:', error);
      setSavedViews(prev => {
        const updated = [...prev, newView];
        const userKey = getUserStorageKey(currentUser.id);
        localStorage.setItem(userKey, JSON.stringify(updated));
        return updated;
      });
    }
    setShowViewList(false);
  };

  const applyView = (view: SavedView) => {
    setActiveDims([...view.dimensions]);
    setMetrics(prev => {
      const visibleKeys = view.visibleMetrics;
      const sorted = [...prev].sort((a, b) => {
        const idxA = visibleKeys.indexOf(a.key as string);
        const idxB = visibleKeys.indexOf(b.key as string);
        if (idxA === -1 && idxB === -1) return 0;
        if (idxA === -1) return 1;
        if (idxB === -1) return -1;
        return idxA - idxB;
      });
      return sorted.map(m => ({ ...m, visible: visibleKeys.includes(m.key as string) }));
    });
    setColorMode(view.colorMode || false);
    setShowViewList(false);
    setActiveFilters([]);
    setQuickFilterText('');
  };

  const deleteView = async (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    if (!window.confirm("Delete this saved view?")) return;

    try {
      await viewsApi.deleteView(id);
      setSavedViews(prev => {
        const updated = prev.filter(v => v.id !== id);
        const userKey = getUserStorageKey(currentUser.id);
        localStorage.setItem(userKey, JSON.stringify(updated));
        return updated;
      });
    } catch (error) {
      console.error('Failed to delete view from backend:', error);
      setSavedViews(prev => {
        const updated = prev.filter(v => v.id !== id);
        const userKey = getUserStorageKey(currentUser.id);
        localStorage.setItem(userKey, JSON.stringify(updated));
        return updated;
      });
    }
  };

  const setDefaultView = async (e: React.MouseEvent, viewId: string) => {
    e.stopPropagation();

    try {
      const updatedView = await viewsApi.setDefaultView(viewId);
      setSavedViews(prev => {
        const updated = prev.map(v => ({ ...v, isDefault: v.id === viewId }));
        const userKey = getUserStorageKey(currentUser.id);
        localStorage.setItem(userKey, JSON.stringify(updated));
        return updated;
      });
    } catch (error) {
      console.error('Failed to set default view:', error);
      setSavedViews(prev => {
        const updated = prev.map(v => ({ ...v, isDefault: v.id === viewId }));
        const userKey = getUserStorageKey(currentUser.id);
        localStorage.setItem(userKey, JSON.stringify(updated));
        return updated;
      });
    }
  };

  const toggleDimension = (dim: Dimension) => {
    setActiveDims(prev => prev.includes(dim) ? prev.filter(d => d !== dim) : [...prev, dim]);
    setActiveFilters([]);
  };

  const draggedDimIndex = useRef<number | null>(null);

  const handleDimDragStart = (e: React.DragEvent, idx: number) => {
    draggedDimIndex.current = idx;
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDimDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
  };

  const handleDimDrop = (e: React.DragEvent, dropIdx: number) => {
    e.preventDefault();
    const dragIdx = draggedDimIndex.current;
    if (dragIdx === null || dragIdx === dropIdx) return;

    const newDims = [...activeDims];
    const [removed] = newDims.splice(dragIdx, 1);
    newDims.splice(dropIdx, 0, removed);

    const validFilters = activeFilters.filter((filter, index) => {
      return newDims[index] === filter.dimension;
    });

    setActiveDims(newDims);
    setActiveFilters(validFilters);
    draggedDimIndex.current = null;
  };

  const dragItemKey = useRef<string | null>(null);
  const dragOverItemKey = useRef<string | null>(null);

  const handleMetricReorder = () => {
    if (dragItemKey.current !== null && dragOverItemKey.current !== null) {
      const copyListItems = [...metrics];
      const fromIndex = copyListItems.findIndex(m => m.key === dragItemKey.current);
      const toIndex = copyListItems.findIndex(m => m.key === dragOverItemKey.current);
      if (fromIndex !== -1 && toIndex !== -1) {
        const dragItemContent = copyListItems[fromIndex];
        copyListItems.splice(fromIndex, 1);
        copyListItems.splice(toIndex, 0, dragItemContent);
        setMetrics(copyListItems);
      }
      dragItemKey.current = null;
      dragOverItemKey.current = null;
    }
  };

  const toggleDimExpansion = async (e: React.MouseEvent, row: AdRow) => {
    e.preventDefault();
    e.stopPropagation();
    if (!row.hasChild) return;

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
          const update = (rows: AdRow[]): AdRow[] => {
            return rows.map(r => {
              const match = r.id === row.id;
              return match
                ? { ...r, children }
                : (r.children ? { ...r, children: update(r.children) } : r);
            });
          };
          return update(prev);
        });
      }
    }
  };

  const toggleDailyBreakdown = async (e: React.MouseEvent, row: AdRow) => {
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

  // Filter and flatten data
  const filteredAndFlattenedData = useMemo(() => {
    const sortRows = (rows: AdRow[]): AdRow[] => {
      if (rows.length === 0) return rows;

      const isDateDimension = rows[0].dimensionType === 'date';
      if (isDateDimension) {
        return [...rows].sort((a, b) => b.name.localeCompare(a.name));
      }

      if (!sortColumn || !sortOrder) return rows;
      return [...rows].sort((a, b) => {
        const aVal = a[sortColumn] as number;
        const bVal = b[sortColumn] as number;
        if (sortOrder === 'asc') return aVal - bVal;
        else return bVal - aVal;
      });
    };

    const flatten = (rows: AdRow[]): AdRow[] => {
      const sortedRows = sortRows(rows);
      const results: AdRow[] = [];

      sortedRows.forEach(row => {
        const searchTerms = quickFilterText.toLowerCase().trim().split(/\s+/).filter(k => k);
        const matchesFilter = searchTerms.length === 0 || searchTerms.some(term => row.name.toLowerCase().includes(term));
        const shouldShow = !hideZeroImpressions || !(row.impressions < 20 && row.revenue === 0);

        if (matchesFilter && shouldShow) {
          results.push(row);
        }

        if (expandedDimRows.has(row.id) && row.children) {
          let childRows = row.children || [];
          childRows = childRows.filter(child => !hideZeroImpressions || !(child.impressions < 20 && child.revenue === 0));
          childRows = sortRows(childRows);
          childRows.forEach(child => {
            results.push(child);
            if (expandedDimRows.has(child.id) && child.children) {
              let grandchildRows = child.children.filter(gc => !hideZeroImpressions || !(gc.impressions < 20 && gc.revenue === 0));
              grandchildRows = sortRows(grandchildRows);
              grandchildRows.forEach(grandchild => {
                results.push(grandchild);
              });
            }
          });
        }
      });
      return results;
    };

    return flatten(data);
  }, [data, expandedDimRows, quickFilterText, hideZeroImpressions, sortColumn, sortOrder]);

  // Summary data
  const summaryData = useMemo(() => {
    const summary = {
      impressions: 0, clicks: 0, conversions: 0,
      spend: 0, revenue: 0, profit: 0, m_imp: 0, m_clicks: 0, m_conv: 0,
    };
    const minLevel = filteredAndFlattenedData.length > 0
      ? Math.min(...filteredAndFlattenedData.map(row => row.level))
      : 0;
    filteredAndFlattenedData.forEach(row => {
      if (row.level === minLevel) {
        summary.impressions += row.impressions || 0;
        summary.clicks += row.clicks || 0;
        summary.conversions += row.conversions || 0;
        summary.spend += row.spend || 0;
        summary.revenue += row.revenue || 0;
        summary.profit += row.profit || 0;
        summary.m_imp += row.m_imp || 0;
        summary.m_clicks += row.m_clicks || 0;
        summary.m_conv += row.m_conv || 0;
      }
    });
    return {
      ...summary,
      ctr: summary.clicks / (summary.impressions || 1),
      cvr: summary.conversions / (summary.clicks || 1),
      roi: summary.spend > 0 ? (summary.revenue - summary.spend) / summary.spend : 0,
      cpa: summary.spend / (summary.conversions || 1),
      rpa: summary.revenue / (summary.conversions || 1),
      epc: summary.revenue / (summary.clicks || 1),
      epv: summary.revenue / (summary.impressions || 1),
      m_epc: summary.revenue / (summary.m_clicks || 1),
      m_epv: summary.revenue / (summary.m_imp || 1),
      m_cpc: summary.spend / (summary.m_clicks || 1),
      m_cpv: summary.spend / (summary.m_imp || 1),
      m_cpa: summary.spend / (summary.m_conv || 1),
      m_epa: summary.revenue / (summary.m_conv || 1),
    };
  }, [filteredAndFlattenedData]);

  // Paginated data
  const paginatedData = useMemo(() => {
    const startIndex = (paginationPage - 1) * rowsPerPage;
    const endIndex = startIndex + rowsPerPage;
    return filteredAndFlattenedData.slice(startIndex, endIndex);
  }, [filteredAndFlattenedData, paginationPage, rowsPerPage]);

  const totalRows = filteredAndFlattenedData.length;
  const totalPages = Math.ceil(totalRows / rowsPerPage);

  // Sort handlers
  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      if (sortOrder === 'desc') {
        setSortOrder('asc');
      } else if (sortOrder === 'asc') {
        const defaultSortColumn = visibleMetrics.find(m => m.key === 'spend')?.key || visibleMetrics[0]?.key || 'spend';
        setSortColumn(defaultSortColumn as SortColumn);
        setSortOrder('desc');
      }
    } else {
      setSortColumn(column);
      setSortOrder('desc');
    }
  };

  const getSortIcon = (column: SortColumn) => {
    if (sortColumn !== column) {
      return <i className="fas fa-sort text-slate-300 text-[8px]"></i>;
    }
    if (sortOrder === 'asc') {
      return <i className="fas fa-sort-up text-indigo-600 text-[10px]"></i>;
    }
    if (sortOrder === 'desc') {
      return <i className="fas fa-sort-down text-indigo-600 text-[10px]"></i>;
    }
    return <i className="fas fa-sort text-slate-300 text-[8px]"></i>;
  };

  const visibleMetrics = metrics.filter(m => {
    if (!m.visible) return false;
    if (currentUser.showRevenue === false) {
      const revenueKeys = ['revenue', 'profit', 'epa', 'epc', 'epv', 'roi', 'm_epc', 'm_epv', 'm_epa'];
      return !revenueKeys.includes(m.key);
    }
    return true;
  });

  return (
    <div className="flex h-screen bg-white overflow-hidden text-slate-900 font-sans">
      {/* Context Menu */}
      {contextMenu && (
        <div
          ref={contextMenuRef}
          className="fixed z-[9999] bg-white rounded-lg shadow-xl border border-slate-200 py-1 min-w-[150px]"
          style={{ left: contextMenu.x, top: contextMenu.y }}
        >
          <button
            onClick={handleCopyText}
            className="w-full px-4 py-2 text-left text-sm text-slate-700 hover:bg-slate-100 flex items-center gap-2"
          >
            <i className="fas fa-copy text-slate-400"></i>
            复制文本
          </button>
        </div>
      )}

      {/* Offer Notes Tooltip */}
      {notesTooltip && createPortal(
        <div
          className="fixed z-[99999]"
          style={{
            left: `${notesTooltip.x}px`,
            top: `${notesTooltip.y}px`,
            transform: 'translateY(-50%)'
          }}
          onMouseEnter={() => {
            if (notesTooltipTimerRef.current) {
              clearTimeout(notesTooltipTimerRef.current);
            }
          }}
          onMouseLeave={() => {
            notesTooltipTimerRef.current = setTimeout(() => {
              setNotesTooltip(null);
            }, 1500);
          }}
        >
          <div className="bg-slate-900 text-white text-xs rounded-lg px-4 py-3 max-w-md whitespace-normal shadow-2xl border border-slate-600">
            <div
              className="font-semibold text-amber-400 mb-2 text-sm cursor-pointer hover:text-amber-300"
              onClick={(e) => {
                e.stopPropagation();
                const offerId = Object.keys(offerDetailsMap).find(key => offerDetailsMap[key]?.notes === notesTooltip.content);
                if (offerId) {
                  const offerName = offerDetailsMap[offerId]?.name || offerId;
                  setNotesModal({ offerName, content: notesTooltip.content });
                }
              }}
            >
              Notes: (点击查看完整内容)
            </div>
            <div className="text-slate-100 break-words whitespace-pre-wrap leading-relaxed max-h-80 overflow-y-auto">{notesTooltip.content}</div>
            <div className="absolute left-0 top-1/2 -translate-y-1/2 -translate-x-1 w-2 h-2 bg-slate-900 rotate-45 border-l border-b border-slate-600"></div>
          </div>
        </div>,
        document.body
      )}

      {/* Offer Notes Modal */}
      {notesModal && createPortal(
        <div
          className="fixed inset-0 z-[99999] flex items-center justify-center bg-black/50 backdrop-blur-sm"
          onClick={() => setNotesModal(null)}
        >
          <div
            className="bg-white rounded-xl shadow-2xl max-w-2xl w-full mx-4 max-h-[80vh] flex flex-col animate-in fade-in zoom-in duration-200"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-200">
              <div className="flex items-center gap-3">
                <i className="fas fa-sticky-note text-amber-500"></i>
                <h3 className="text-lg font-bold text-slate-800">Offer Notes</h3>
              </div>
              <button onClick={() => setNotesModal(null)} className="w-8 h-8 flex items-center justify-center rounded-full hover:bg-slate-100">
                <i className="fas fa-times text-slate-400 hover:text-slate-600"></i>
              </button>
            </div>
            <div className="px-6 py-3 bg-slate-50 border-b border-slate-100">
              <span className="text-sm text-slate-500">Offer: </span>
              <span className="text-sm font-semibold text-slate-800 ml-2">{notesModal.offerName}</span>
            </div>
            <div className="flex-1 overflow-y-auto px-6 py-4">
              <pre className="text-sm text-slate-700 whitespace-pre-wrap leading-relaxed font-sans">{notesModal.content || '(无内容)'}</pre>
            </div>
            <div className="px-6 py-3 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={() => navigator.clipboard.writeText(notesModal.content)} className="px-4 py-2 bg-indigo-500 hover:bg-indigo-600 text-white text-sm rounded-lg flex items-center gap-2">
                <i className="fas fa-copy"></i> 复制内容
              </button>
              <button onClick={() => setNotesModal(null)} className="px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm rounded-lg">
                关闭
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* Connection Status Indicator */}
      {connectionStatus === 'retrying' && (
        <div className="fixed top-4 right-4 z-50 bg-amber-50 text-amber-800 px-4 py-2.5 rounded-lg shadow-lg border border-amber-200 flex items-center gap-3 animate-pulse">
          <div className="w-4 h-4 border-2 border-amber-600 border-t-transparent rounded-full animate-spin"></div>
          <span className="text-sm font-medium">正在重连服务器...</span>
        </div>
      )}
      {connectionStatus === 'failed' && (
        <div className="fixed top-4 right-4 z-50 bg-rose-50 text-rose-800 px-4 py-2.5 rounded-lg shadow-lg border border-rose-200 flex items-center gap-3">
          <i className="fas fa-exclamation-triangle text-rose-600"></i>
          <span className="text-sm font-medium">服务器连接失败，请刷新页面重试</span>
        </div>
      )}

      {/* Sidebar */}
      <aside className={`bg-[#1e293b] text-slate-400 flex flex-col shrink-0 transition-all ${isSidebarOpen ? 'w-64' : 'w-20'}`}>
        <div className="p-6 border-b border-slate-700/50 flex items-center gap-3">
          <div className="w-8 h-8 bg-indigo-500 rounded-lg text-white flex items-center justify-center font-bold text-xs">EF</div>
          {isSidebarOpen && <span className="text-white font-black text-sm uppercase italic tracking-tighter">Data Insight</span>}
        </div>
        <nav className="flex-1 py-6">
          <div>
            <button
              onClick={() => setPerformanceMenuOpen(!performanceMenuOpen)}
              className={`w-full flex items-center justify-between gap-4 px-6 py-4 transition-colors ${(currentPage === 'performance' || currentPage === 'hourly') ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}
            >
              <div className="flex items-center gap-4">
                <i className="fas fa-chart-bar w-5 text-center"></i>
                {isSidebarOpen && <span className="text-sm font-bold">Performance</span>}
              </div>
              {isSidebarOpen && (
                <i className={`fas fa-chevron-${performanceMenuOpen ? 'down' : 'right'} text-xs transition-transform`}></i>
              )}
            </button>
            {performanceMenuOpen && isSidebarOpen && (
              <div className="ml-6 border-l border-slate-700/50">
                <button
                  onClick={() => navigateTo('performance')}
                  className={`w-full flex items-center gap-4 px-6 py-3 transition-colors ${currentPage === 'performance' && performanceSubPage === 'dates' ? 'text-indigo-400 bg-slate-800/50' : 'text-slate-500 hover:bg-slate-800/30'}`}
                >
                  <span className="w-2"></span>
                  <i className="fas fa-calendar-alt w-4 text-center text-xs"></i>
                  <span className="text-xs font-bold">Dates Report</span>
                </button>
                {currentUser.showRevenue !== false && (
                  <button
                    onClick={() => navigateTo('hourly')}
                    className={`w-full flex items-center gap-4 px-6 py-3 transition-colors ${currentPage === 'hourly' ? 'text-indigo-400 bg-slate-800/50' : 'text-slate-500 hover:bg-slate-800/30'}`}
                  >
                    <span className="w-2"></span>
                    <i className="fas fa-clock w-4 text-center text-xs"></i>
                    <span className="text-xs font-bold">Hourly Insight</span>
                  </button>
                )}
              </div>
            )}
          </div>

          {currentUser.role === 'admin' && (
            <button onClick={() => navigateTo('daily_report')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'daily_report' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
              <i className="fas fa-calendar-day w-5 text-center"></i>
              {isSidebarOpen && <span className="text-sm font-bold">Daily Report</span>}
            </button>
          )}
          {currentUser.role === 'admin' && (
            <button onClick={() => navigateTo('permissions')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'permissions' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
              <i className="fas fa-user-shield w-5 text-center"></i>
              {isSidebarOpen && <span className="text-sm font-bold">Permissions</span>}
            </button>
          )}
          {currentUser.role === 'admin' && (
            <button onClick={() => navigateTo('config')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'config' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
              <i className="fas fa-cog w-5 text-center"></i>
              {isSidebarOpen && <span className="text-sm font-bold">Config</span>}
            </button>
          )}
        </nav>
        <div className="p-4 border-t border-slate-700/50">
          <button onClick={onLogout} className="w-full flex items-center gap-4 px-2 py-3 hover:bg-slate-800 rounded-lg text-rose-400">
            <i className="fas fa-sign-out-alt w-5 text-center"></i>
            {isSidebarOpen && <span className="text-sm font-bold">Sign Out</span>}
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col min-w-0 bg-[#f8fafc]">
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 z-[100] shadow-sm shrink-0">
          <div className="flex items-center gap-4">
            <button onClick={() => setIsSidebarOpen(!isSidebarOpen)} className="p-2 hover:bg-slate-50 rounded-lg text-slate-400"><i className="fas fa-bars"></i></button>
            <h2 className="font-extrabold text-slate-800 tracking-tight ml-2 uppercase italic text-sm">
              {currentPage === 'performance' && performanceSubPage === 'dates' ? 'Dates Report' :
               currentPage === 'hourly' ? 'Hourly Insight' :
               currentPage === 'daily_report' ? 'Daily Report' :
               currentPage === 'config' ? 'Config' : 'Permissions'}
            </h2>
            {(currentPage === 'performance' || currentPage === 'daily_report' || currentPage === 'hourly') && (
              <DatePicker
                onRangeChange={(range, start, end) => {
                  if (currentPage === 'daily_report') {
                    setDailyReportRange(range);
                    setDailyReportStart(start);
                    setDailyReportEnd(end);
                  } else if (currentPage === 'hourly') {
                    setHourlyRange(range);
                    setHourlyDateStart(start);
                    setHourlyDateEnd(end);
                  } else {
                    setSelectedRange(range);
                    setCustomDateStart(start);
                    setCustomDateEnd(end);
                    setPendingRangeRefresh(true);
                  }
                }}
                currentDisplay={dateDisplayString}
                currentRange={currentPage === 'daily_report' ? dailyReportRange : currentPage === 'hourly' ? hourlyRange : selectedRange}
              />
            )}
            {currentPage === 'performance' && (
              <div className={`flex items-center gap-1.5 px-3 py-1 rounded-lg text-[10px] font-bold ${
                useMock
                  ? 'bg-amber-100 text-amber-700'
                  : etlStatus?.all_success === false
                    ? 'bg-amber-100 text-amber-700'
                    : 'bg-emerald-100 text-emerald-700'
              }`}>
                <span className={`w-2 h-2 rounded-full ${
                  useMock
                    ? 'bg-amber-500'
                    : etlStatus?.all_success === false
                      ? 'bg-amber-500 animate-pulse'
                      : 'bg-emerald-500 animate-pulse'
                }`}></span>
                <span>{useMock ? 'Mock Data' : 'Live API'}</span>
                <span className="w-px h-3 bg-current opacity-30"></span>
                <span className="font-normal">
                  {etlStatus?.last_update
                    ? `${etlStatus.all_success === false ? 'part update' : 'all update'} ${etlStatus.last_update}`
                    : 'null'
                  }
                </span>
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            {error && (
              <div className="flex items-center gap-2 px-3 py-1.5 bg-rose-100 text-rose-700 rounded-xl text-xs font-bold">
                <i className="fas fa-exclamation-triangle"></i>
                <span>API Error - Using Mock</span>
                <button onClick={() => { setUseMock(false); setError(null); }} className="ml-1 underline hover:text-rose-900">Retry</button>
              </div>
            )}
            {currentPage === 'performance' && (
              <div className="flex items-center gap-2">
                <div className="relative" ref={viewsDropdownRef}>
                  <button onClick={() => setShowViewList(!showViewList)} className={`px-4 py-2 bg-white border ${showViewList ? 'border-indigo-500 ring-2 ring-indigo-500/10' : 'border-slate-200'} text-slate-700 rounded-xl text-xs font-bold shadow-sm flex items-center gap-2 hover:bg-slate-50`}>
                    <i className="fas fa-bookmark text-amber-500"></i> Views
                  </button>
                  {showViewList && (
                    <div className="absolute top-full mt-2 right-0 w-80 bg-white rounded-2xl shadow-2xl border border-slate-100 z-[100] p-2">
                      <div className="flex items-center justify-between p-3 border-b border-slate-50 mb-1">
                        <span className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Global Views</span>
                        <button onClick={handleSaveView} className="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-[10px] font-black uppercase hover:bg-indigo-700"><i className="fas fa-plus mr-1"></i> Save Current</button>
                      </div>
                      <div className="max-h-72 overflow-y-auto p-1">
                        {savedViews.length > 0 ? savedViews.map(v => (
                          <div key={v.id} onClick={() => applyView(v)} className="flex items-center justify-between w-full px-3 py-3 text-[11px] font-bold text-slate-600 hover:bg-indigo-50 rounded-xl cursor-pointer group mb-1">
                            <div className="flex items-center gap-2 overflow-hidden">
                              <i className="fas fa-table-list text-slate-300 group-hover:text-indigo-400"></i>
                              <span className="truncate">{v.name}</span>
                              {v.isDefault && (
                                <span className="px-1.5 py-0.5 bg-slate-100 text-slate-400 rounded text-[9px] font-bold uppercase">default</span>
                              )}
                            </div>
                            <div className="flex items-center gap-1">
                              <button onClick={(e) => setDefaultView(e, v.id)} className={`w-7 h-7 flex items-center justify-center rounded-lg opacity-0 group-hover:opacity-100 ${v.isDefault ? 'text-amber-500 bg-amber-50' : 'text-slate-400 hover:text-amber-500'}`}>
                                <i className={`fas ${v.isDefault ? 'fa-star' : 'fa-regular fa-star'} text-[10px]`}></i>
                              </button>
                              <button onClick={(e) => deleteView(e, v.id)} className="w-7 h-7 flex items-center justify-center rounded-lg opacity-0 group-hover:opacity-100 text-slate-400 hover:text-rose-500"><i className="fas fa-trash-can text-[10px]"></i></button>
                            </div>
                          </div>
                        )) : <p className="py-10 text-center text-slate-400 text-[10px]">No saved views</p>}
                      </div>
                    </div>
                  )}
                </div>
                <button onClick={() => setShowColumnEditor(true)} className="px-4 py-2 bg-slate-900 text-white rounded-xl text-xs font-bold shadow-lg flex items-center gap-2 hover:bg-slate-800"><i className="fas fa-columns"></i> Columns</button>
              </div>
            )}
          </div>
        </header>

        {/* Page Content */}
        {currentPage === 'performance' ? (
          <>
            <div className="px-8 py-5 bg-white border-b border-slate-200 flex flex-col gap-4 z-30 shadow-sm shrink-0">
              <div className="flex items-center gap-4 overflow-x-auto pb-1">
                <div className="flex flex-col">
                  <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest whitespace-nowrap">Pivot Layout:</span>
                  <span className="text-[8px] text-slate-400 leading-tight">Default filter imp&lt;20 &amp; rev=0</span>
                </div>
                <div className="flex items-center gap-2">
                  {activeDims.map((dim, idx) => {
                    const dimLabel = ALL_DIMENSIONS.find(d => d.value === dim)?.label;
                    return (
                      <div key={dim} className="relative" data-dim-dropdown="true">
                        <div
                          draggable
                          onDragStart={(e) => handleDimDragStart(e, idx)}
                          onDragOver={(e) => handleDimDragOver(e)}
                          onDrop={(e) => handleDimDrop(e, idx)}
                          className="flex items-center bg-indigo-50 border border-indigo-100 px-3 py-1.5 rounded-xl gap-2 shadow-sm cursor-move hover:bg-indigo-100"
                        >
                          <i className="fas fa-grip-vertical text-indigo-300 text-xs"></i>
                          <span
                            onClick={(e) => openDimDropdown(idx, e)}
                            className="text-xs font-black text-indigo-700 cursor-pointer hover:text-indigo-900 underline decoration-dotted underline-offset-2"
                          >
                            {dimLabel}
                          </span>
                          <button onClick={(e) => { e.stopPropagation(); toggleDimension(dim); }} className="ml-1 text-indigo-200 hover:text-rose-500"><i className="fas fa-times-circle text-xs"></i></button>
                        </div>
                        {editingDimIndex === idx && dropdownPosition && createPortal(
                          <div
                            className="fixed z-[99999] bg-white rounded-xl shadow-2xl border border-slate-200 py-2 min-w-[180px]"
                            style={{ top: `${dropdownPosition.top}px`, left: `${dropdownPosition.left}px` }}
                            data-dim-dropdown="true"
                          >
                            <div className="px-3 py-2 border-b border-slate-100">
                              <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">Replace <span className="text-indigo-600">{dimLabel}</span> with:</div>
                            </div>
                            {ALL_DIMENSIONS.filter(d => d.value !== dim).map(d => (
                              <button
                                key={d.value}
                                onClick={(e) => { e.stopPropagation(); replaceDimension(idx, d.value); }}
                                className={`w-full px-4 py-2 text-left text-xs font-medium hover:bg-indigo-50 flex items-center gap-2 ${activeDims.includes(d.value) ? 'text-slate-300' : 'text-slate-700'}`}
                              >
                                <i className={`fas fa-circle text-[6px] ${activeDims.includes(d.value) ? 'text-slate-300' : 'text-indigo-400'}`}></i>
                                {d.label}
                              </button>
                            ))}
                          </div>,
                          document.body
                        )}
                      </div>
                    );
                  })}
                  <div className="h-4 w-px bg-slate-200 mx-2"></div>
                  {ALL_DIMENSIONS.filter(d => !activeDims.includes(d.value)).map(d => (
                    <button key={d.value} onClick={() => toggleDimension(d.value)} className="px-3 py-1.5 border border-dashed border-slate-300 text-slate-400 rounded-xl text-[10px] font-bold hover:border-indigo-400 hover:text-indigo-500">+ {d.label}</button>
                  ))}
                </div>
              </div>
              <div className="flex justify-between items-center">
                <div className="flex gap-2 items-center">
                  {activeFilters.length > 0 ? activeFilters.map((f, i) => (
                    <div key={i} className="flex items-center bg-indigo-50 border border-indigo-100 rounded-md px-2 py-1 gap-2">
                      <span className="text-[10px] font-bold text-indigo-700">{f.value}</span>
                      <button onClick={() => setActiveFilters(prev => prev.slice(0, i))} className="text-indigo-300 hover:text-rose-500"><i className="fas fa-times text-[9px]"></i></button>
                    </div>
                  )) : <span className="text-[10px] text-slate-400 italic">Drill down by clicking rows</span>}
                  <label className="flex items-center gap-2 px-3 py-1 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100">
                    <input type="checkbox" checked={colorMode} onChange={(e) => setColorMode(e.target.checked)} className="w-3 h-3 rounded border-slate-300 text-indigo-600" />
                    <span className="text-[10px] font-bold text-slate-600">Color Mode</span>
                  </label>
                </div>
                <div className="flex gap-2 items-center">
                  <button type="button" onClick={handleRefreshData} disabled={isRefreshing} className={`px-3 py-1.5 rounded-lg text-xs font-bold flex items-center gap-1.5 ${isRefreshing ? 'bg-slate-100 text-slate-400' : 'bg-indigo-50 text-indigo-600 hover:bg-indigo-100'}`}>
                    <i className={`fas fa-sync-alt ${isRefreshing ? 'animate-spin' : ''}`}></i>
                    <span>{isRefreshing ? 'Refreshing...' : 'Refresh'}</span>
                  </button>
                  <div className="relative w-64">
                    <i className="fas fa-search absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 text-xs"></i>
                    <input type="text" placeholder="Quick Search..." className="w-full pl-9 pr-4 py-2 bg-slate-50 border border-slate-200 rounded-xl text-xs" value={quickFilterText} onChange={(e) => setQuickFilterText(e.target.value)} />
                  </div>
                </div>
              </div>
            </div>

            <div className="flex-1 overflow-auto bg-white flex flex-col">
              <div className="flex-1 overflow-auto relative">
                {loading && (
                  <div className="absolute inset-0 bg-white/80 backdrop-blur-sm z-50 flex items-center justify-center">
                    <div className="flex flex-col items-center gap-3">
                      <div className="w-10 h-10 border-[3px] border-indigo-200 border-t-indigo-600 rounded-full animate-spin"></div>
                      <span className="text-sm font-bold text-slate-600">Loading...</span>
                    </div>
                  </div>
                )}
                <table ref={tableRef} className="w-full text-left border-collapse" style={{ minWidth: Object.values(columnWidths).reduce((a, b) => a + b, 0) + 200 }}>
                  <thead>
                    <tr className="bg-slate-50/95 text-[10px] font-black text-slate-400 uppercase tracking-widest border-b border-slate-200 sticky top-0 z-50">
                      <th className="px-4 py-4 sticky left-0 bg-slate-50/95 z-[60] border-r border-slate-200 relative" style={{ width: columnWidths.hierarchy }}>
                        Grouping Hierarchy
                        <div className={`absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-indigo-500 ${resizingColumn === 'hierarchy' ? 'bg-indigo-500' : ''}`} onMouseDown={(e) => handleResizeStart('hierarchy', e)}></div>
                      </th>
                      {visibleMetrics.map(m => (
                        <th key={m.key} className="px-4 py-4 text-right relative group" style={{ width: columnWidths[m.key] || 90 }}>
                          <div className="flex items-center justify-end gap-1 cursor-pointer hover:text-indigo-600" onClick={() => handleSort(m.key as SortColumn)}>
                            <span>{m.label}</span>
                            <span className="inline-flex w-3">{getSortIcon(m.key as SortColumn)}</span>
                          </div>
                          <div className={`absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-indigo-500 ${resizingColumn === m.key ? 'bg-indigo-500' : ''}`} onMouseDown={(e) => handleResizeStart(m.key, e)}></div>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-50">
                    {(() => {
                      let childIndex = 0;
                      const dataWithStripe = paginatedData.map((row, idx) => {
                        let isEvenRow = false;
                        if (row.level === 0) isEvenRow = idx % 2 === 0;
                        else { isEvenRow = childIndex % 2 === 1; childIndex++; }
                        return { ...row, isEvenRow };
                      });

                      return dataWithStripe.map((row) => {
                        const isExpanded = expandedDimRows.has(row.id);
                        const isEvenRow = row.isEvenRow;
                        const isChild = row.level > 0;
                        const cellBgClass = isChild ? (isEvenRow ? 'bg-white' : 'bg-yellow-50') : (isEvenRow ? 'bg-slate-100' : 'bg-white');
                        const pyClass = isChild ? 'py-1.5' : 'py-3';
                        const nameClass = isChild ? 'text-[12px] font-semibold text-slate-800' : 'text-[13px] font-black text-slate-800';
                        const labelClass = isChild ? 'text-[8px] text-slate-400 uppercase tracking-wider' : 'text-[9px] text-slate-400 font-bold uppercase tracking-wider';
                        const borderClass = isChild ? 'border-l-4 border-indigo-300' : '';

                        return (
                          <React.Fragment key={row.id}>
                            <tr className="group" onContextMenu={(e) => handleContextMenu(e, getRowText(row))}>
                              <td className={`px-4 sticky left-0 z-10 border-r border-slate-200 group-hover:bg-violet-50 ${cellBgClass} ${pyClass} ${borderClass}`} style={{ paddingLeft: `${row.level * 20 + 32}px`, width: columnWidths.hierarchy }}>
                                <div className="flex items-center gap-2 cursor-pointer" onClick={() => {
                                  const nextFilters = row.filterPath || row.id.split('|').map((v, i) => ({ dimension: activeDims[i], value: v }));
                                  setActiveFilters(nextFilters);
                                  setQuickFilterText('');
                                }}>
                                  <button onClick={(e) => { e.stopPropagation(); toggleDailyBreakdown(e, row); }} className={`w-5 h-5 rounded-full flex items-center justify-center ${expandedDailyRows.has(row.id) ? 'bg-indigo-600' : loadingDailyRows.has(row.id) ? 'bg-amber-400' : 'bg-slate-100'}`}>
                                    {loadingDailyRows.has(row.id) ? (
                                      <svg className="animate-spin h-2.5 w-2.5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                      </svg>
                                    ) : (
                                      <div className={`w-1.5 h-1.5 rounded-full ${expandedDailyRows.has(row.id) ? 'bg-white' : 'bg-slate-400'}`}></div>
                                    )}
                                  </button>
                                  {row.hasChild && <button onClick={(e) => { e.stopPropagation(); toggleDimExpansion(e, row); }} className={`w-6 h-6 rounded flex items-center justify-center ${loadingDimRows.has(row.id) ? 'bg-amber-100 border-amber-200' : 'bg-slate-50 border border-slate-100'} ${isExpanded ? 'rotate-90' : ''}`}>
                                    {loadingDimRows.has(row.id) ? (
                                      <svg className="animate-spin h-3 w-3 text-amber-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                      </svg>
                                    ) : (
                                      <i className={`fas fa-chevron-right text-[10px] ${loadingDimRows.has(row.id) ? 'text-amber-500' : 'text-slate-400'}`}></i>
                                    )}
                                  </button>}
                                  <div className="flex flex-col min-w-0">
                                    <div className="flex items-center gap-1.5 min-w-0">
                                      <span className={`${nameClass} truncate group-hover:text-indigo-600`}>{row.name}</span>
                                      {/* Offer URL Copy and Notes buttons would go here */}
                                    </div>
                                    <span className={labelClass}>{ALL_DIMENSIONS.find(d => d.value === row.dimensionType)?.label}</span>
                                  </div>
                                </div>
                              </td>
                              {visibleMetrics.map(m => <td key={m.key} className={`px-4 ${pyClass} text-right group-hover:bg-violet-50 ${cellBgClass}`} style={{ width: columnWidths[m.key] || 90 }}><MetricValue value={row[m.key] as number} type={m.type} colorMode={colorMode} metricKey={m.key as string} isSub={isChild} /></td>)}
                            </tr>
                            {expandedDailyRows.has(row.id) && (() => {
                              const dailyData = dailyDataMap.get(row.id);
                              return dailyData?.slice(0, 7).map((day, dayIdx) => (
                                <tr key={`${row.id}-${dayIdx}`} className="bg-violet-50 hover:bg-violet-100">
                                  <td className="px-4 py-2 sticky left-0 bg-violet-50 z-10 border-l-4 border-indigo-600/60 border-r border-violet-100" style={{ paddingLeft: `${row.level * 20 + 72}px`, width: columnWidths.hierarchy }}><span className="text-[12px] font-bold text-violet-700">{day.date}</span></td>
                                  {visibleMetrics.map(m => <td key={m.key} className="px-4 py-2 text-right opacity-80 bg-violet-50" style={{ width: columnWidths[m.key] || 90 }}><MetricValue value={day[m.key as keyof DailyBreakdown] as number || 0} type={m.type} isSub colorMode={colorMode} metricKey={m.key as string} /></td>)}
                                </tr>
                              ));
                            })()}
                          </React.Fragment>
                        );
                      });
                    })()}

                    {totalRows > 0 && (
                      <tr className="bg-slate-100 border-t-2 border-slate-300 sticky bottom-0 z-40">
                        <td className="px-4 py-2 sticky left-0 bg-slate-100 z-10 border-r border-slate-300 border-l-4 border-slate-400" style={{ paddingLeft: '52px', width: columnWidths.hierarchy }}>
                          <span className="text-[11px] font-black uppercase text-slate-500 tracking-widest">Summary</span>
                        </td>
                        {visibleMetrics.map(m => {
                          const valueMap: Record<string, number> = {
                            impressions: summaryData.impressions,
                            clicks: summaryData.clicks,
                            conversions: summaryData.conversions,
                            spend: summaryData.spend,
                            revenue: summaryData.revenue,
                            profit: summaryData.profit,
                            ctr: summaryData.ctr * 100,
                            cvr: summaryData.cvr * 100,
                            roi: summaryData.roi * 100,
                            cpa: summaryData.cpa,
                            epa: summaryData.rpa,
                            rpa: summaryData.rpa,
                            epc: summaryData.epc,
                            epv: summaryData.epv,
                            m_epc: summaryData.m_epc,
                            m_epv: summaryData.m_epv,
                            m_cpc: summaryData.m_cpc,
                            m_cpv: summaryData.m_cpv,
                            m_cpa: summaryData.m_cpa,
                            m_epa: summaryData.m_epa,
                            m_imp: summaryData.m_imp,
                            m_clicks: summaryData.m_clicks,
                            m_conv: summaryData.m_conv,
                          };
                          const value = valueMap[m.key] || 0;

                          let displayValue: React.ReactNode;
                          if (m.type === 'money' || m.type === 'profit') {
                            const colorClass = m.key === 'profit' || m.type === 'profit' ? (value > 0 ? 'text-emerald-600' : value < 0 ? 'text-rose-600' : '') : m.key === 'spend' ? 'text-rose-600' : m.key === 'revenue' ? 'text-amber-600' : '';
                            displayValue = <span className={`font-mono tracking-tight leading-none text-[14px] font-bold ${colorClass}`}>${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
                          } else if (m.type === 'percent') {
                            const colorClass = m.key === 'roi' ? (value > 0 ? 'text-emerald-600' : value < 0 ? 'text-rose-600' : '') : '';
                            displayValue = <span className={`font-mono tracking-tight leading-none text-[14px] font-bold ${colorClass}`}>{value.toFixed(2)}%</span>;
                          } else {
                            displayValue = <span className="font-mono tracking-tight leading-none text-[14px] font-bold text-slate-700">{Math.floor(value).toLocaleString()}</span>;
                          }

                          return (
                            <td key={m.key} className="px-4 py-2 text-right" style={{ width: columnWidths[m.key] || 90 }}>
                              {displayValue}
                            </td>
                          );
                        })}
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              <div className="border-t border-slate-200 bg-white px-6 py-3 flex items-center justify-between shrink-0">
                <div className="text-[11px] text-slate-500">
                  Showing <span className="font-bold text-slate-700">{Math.min((paginationPage - 1) * rowsPerPage + 1, totalRows)}</span> to <span className="font-bold text-slate-700">{Math.min(paginationPage * rowsPerPage, totalRows)}</span> of <span className="font-bold text-slate-700">{totalRows}</span> rows
                </div>
                <div className="flex items-center gap-1">
                  <button onClick={() => setPaginationPage(1)} disabled={paginationPage === 1} className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40"><i className="fas fa-angle-double-left"></i></button>
                  <button onClick={() => setPaginationPage(p => Math.max(1, p - 1))} disabled={paginationPage === 1} className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40"><i className="fas fa-chevron-left"></i></button>
                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                    let pageNum;
                    if (totalPages <= 5) pageNum = i + 1;
                    else if (paginationPage <= 3) pageNum = i + 1;
                    else if (paginationPage >= totalPages - 2) pageNum = totalPages - 4 + i;
                    else pageNum = paginationPage - 2 + i;
                    return (
                      <button key={pageNum} onClick={() => setPaginationPage(pageNum)} className={`px-3 py-1 text-[11px] font-bold rounded ${paginationPage === pageNum ? 'bg-indigo-600 text-white' : 'hover:bg-slate-100 text-slate-600'}`}>
                        {pageNum}
                      </button>
                    );
                  })}
                  <button onClick={() => setPaginationPage(p => Math.min(totalPages, p + 1))} disabled={paginationPage === totalPages} className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40"><i className="fas fa-chevron-right"></i></button>
                  <button onClick={() => setPaginationPage(totalPages)} disabled={paginationPage === totalPages} className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40"><i className="fas fa-angle-double-right"></i></button>
                </div>
              </div>
            </div>
          </>
        ) : currentPage === 'hourly' ? (
          <HourlyReport
            selectedRange={hourlyRange}
            customDateStart={hourlyDateStart}
            customDateEnd={hourlyDateEnd}
            onRangeChange={(range, start, end) => {
              setHourlyRange(range);
              setHourlyDateStart(start);
              setHourlyDateEnd(end);
            }}
            currentUser={currentUser}
          />
        ) : currentPage === 'daily_report' ? (
          <DailyReport
            selectedRange={dailyReportRange}
            customDateStart={dailyReportStart}
            customDateEnd={dailyReportEnd}
            onRangeChange={(range, start, end) => {
              setDailyReportRange(range);
              setDailyReportStart(start);
              setDailyReportEnd(end);
            }}
            currentUser={currentUser}
          />
        ) : currentPage === 'config' ? (
          <Config currentUser={currentUser} />
        ) : (
          <PermissionsPage currentUser={currentUser} />
        )}
      </main>

      {/* Column Editor */}
      {showColumnEditor && (
        <div className="fixed inset-0 z-[100] flex justify-end">
          <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm" onClick={() => setShowColumnEditor(false)}></div>
          <div className="relative w-[720px] bg-white h-full shadow-2xl flex flex-col p-8 animate-in slide-in-from-right duration-200">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-2xl font-black uppercase italic tracking-tighter">Manage Columns</h3>
              <button onClick={() => setShowColumnEditor(false)} className="w-10 h-10 flex items-center justify-center rounded-full hover:bg-slate-100"><i className="fas fa-times"></i></button>
            </div>
            <div className="flex-1 flex min-h-0 gap-6">
              <div className="flex-1 flex flex-col min-h-0 bg-slate-50/50 rounded-2xl border border-slate-100 p-5 overflow-y-auto">
                {(['Basic', 'Calculated'] as const).map(groupName => (
                  <div key={groupName} className="mb-6">
                    <span className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-3 block">{groupName} Metrics</span>
                    <div className="space-y-2">
                      {metrics.filter(m => m.group === groupName).map(m => {
                        const revenueKeys = ['revenue', 'profit', 'epa', 'epc', 'epv', 'roi', 'm_epc', 'm_epv', 'm_epa'];
                        const isHiddenByPermission = currentUser.showRevenue === false && revenueKeys.includes(m.key);
                        if (isHiddenByPermission) return null;
                        return (
                          <label key={m.key} className={`flex items-center gap-3 p-3 border rounded-xl cursor-pointer ${m.visible ? 'border-indigo-500 bg-indigo-50' : 'border-slate-100 bg-white'}`}>
                            <input type="checkbox" checked={m.visible} onChange={() => setMetrics(prev => prev.map(p => p.key === m.key ? { ...p, visible: !p.visible } : p))} className="w-4 h-4 rounded border-slate-300 text-indigo-600" />
                            <span className="text-xs font-bold text-slate-700">{m.label}</span>
                          </label>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
              <div className="w-[280px] bg-indigo-50/20 rounded-2xl border border-indigo-100 p-5 flex flex-col overflow-y-auto">
                <span className="text-[10px] font-black uppercase tracking-widest text-indigo-600 mb-4 block">Selected Order</span>
                {metrics.filter(m => {
                  if (!m.visible) return false;
                  const revenueKeys = ['revenue', 'profit', 'epa', 'epc', 'epv', 'roi', 'm_epc', 'm_epv', 'm_epa'];
                  return !(currentUser.showRevenue === false && revenueKeys.includes(m.key));
                }).map((m) => (
                  <div key={m.key} draggable onDragStart={() => dragItemKey.current = m.key} onDragEnter={() => dragOverItemKey.current = m.key} onDragEnd={handleMetricReorder} onDragOver={e => e.preventDefault()} className="p-3 bg-white border border-indigo-200 rounded-xl flex items-center gap-3 mb-2 cursor-grab">
                    <i className="fas fa-grip-vertical text-indigo-300 text-xs"></i>
                    <span className="text-xs font-bold text-slate-700 truncate flex-1">{m.label}</span>
                  </div>
                ))}
              </div>
            </div>
            <button onClick={() => setShowColumnEditor(false)} className="mt-8 py-5 bg-slate-900 text-white rounded-2xl font-black text-sm uppercase tracking-widest">Apply Layout</button>
          </div>
        </div>
      )}
    </div>
  );
};

export default App;
