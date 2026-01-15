/**
 * Daily Report Component
 *
 * Displays daily report data with date → media hierarchy.
 * Used for correcting data from the general report.
 * Only supports date and media dimensions.
 */

import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { AdRow, MetricConfig, UserPermission } from '../types';

// Metric configuration
const DAILY_METRICS: MetricConfig[] = [
  { key: 'spend', label: 'Spend', visible: true, type: 'money', group: 'Basic' },
  { key: 'revenue', label: 'Revenue', visible: true, type: 'money', group: 'Basic' },
  { key: 'profit', label: 'Profit', visible: true, type: 'profit' as const, group: 'Basic' },
  { key: 'roi', label: 'ROI', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'conversions', label: 'Conversions', visible: true, type: 'number', group: 'Basic' },
  { key: 'cpa', label: 'CPA', visible: true, type: 'money', group: 'Calculated' },
  { key: 'epa', label: 'EPA', visible: true, type: 'money', group: 'Calculated' },
  { key: 'epc', label: 'EPC', visible: true, type: 'money', group: 'Calculated' },
  { key: 'epv', label: 'EPV', visible: true, type: 'money', group: 'Calculated' },
  { key: 'ctr', label: 'CTR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'cvr', label: 'CVR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'impressions', label: 'Impressions', visible: true, type: 'number', group: 'Basic' },
  { key: 'clicks', label: 'Clicks', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_imp', label: 'm_imp', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_clicks', label: 'm_clicks', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_conv', label: 'm_conv', visible: true, type: 'number', group: 'Basic' },
];

// Metric value display component
const MetricValue: React.FC<{
  value: number;
  type: 'money' | 'percent' | 'number' | 'profit';
  isSub?: boolean;
  colorMode?: boolean;
  metricKey?: string;
  isManualEdited?: boolean;
}> = ({ value, type, isSub, colorMode, metricKey, isManualEdited }) => {
  const displayValue = isFinite(value) ? value : 0;

  // 手动编辑的值使用琥珀色
  if (isManualEdited) {
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    if (type === 'money' || type === 'profit') return <span className={`font-mono tracking-tight leading-none font-bold text-amber-600 ${sizeClass}`}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
    if (type === 'percent') return <span className={`font-mono tracking-tight leading-none font-bold text-amber-600 ${sizeClass}`}>{(displayValue * 100).toFixed(2)}%</span>;
    return <span className={`font-mono tracking-tight leading-none font-bold text-amber-600 ${sizeClass}`}>{Math.floor(displayValue).toLocaleString()}</span>;
  }

  if (type === 'profit') {
    const colorClass = displayValue > 0 ? 'text-emerald-600' : displayValue < 0 ? 'text-rose-600' : 'text-slate-800';
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    return <span className={`font-mono tracking-tight leading-none font-bold ${colorClass} ${sizeClass}`}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
  }

  if (metricKey === 'roi') {
    const colorClass = displayValue > 0 ? 'text-emerald-600' : displayValue < 0 ? 'text-rose-600' : 'text-slate-800';
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    return <span className={`font-mono tracking-tight leading-none font-bold ${colorClass} ${sizeClass}`}>{(displayValue * 100).toFixed(2)}%</span>;
  }

  let colorClasses = '';
  if (colorMode && !isSub) {
    if (metricKey === 'revenue') colorClasses = 'text-amber-500';
    else if (metricKey === 'spend') colorClasses = 'text-rose-500';
    else if (metricKey === 'cpa') colorClasses = 'text-blue-500';
    else if (metricKey === 'epa') colorClasses = 'text-amber-500';
    else if (metricKey === 'epc') colorClasses = 'text-amber-500';
    else if (metricKey === 'epv') colorClasses = 'text-amber-500';
  }

  const baseClasses = `font-mono tracking-tight leading-none ${isSub ? 'text-[13px] text-slate-500 font-medium' : `text-[14px] ${colorClasses} font-bold`}`;

  if (type === 'money' || type === 'profit') return <span className={baseClasses}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
  if (type === 'percent') return <span className={baseClasses}>{(displayValue * 100).toFixed(2)}%</span>;
  return <span className={baseClasses}>{Math.floor(displayValue).toLocaleString()}</span>;
};

// Date utilities
const formatDate = (date: Date) => {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${months[date.getMonth()]} ${date.getDate()}, ${date.getFullYear()}`;
};

const formatRange = (start: Date, end: Date) => {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  if (start.toDateString() === end.toDateString()) {
    return `${months[start.getMonth()]} ${start.getDate()}, ${start.getFullYear()}`;
  }
  if (start.getFullYear() === end.getFullYear()) {
    if (start.getMonth() === end.getMonth()) {
      return `${months[start.getMonth()]} ${start.getDate()} - ${end.getDate()}, ${start.getFullYear()}`;
    }
    return `${months[start.getMonth()]} ${start.getDate()} - ${months[end.getMonth()]} ${end.getDate()}, ${start.getFullYear()}`;
  }
  return `${months[start.getMonth()]} ${start.getDate()}, ${start.getFullYear()} - ${months[end.getMonth()]} ${end.getDate()}, ${end.getFullYear()}`;
};

const getRangeInfo = (range: string, customStart?: Date, customEnd?: Date) => {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  const sevenDaysAgo = new Date(today);
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
  // This month: from 1st to today
  const thisMonthStart = new Date(today.getFullYear(), today.getMonth(), 1);

  switch (range) {
    case 'Today':
      return { label: 'Today', dateString: formatDate(today), start: today, end: today };
    case 'Yesterday':
      return { label: 'Yesterday', dateString: formatDate(yesterday), start: yesterday, end: yesterday };
    case 'Last 7 Days':
      return { label: 'Last 7 Days', dateString: formatRange(sevenDaysAgo, today), start: sevenDaysAgo, end: today };
    case 'This Month':
      return { label: 'This Month', dateString: formatRange(thisMonthStart, today), start: thisMonthStart, end: today };
    case 'Custom':
      if (customStart && customEnd) {
        return { label: 'Custom', dateString: formatRange(customStart, customEnd), start: customStart, end: customEnd };
      }
      return { label: 'Custom', dateString: 'Select dates...', start: today, end: today };
    default:
      return { label: 'Yesterday', dateString: formatDate(yesterday), start: yesterday, end: yesterday };
  }
};

const DatePicker: React.FC<{
  onRangeChange: (range: string, start?: Date, end?: Date) => void;
  currentDisplay: string;
  currentRange: string;
}> = ({ onRangeChange, currentDisplay, currentRange }) => {
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    const clickOutside = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      if (!target.closest('.date-picker-dropdown')) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', clickOutside);
    return () => document.removeEventListener('mousedown', clickOutside);
  }, []);

  const handleQuickSelect = (r: string) => {
    const info = getRangeInfo(r);
    onRangeChange(r, info.start, info.end);
    setIsOpen(false);
  };

  return (
    <div className="relative date-picker-dropdown">
      <button onClick={() => setIsOpen(!isOpen)} className="flex items-center gap-3 px-4 py-2 bg-white border border-slate-200 rounded-xl text-xs font-bold text-slate-600 hover:bg-slate-50 transition-all shadow-sm active:scale-95">
        <i className="far fa-calendar text-indigo-500"></i>
        <span>{currentDisplay}</span>
        <i className={`fas fa-chevron-down text-[10px] text-slate-400 transition-transform ${isOpen ? 'rotate-180' : ''}`}></i>
      </button>
      {isOpen && (
        <div className="absolute top-full mt-2 left-0 bg-white rounded-2xl shadow-2xl border border-slate-100 z-[9999] overflow-hidden">
          <div className="p-2 w-48">
            <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2 px-1">Quick Select</div>
            {['Today', 'Yesterday', 'Last 7 Days', 'This Month'].map(r => (
              <button key={r} onClick={() => handleQuickSelect(r)} className={`w-full text-left px-3 py-2 text-[11px] font-bold rounded-xl ${currentRange === r ? 'bg-indigo-100 text-indigo-700' : 'text-slate-600 hover:bg-indigo-50'}`}>
                {r}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

interface DailyReportProps {
  selectedRange: string;
  customDateStart?: Date;
  customDateEnd?: Date;
  onRangeChange: (range: string, start?: Date, end?: Date) => void;
  currentUser: UserPermission;
}

interface EditingSpendState {
  date: string;
  media: string;
  rowId: string;
}

interface DailyDataNode {
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
    spend_manual?: number;  // 手动修正值（非零表示已编辑）
  };
  _children?: Record<string, DailyDataNode>;
}

interface DailyHierarchyResponse {
  dimensions: string[];
  hierarchy: Record<string, DailyDataNode>;
  startDate: string;
  endDate: string;
}

type HierarchyOrder = 'date-media' | 'media-date';

interface LockConfirmState {
  date: string;
  displayName: string;
  currentLocked: boolean;
}

interface SyncModalState {
  isOpen: boolean;
  startDate: Date;
  endDate: Date;
}

// Date Badge Component
const DateBadge: React.FC<{
  dateValue: string;
  isLocked: boolean;
  onClick: (date: string) => void;
}> = ({ dateValue, isLocked, onClick }) => {
  return (
    <button
      type="button"
      onClick={(e) => {
        e.stopPropagation();
        console.log('Date badge clicked:', dateValue, 'locked:', isLocked);
        onClick(dateValue);
      }}
      className={`text-[9px] px-1.5 py-0.5 rounded flex items-center gap-1 transition-colors ${
        isLocked
          ? 'text-emerald-700 bg-emerald-100 hover:bg-emerald-200 cursor-pointer'
          : 'text-slate-400 bg-slate-100 hover:bg-slate-200 cursor-pointer'
      }`}
      title={isLocked ? '点击解锁 (数据已锁定)' : '点击锁定 (防止自动同步覆盖)'}
    >
      {isLocked && <i className="fas fa-lock text-[8px]"></i>}
      Date
    </button>
  );
};

const DailyReport: React.FC<DailyReportProps> = ({
  selectedRange,
  customDateStart,
  customDateEnd,
  onRangeChange,
  currentUser,
}) => {
  const [hierarchy, setHierarchy] = useState<DailyHierarchyResponse | null>(null);
  const [summary, setSummary] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [colorMode, setColorMode] = useState(false);
  const [metrics, setMetrics] = useState<MetricConfig[]>(DAILY_METRICS);
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const [editingSpend, setEditingSpend] = useState<EditingSpendState | null>(null);
  const [spendInputValue, setSpendInputValue] = useState('');
  const [quickFilterText, setQuickFilterText] = useState('');
  const [hierarchyOrder, setHierarchyOrder] = useState<HierarchyOrder>('date-media');
  const [lockedDates, setLockedDates] = useState<Set<string>>(new Set());
  const [lockConfirm, setLockConfirm] = useState<LockConfirmState | null>(null);
  const [lockingInProgress, setLockingInProgress] = useState(false);
  const [syncModal, setSyncModal] = useState<SyncModalState>({
    isOpen: false,
    startDate: new Date(),
    endDate: new Date()
  });
  const [syncInProgress, setSyncInProgress] = useState(false);
  const [syncResult, setSyncResult] = useState<string | null>(null);
  const [syncStatus, setSyncStatus] = useState<{ last_update: string | null }>({ last_update: null });
  const spendInputRef = React.useRef<HTMLInputElement>(null);

  const rangeInfo = useMemo(() => getRangeInfo(selectedRange, customDateStart, customDateEnd), [selectedRange, customDateStart, customDateEnd]);

  const formatDateForApi = (date: Date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };

  const loadHierarchy = async (startDate: string, endDate: string) => {
    const token = localStorage.getItem('addata_access_token');
    const response = await fetch(`/api/daily-report/hierarchy?start_date=${startDate}&end_date=${endDate}`, {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    if (!response.ok) {
      throw new Error('Failed to load hierarchy');
    }
    return await response.json() as DailyHierarchyResponse;
  };

  const loadSummary = async (startDate: string, endDate: string) => {
    const token = localStorage.getItem('addata_access_token');
    const response = await fetch(`/api/daily-report/summary?start_date=${startDate}&end_date=${endDate}`, {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    if (!response.ok) {
      throw new Error('Failed to load summary');
    }
    return await response.json();
  };

  const loadLockedDates = async () => {
    const token = localStorage.getItem('addata_access_token');
    try {
      const response = await fetch('/api/daily-report/locked-dates', {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      if (response.ok) {
        const data = await response.json();
        setLockedDates(new Set(data.locked_dates || []));
      }
    } catch (error) {
      console.error('Error loading locked dates:', error);
    }
  };

  const loadSyncStatus = async () => {
    const token = localStorage.getItem('addata_access_token');
    try {
      const response = await fetch('/api/daily-report/sync-status', {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      if (response.ok) {
        const data = await response.json();
        setSyncStatus(data);
      }
    } catch (error) {
      console.error('Error loading sync status:', error);
    }
  };

  const handleDateBadgeClick = (dateValue: string, displayName: string) => {
    const isLocked = lockedDates.has(dateValue);
    setLockConfirm({ date: dateValue, displayName, currentLocked: isLocked });
  };

  const confirmLock = async () => {
    if (!lockConfirm) return;
    setLockingInProgress(true);

    const token = localStorage.getItem('addata_access_token');
    const newLockState = !lockConfirm.currentLocked;

    try {
      const response = await fetch('/api/daily-report/lock-date', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          date: lockConfirm.date,
          lock: newLockState
        })
      });

      if (response.ok) {
        // Update locked dates state
        setLockedDates(prev => {
          const newSet = new Set(prev);
          if (newLockState) {
            newSet.add(lockConfirm.date);
          } else {
            newSet.delete(lockConfirm.date);
          }
          return newSet;
        });
      }
    } catch (error) {
      console.error('Error toggling lock:', error);
    } finally {
      setLockingInProgress(false);
      setLockConfirm(null);
    }
  };

  const handleSyncData = async () => {
    setSyncInProgress(true);
    setSyncResult(null);

    const token = localStorage.getItem('addata_access_token');
    const startDate = formatDateForApi(syncModal.startDate);
    const endDate = formatDateForApi(syncModal.endDate);

    try {
      const response = await fetch('/api/daily-report/sync', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          start_date: startDate,
          end_date: endDate
        })
      });

      if (response.ok) {
        const data = await response.json();
        setSyncResult(`成功同步 ${data.row_count} 条数据 (${startDate} ~ ${endDate})`);
        // Reload data after sync
        await loadData();
        // Reload locked dates
        await loadLockedDates();
        // Reload sync status
        await loadSyncStatus();
      } else {
        const error = await response.json();
        setSyncResult(`同步失败: ${error.detail || 'Unknown error'}`);
      }
    } catch (error) {
      console.error('Error syncing data:', error);
      setSyncResult(`同步失败: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setSyncInProgress(false);
    }
  };

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const startDate = formatDateForApi(rangeInfo.start);
      const endDate = formatDateForApi(rangeInfo.end);

      const [hierarchyData, summaryData] = await Promise.all([
        loadHierarchy(startDate, endDate),
        loadSummary(startDate, endDate)
      ]);

      setHierarchy(hierarchyData);
      setSummary(summaryData);
    } catch (error) {
      console.error('Error loading Daily Report data:', error);
    } finally {
      setLoading(false);
    }
  }, [rangeInfo]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    loadLockedDates();
    loadSyncStatus();
  }, []);

  useEffect(() => {
    if (editingSpend && spendInputRef.current) {
      spendInputRef.current.focus();
      spendInputRef.current.select();
    }
  }, [editingSpend]);

  const visibleMetrics = useMemo(() => metrics.filter(m => m.visible), [metrics]);

  // Convert hierarchy node to AdRow
  const hierarchyNodeToAdRow = (
    name: string,
    node: DailyDataNode,
    level: number,
    parentId: string | null = null
  ): AdRow => {
    // Defensive check for node structure
    if (!node || !node._metrics) {
      console.error('Invalid node structure:', { name, node, level });
      return {
        id: parentId ? `${parentId}|${name}` : name,
        name,
        level,
        dimensionType: 'unknown' as any,
        impressions: 0,
        clicks: 0,
        conversions: 0,
        spend: 0,
        revenue: 0,
        profit: 0,
        m_imp: 0,
        m_clicks: 0,
        m_conv: 0,
        ctr: 0,
        cvr: 0,
        roi: 0,
        cpa: 0,
        rpa: 0,
        epa: 0,
        epc: 0,
        epv: 0,
        m_epc: 0,
        m_epv: 0,
        m_cpc: 0,
        m_cpv: 0,
        hasChild: false,
        isExpanded: false,
        children: [],
        filterPath: [{ dimension: 'unknown', value: name }],
      };
    }

    const metrics = node._metrics;
    const uniqueId = parentId ? `${parentId}|${name}` : name;

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
      epa: metrics.rpa,
      epc: metrics.epc,
      epv: metrics.epv,
      m_epc: metrics.m_epc,
      m_epv: metrics.m_epv,
      m_cpc: metrics.m_cpc,
      m_cpv: metrics.m_cpv,
      hasChild: !!node._children && Object.keys(node._children).length > 0,
      isExpanded: false,
      children: [],
      filterPath: [{ dimension: node._dimension, value: name }],
      spend_manual: metrics.spend_manual ?? 0,
    };
  };

  // Get flat list of rows from hierarchy
  const getFlatRows = (): AdRow[] => {
    if (!hierarchy) return [];

    const rows: AdRow[] = [];

    if (hierarchyOrder === 'date-media') {
      // Date → Media: date as parent, media as children
      // Sort dates by date DESC
      const sortedDates = Object.entries(hierarchy.hierarchy).sort((a, b) => b[0].localeCompare(a[0]));

      for (const [date, dateNode] of sortedDates) {
        const dateRow = hierarchyNodeToAdRow(date, dateNode, 0, null);
        const isExpanded = expandedRows.has(dateRow.id);
        dateRow.isExpanded = isExpanded;

        rows.push(dateRow);

        // Level 1: Media level (children of date) - sort by revenue DESC, filter out zero revenue
        if (isExpanded && dateNode._children) {
          const sortedMedia = Object.entries(dateNode._children)
            .filter(([_, mediaNode]) => mediaNode._metrics.revenue > 0)
            .sort((a, b) => b[1]._metrics.revenue - a[1]._metrics.revenue);
          for (const [media, mediaNode] of sortedMedia) {
            const mediaRow = hierarchyNodeToAdRow(media, mediaNode, 1, dateRow.id);
            mediaRow.filterPath = [
              { dimension: 'date', value: date },
              { dimension: 'media', value: media }
            ];
            rows.push(mediaRow);
          }
        }
      }
    } else {
      // Media → Date: Transform date->media to media->date
      // Build a media->date hierarchy from the date->media data
      const mediaHierarchy: Record<string, DailyDataNode> = {};

      for (const [date, dateNode] of Object.entries(hierarchy.hierarchy)) {
        if (!dateNode._children) continue;

        for (const [media, mediaNode] of Object.entries(dateNode._children)) {
          // Skip media with zero revenue
          if (mediaNode._metrics.revenue <= 0) continue;
          // Create media level node if not exists
          if (!mediaHierarchy[media]) {
            mediaHierarchy[media] = {
              _dimension: 'media',
              _metrics: {
                impressions: 0,
                clicks: 0,
                conversions: 0,
                spend: 0,
                revenue: 0,
                profit: 0,
                m_imp: 0,
                m_clicks: 0,
                m_conv: 0,
                ctr: 0,
                cvr: 0,
                roi: 0,
                cpa: 0,
                rpa: 0,
                epc: 0,
                epv: 0,
                m_epc: 0,
                m_epv: 0,
                m_cpc: 0,
                m_cpv: 0,
              },
              _children: {}
            };
          }

          // Accumulate media level metrics
          const mNode = mediaHierarchy[media]._metrics!;
          mNode.impressions += mediaNode._metrics.impressions;
          mNode.clicks += mediaNode._metrics.clicks;
          mNode.conversions += mediaNode._metrics.conversions;
          mNode.spend += mediaNode._metrics.spend;
          mNode.revenue += mediaNode._metrics.revenue;
          mNode.profit += mediaNode._metrics.profit;
          mNode.m_imp += mediaNode._metrics.m_imp;
          mNode.m_clicks += mediaNode._metrics.m_clicks;
          mNode.m_conv += mediaNode._metrics.m_conv;

          // Add date as child of media (create a proper date node)
          mediaHierarchy[media]._children![date] = {
            _dimension: 'date',
            _metrics: { ...mediaNode._metrics },
            _children: undefined
          };
        }
      }

      // Calculate derived metrics for media level
      for (const mediaNode of Object.values(mediaHierarchy)) {
        const m = mediaNode._metrics;
        m.profit = m.revenue - m.spend;
        m.ctr = m.clicks / (m.impressions || 1);
        m.cvr = m.conversions / (m.clicks || 1);
        m.roi = m.spend > 0 ? (m.revenue - m.spend) / m.spend : 0;
        m.cpa = m.conversions > 0 ? m.spend / m.conversions : 0;
        m.rpa = m.conversions > 0 ? m.revenue / m.conversions : 0;
        m.epa = m.conversions > 0 ? m.profit / m.conversions : 0;
        m.epc = m.clicks > 0 ? m.revenue / m.clicks : 0;
        m.epv = m.impressions > 0 ? m.revenue / m.impressions : 0;
        m.m_epc = m.m_clicks > 0 ? m.revenue / m.m_clicks : 0;
        m.m_epv = m.m_imp > 0 ? m.revenue / m.m_imp : 0;
        m.m_cpc = m.m_clicks > 0 ? m.spend / m.m_clicks : 0;
        m.m_cpv = m.m_imp > 0 ? m.spend / m.m_imp : 0;
      }

      // Sort media by revenue DESC
      const sortedMediaNames = Object.entries(mediaHierarchy).sort((a, b) => b[1]._metrics.revenue - a[1]._metrics.revenue);

      // Level 0: Media level
      for (const [media, mediaNode] of sortedMediaNames) {
        const mediaRow = hierarchyNodeToAdRow(media, mediaNode, 0, null);
        const isExpanded = expandedRows.has(mediaRow.id);
        mediaRow.isExpanded = isExpanded;

        rows.push(mediaRow);

        // Level 1: Date level (children of media) - sort by date DESC
        if (isExpanded && mediaNode._children) {
          const sortedDates = Object.entries(mediaNode._children).sort((a, b) => b[0].localeCompare(a[0]));
          for (const [date, dateNode] of sortedDates) {
            const dateRow = hierarchyNodeToAdRow(date, dateNode, 1, mediaRow.id);
            dateRow.filterPath = [
              { dimension: 'media', value: media },
              { dimension: 'date', value: date }
            ];
            rows.push(dateRow);
          }
        }
      }
    }

    return rows;
  };

  const filteredRows = useMemo(() => {
    const rows = getFlatRows();
    if (!quickFilterText) return rows;
    const lowerText = quickFilterText.toLowerCase();
    return rows.filter(row =>
      row.name.toLowerCase().includes(lowerText) ||
      String(row.spend).includes(lowerText) ||
      String(row.revenue).includes(lowerText)
    );
  }, [hierarchy, expandedRows, quickFilterText, hierarchyOrder]);

  // Toggle row expansion
  const toggleRowExpansion = (rowId: string) => {
    const newExpanded = new Set(expandedRows);
    if (newExpanded.has(rowId)) {
      newExpanded.delete(rowId);
    } else {
      newExpanded.add(rowId);
    }
    setExpandedRows(newExpanded);
  };

  // Handle spend click (only for rows with both date and media in filterPath)
  const handleSpendClick = (row: AdRow) => {
    // Spend can only be edited when we have both date and media information
    const date = row.filterPath?.find(f => f.dimension === 'date')?.value;
    const media = row.filterPath?.find(f => f.dimension === 'media')?.value;

    // In date-media mode: level 1 is media (has both date and media)
    // In media-date mode: level 1 is date (has both media and date)
    // Check if filterPath has both dimensions
    const hasBothDimensions = row.filterPath?.length === 2 &&
      row.filterPath.some(f => f.dimension === 'date') &&
      row.filterPath.some(f => f.dimension === 'media');

    if (hasBothDimensions && date && media) {
      setEditingSpend({ date, media, rowId: row.id });
      setSpendInputValue(row.spend.toFixed(2));
    }
  };

  const handleSpendSave = async () => {
    if (!editingSpend || !hierarchy) return;

    const newValue = parseFloat(spendInputValue);
    if (isNaN(newValue)) {
      setEditingSpend(null);
      return;
    }

    try {
      const token = localStorage.getItem('addata_access_token');
      const response = await fetch('/api/daily-report/update-spend', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          date: editingSpend.date,
          media: editingSpend.media,
          spend_value: newValue
        })
      });

      if (!response.ok) {
        console.error('Failed to update spend');
        return;
      }

      // Update local state instead of reloading
      setHierarchy(prevHierarchy => {
        if (!prevHierarchy) return prevHierarchy;

        const newHierarchy = { ...prevHierarchy };
        const dateNode = newHierarchy.hierarchy[editingSpend.date];

        if (dateNode && dateNode._children && dateNode._children[editingSpend.media]) {
          const mediaNode = dateNode._children[editingSpend.media];
          const oldSpend = mediaNode._metrics.spend;
          const spendDiff = newValue - oldSpend;

          // Update media node spend
          mediaNode._metrics = { ...mediaNode._metrics };
          mediaNode._metrics.spend = newValue;
          mediaNode._metrics.profit = mediaNode._metrics.revenue - newValue;
          // Mark as manually edited (累加差值到 spend_manual)
          mediaNode._metrics.spend_manual = (mediaNode._metrics.spend_manual || 0) + spendDiff;

          // Recalculate derived metrics for media node
          const m = mediaNode._metrics;
          m.roi = newValue > 0 ? (m.revenue - newValue) / newValue : 0;
          m.cpa = m.conversions > 0 ? newValue / m.conversions : 0;
          m.epa = m.conversions > 0 ? m.profit / m.conversions : 0;
          m.m_cpc = m.m_clicks > 0 ? newValue / m.m_clicks : 0;
          m.m_cpv = m.m_imp > 0 ? newValue / m.m_imp : 0;

          // Update date node aggregate metrics
          dateNode._metrics = { ...dateNode._metrics };
          dateNode._metrics.spend += spendDiff;
          dateNode._metrics.profit = dateNode._metrics.revenue - dateNode._metrics.spend;

          // Recalculate derived metrics for date node
          const dm = dateNode._metrics;
          dm.roi = dm.spend > 0 ? (dm.revenue - dm.spend) / dm.spend : 0;
          dm.cpa = dm.conversions > 0 ? dm.spend / dm.conversions : 0;
          dm.epa = dm.conversions > 0 ? dm.profit / dm.conversions : 0;
          dm.m_cpc = dm.m_clicks > 0 ? dm.spend / dm.m_clicks : 0;
          dm.m_cpv = dm.m_imp > 0 ? dm.spend / dm.m_imp : 0;

          // Update summary
          setSummary(prevSummary => {
            if (!prevSummary) return prevSummary;
            return {
              ...prevSummary,
              spend: (prevSummary.spend || 0) + spendDiff,
              profit: (prevSummary.profit || 0) - spendDiff,
              roi: (prevSummary.spend || 0) + spendDiff > 0
                ? ((prevSummary.revenue || 0) - ((prevSummary.spend || 0) + spendDiff)) / ((prevSummary.spend || 0) + spendDiff)
                : 0,
            };
          });
        }

        return newHierarchy;
      });
    } catch (error) {
      console.error('Error updating spend:', error);
    } finally {
      setEditingSpend(null);
    }
  };

  const handleSpendKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSpendSave();
    } else if (e.key === 'Escape') {
      setEditingSpend(null);
    }
  };

  return (
    <div className="flex-1 flex flex-col bg-white min-h-0">
      {/* Header */}
      <div className="px-8 py-5 bg-white border-b border-slate-200 flex flex-col gap-4 z-30 shadow-sm shrink-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h1 className="text-xl font-black text-slate-800 tracking-tight">Daily Report</h1>
          </div>
          <div className="flex gap-2 items-center">
            <DatePicker
              onRangeChange={onRangeChange}
              currentDisplay={rangeInfo.dateString}
              currentRange={selectedRange}
            />
          </div>
        </div>

        <div className="flex justify-between items-center">
          <div className="flex flex-col gap-2 items-start">
            <button
              onClick={() => setHierarchyOrder(hierarchyOrder === 'date-media' ? 'media-date' : 'date-media')}
              className="text-[15px] font-bold text-indigo-600 bg-indigo-50 hover:bg-indigo-100 px-3 py-1.5 rounded-md transition-colors cursor-pointer flex items-center gap-1"
              title="Click to switch hierarchy"
            >
              {hierarchyOrder === 'date-media' ? 'Date ⇄ Media' : 'Media ⇄ Date'}
            </button>
            <button
              onClick={() => setSyncModal({ ...syncModal, isOpen: true })}
              className="text-[11px] font-bold text-emerald-600 bg-emerald-50 hover:bg-emerald-100 px-3 py-1 rounded-md transition-colors cursor-pointer flex items-center gap-1"
              title="从 Performance 表同步数据"
            >
              <i className="fas fa-sync-alt text-[10px]"></i>
              同步数据
            </button>
            {syncStatus.last_update && (
              <span className="text-[10px] font-medium text-slate-400 bg-slate-50 px-2 py-1 rounded-md">
                Update {syncStatus.last_update}
              </span>
            )}
          </div>
          <div className="flex items-center gap-3">
            <label className="flex items-center gap-2 px-3 py-1 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors">
              <input type="checkbox" checked={colorMode} onChange={(e) => setColorMode(e.target.checked)} className="w-3 h-3 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" />
              <span className="text-[10px] font-bold text-slate-600">Color Mode</span>
            </label>
            <div className="flex items-center gap-2">
              <i className="fas fa-search text-slate-400 text-xs"></i>
              <input
                type="text"
                placeholder="Quick filter..."
                value={quickFilterText}
                onChange={(e) => setQuickFilterText(e.target.value)}
                className="w-40 px-3 py-1.5 bg-slate-50 border border-slate-200 rounded-xl text-[11px] font-medium text-slate-600 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
              />
            </div>
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto px-8 py-4">
        {loading ? (
          <div className="flex items-center justify-center h-64">
            <div className="flex flex-col items-center gap-3">
              <div className="w-10 h-10 border-3 border-indigo-200 border-t-indigo-600 rounded-full animate-spin"></div>
              <span className="text-xs font-bold text-slate-400">Loading data...</span>
            </div>
          </div>
        ) : filteredRows.length === 0 ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <i className="fas fa-folder-open text-3xl text-slate-300 mb-3"></i>
              <p className="text-sm font-bold text-slate-400">No data found</p>
            </div>
          </div>
        ) : (
          <table ref={spendInputRef} className="w-full border-collapse">
            <thead className="sticky top-0 bg-white z-20 shadow-sm">
              {/* Summary Row */}
              {summary && (
                <tr className="bg-gradient-to-r from-slate-50 to-slate-100">
                  <th className="left-0 bg-gradient-to-r from-slate-50 to-slate-100 z-30 px-3 py-2 text-left border-b border-slate-200 min-w-[300px]">
                    <div className="flex items-center gap-2">
                      <i className="fas fa-calculator text-indigo-500 text-xs"></i>
                      <span className="text-[10px] font-black text-slate-500 uppercase tracking-wider">Summary</span>
                    </div>
                  </th>
                  {visibleMetrics.map(metric => {
                    const config = DAILY_METRICS.find(m => m.key === metric.key);
                    if (!config) return null;
                    const value = summary[metric.key] || 0;
                    return (
                      <th key={metric.key} className="px-3 py-2 text-right border-b border-slate-200 whitespace-nowrap">
                        <MetricValue value={value} type={config.type} metricKey={config.key as any} />
                      </th>
                    );
                  })}
                </tr>
              )}
              {/* Header Row */}
              <tr>
                <th className="left-0 bg-white z-30 px-3 py-2 text-left text-[10px] font-black text-slate-400 uppercase tracking-wider border-b border-slate-200 min-w-[300px]">
                  Dimension
                </th>
                {visibleMetrics.map(metric => (
                  <th key={metric.key} className="px-3 py-2 text-right text-[10px] font-black text-slate-400 uppercase tracking-wider border-b border-slate-200 whitespace-nowrap">
                    {metric.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredRows.map(row => {
                const isEditingSpend = editingSpend?.rowId === row.id;
                // Check if this row has both date and media info (editable spend)
                const hasBothDimensions = row.filterPath?.length === 2 &&
                  row.filterPath.some(f => f.dimension === 'date') &&
                  row.filterPath.some(f => f.dimension === 'media');
                const canEditSpend = hasBothDimensions;

                return (
                  <tr
                    key={row.id}
                    className={`group hover:bg-slate-50 transition-colors ${row.level === 1 ? 'bg-slate-50/50' : ''}`}
                  >
                    <td className={`left-0 bg-white z-10 px-3 py-2 border-b border-slate-200 font-medium text-slate-700 text-xs transition-colors ${row.level === 0 ? 'font-bold' : ''} ${row.level === 1 ? 'text-indigo-600 group-hover:!text-purple-600' : 'group-hover:text-purple-600'} min-w-[300px]`}>
                      <div className={`flex items-center gap-2 ${row.level === 1 ? 'justify-between w-full' : ''}`}>
                        <div className="flex items-center gap-2 flex-1">
                          {row.hasChild && (
                            <button
                              onClick={() => toggleRowExpansion(row.id)}
                              className="w-5 h-5 flex items-center justify-center rounded hover:bg-slate-200 transition-colors"
                            >
                              <i className={`fas fa-chevron-right text-[10px] text-slate-400 transition-transform ${row.isExpanded ? 'rotate-90' : ''}`}></i>
                            </button>
                          )}
                          {!row.hasChild && <span className="w-5"></span>}
                          <span className={row.level === 1 ? 'flex-1' : ''}>{row.name}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          {row.dimensionType === 'media' && (
                            <span className="text-[9px] text-slate-400 bg-slate-100 px-1.5 py-0.5 rounded">Media</span>
                          )}
                          {row.dimensionType === 'date' && (
                            <DateBadge
                              dateValue={row.name}
                              isLocked={lockedDates.has(row.name)}
                              onClick={handleDateBadgeClick}
                            />
                          )}
                        </div>
                      </div>
                    </td>
                    {visibleMetrics.map(metric => {
                      const config = DAILY_METRICS.find(m => m.key === metric.key);
                      if (!config) return null;
                      const value = row[metric.key as keyof AdRow] as number || 0;

                      if (metric.key === 'spend' && canEditSpend) {
                        const isManualEdited = row.spend_manual !== undefined && row.spend_manual !== 0;
                        return (
                          <td key={metric.key} className="px-3 py-2 text-right border-b border-slate-200 group-hover:text-purple-600 [&_*]:group-hover:!text-purple-600 transition-colors">
                            {isEditingSpend ? (
                              <input
                                ref={spendInputRef}
                                type="number"
                                step="0.01"
                                value={spendInputValue}
                                onChange={(e) => setSpendInputValue(e.target.value)}
                                onBlur={handleSpendSave}
                                onKeyDown={handleSpendKeyDown}
                                className="w-24 px-2 py-1 text-right text-xs font-mono border border-indigo-300 rounded focus:outline-none focus:ring-2 focus:ring-indigo-500"
                              />
                            ) : (
                              <button
                                onClick={() => handleSpendClick(row)}
                                className={`transition-colors cursor-pointer ${isManualEdited ? 'border-b border-dashed border-amber-500 text-amber-600 font-bold' : 'hover:text-indigo-600'}`}
                                title={isManualEdited ? "已手动编辑 (点击修改)" : "Click to edit spend"}
                              >
                                <MetricValue value={value} type={config.type} metricKey={config.key as any} isManualEdited={isManualEdited} />
                              </button>
                            )}
                          </td>
                        );
                      }

                      return (
                        <td key={metric.key} className="px-3 py-2 text-right border-b border-slate-200 group-hover:text-purple-600 [&_*]:group-hover:!text-purple-600 transition-colors">
                          <MetricValue value={value} type={config.type} isSub={row.level === 1} colorMode={colorMode} metricKey={config.key as any} />
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Lock Confirmation Modal */}
      {lockConfirm && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-[9999]">
          <div className="bg-white rounded-2xl shadow-2xl p-6 w-96">
            <h3 className="text-lg font-bold text-slate-800 mb-2">
              {lockConfirm.currentLocked ? '解锁日期' : '锁定日期'}
            </h3>
            <p className="text-sm text-slate-600 mb-4">
              {lockConfirm.currentLocked
                ? `确定要解锁 ${lockConfirm.displayName} 吗？解锁后，该日期的数据可能会被自动同步覆盖。`
                : `确定要锁定 ${lockConfirm.displayName} 吗？锁定后，该日期的数据将以手动修正为准，不会被自动同步覆盖。`
              }
            </p>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setLockConfirm(null)}
                disabled={lockingInProgress}
                className="px-4 py-2 text-sm font-bold text-slate-600 bg-slate-100 rounded-xl hover:bg-slate-200 transition-colors disabled:opacity-50"
              >
                取消
              </button>
              <button
                onClick={confirmLock}
                disabled={lockingInProgress}
                className={`px-4 py-2 text-sm font-bold text-white rounded-xl transition-colors disabled:opacity-50 ${
                  lockConfirm.currentLocked
                    ? 'bg-amber-500 hover:bg-amber-600'
                    : 'bg-emerald-500 hover:bg-emerald-600'
                }`}
              >
                {lockingInProgress
                  ? '处理中...'
                  : lockConfirm.currentLocked
                    ? '解锁'
                    : '锁定'
                }
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Sync Data Modal */}
      {syncModal.isOpen && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-[9999]">
          <div className="bg-white rounded-2xl shadow-2xl p-6 w-[480px]">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-bold text-slate-800">从 Performance 同步数据</h3>
              <button
                onClick={() => {
                  setSyncModal({ ...syncModal, isOpen: false });
                  setSyncResult(null);
                }}
                className="text-slate-400 hover:text-slate-600 transition-colors"
              >
                <i className="fas fa-times"></i>
              </button>
            </div>

            <div className="space-y-4">
              {/* Date Range Selection */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-[11px] font-bold text-slate-600 mb-1">开始日期</label>
                  <input
                    type="date"
                    value={formatDateForApi(syncModal.startDate)}
                    onChange={(e) => setSyncModal({ ...syncModal, startDate: new Date(e.target.value) })}
                    className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500"
                  />
                </div>
                <div>
                  <label className="block text-[11px] font-bold text-slate-600 mb-1">结束日期</label>
                  <input
                    type="date"
                    value={formatDateForApi(syncModal.endDate)}
                    onChange={(e) => setSyncModal({ ...syncModal, endDate: new Date(e.target.value) })}
                    className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500"
                  />
                </div>
              </div>

              {/* Quick Select Buttons */}
              <div className="flex gap-2 flex-wrap">
                <button
                  onClick={() => {
                    const today = new Date();
                    setSyncModal({ ...syncModal, startDate: today, endDate: today });
                  }}
                  className="text-[10px] font-bold text-slate-600 bg-slate-100 hover:bg-slate-200 px-3 py-1 rounded-lg transition-colors"
                >
                  今天
                </button>
                <button
                  onClick={() => {
                    const today = new Date();
                    const yesterday = new Date(today);
                    yesterday.setDate(yesterday.getDate() - 1);
                    setSyncModal({ ...syncModal, startDate: yesterday, endDate: today });
                  }}
                  className="text-[10px] font-bold text-slate-600 bg-slate-100 hover:bg-slate-200 px-3 py-1 rounded-lg transition-colors"
                >
                  最近2天
                </button>
                <button
                  onClick={() => {
                    const today = new Date();
                    const sevenDaysAgo = new Date(today);
                    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
                    setSyncModal({ ...syncModal, startDate: sevenDaysAgo, endDate: today });
                  }}
                  className="text-[10px] font-bold text-slate-600 bg-slate-100 hover:bg-slate-200 px-3 py-1 rounded-lg transition-colors"
                >
                  最近7天
                </button>
                <button
                  onClick={() => {
                    const today = new Date();
                    const thisMonthStart = new Date(today.getFullYear(), today.getMonth(), 1);
                    setSyncModal({ ...syncModal, startDate: thisMonthStart, endDate: today });
                  }}
                  className="text-[10px] font-bold text-slate-600 bg-slate-100 hover:bg-slate-200 px-3 py-1 rounded-lg transition-colors"
                >
                  本月
                </button>
              </div>

              {/* Sync Result Message */}
              {syncResult && (
                <div className={`p-3 rounded-lg text-sm ${
                  syncResult.includes('成功')
                    ? 'bg-emerald-50 text-emerald-700'
                    : 'bg-rose-50 text-rose-700'
                }`}>
                  {syncResult}
                </div>
              )}

              {/* Action Buttons */}
              <div className="flex justify-end gap-3 pt-2">
                <button
                  onClick={() => {
                    setSyncModal({ ...syncModal, isOpen: false });
                    setSyncResult(null);
                  }}
                  disabled={syncInProgress}
                  className="px-4 py-2 text-sm font-bold text-slate-600 bg-slate-100 rounded-xl hover:bg-slate-200 transition-colors disabled:opacity-50"
                >
                  取消
                </button>
                <button
                  onClick={handleSyncData}
                  disabled={syncInProgress}
                  className="px-4 py-2 text-sm font-bold text-white bg-emerald-500 hover:bg-emerald-600 rounded-xl transition-colors disabled:opacity-50 flex items-center gap-2"
                >
                  {syncInProgress ? (
                    <>
                      <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                      同步中...
                    </>
                  ) : (
                    <>
                      <i className="fas fa-sync-alt text-xs"></i>
                      开始同步
                    </>
                  )}
                </button>
              </div>

              {/* Warning Note */}
              <div className="text-[10px] text-slate-400 bg-amber-50 px-3 py-2 rounded-lg">
                <i className="fas fa-exclamation-triangle text-amber-400 mr-1"></i>
                注意：同步会覆盖未锁定的日期数据，已锁定的日期不会受影响
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default DailyReport;
