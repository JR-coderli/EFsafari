
import { GoogleGenAI } from "@google/genai";
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { AdRow, Dimension, MetricConfig, SavedView, UserPermission, DailyBreakdown } from './types';
import { generateMockReport } from './mockData';
import { loadRootData as apiLoadRootData, loadChildData, loadDailyData as apiLoadDailyData } from './src/api/hooks';
import { authApi, usersApi, tokenManager } from './src/api/auth';
import { dailyReportApi, dashboardApi } from './src/api/client';
import { viewsApi } from './src/api/views';

interface Filter {
  dimension: Dimension;
  value: string;
}

interface ETLStatus {
  last_update: string | null;
  report_date: string | null;
  all_success: boolean;
}

const calculateMetrics = (data: { impressions: number; clicks: number; conversions: number; spend: number; revenue: number; m_imp: number; m_clicks: number }) => {
  return {
    ctr: data.clicks / (data.impressions || 1),
    cvr: data.conversions / (data.clicks || 1),
    roi: (data.revenue - data.spend) / (data.spend || 1),
    cpa: data.spend / (data.conversions || 1),
    rpa: data.revenue / (data.conversions || 1),
    epc: data.revenue / (data.clicks || 1),
    epv: data.revenue / (data.impressions || 1),
    m_epc: data.revenue / (data.m_clicks || 1),
    m_epv: data.revenue / (data.m_imp || 1),
    m_cpc: data.spend / (data.m_clicks || 1),
    m_cpv: data.spend / (data.m_imp || 1)
  };
};

const ALL_DIMENSIONS: { value: Dimension; label: string }[] = [
  { value: 'platform', label: 'Media' },
  { value: 'advertiser', label: 'Advertiser' },
  { value: 'offer', label: 'Offer' },
  { value: 'campaign_name', label: 'Campaign' },
  { value: 'sub_campaign_name', label: 'Adset' },
  { value: 'creative_name', label: 'Ads' },
];

const DEFAULT_METRICS: MetricConfig[] = [
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
];

const MetricValue: React.FC<{ value: number; type: 'money' | 'percent' | 'number' | 'profit'; isSub?: boolean; colorMode?: boolean; metricKey?: string }> = ({ value, type, isSub, colorMode, metricKey }) => {
  const displayValue = isFinite(value) ? value : 0;

  // Profit always has color (positive=green, negative=red)
  if (!isSub && type === 'profit') {
    if (displayValue > 0) return <span className="font-mono tracking-tight leading-none text-[13px] font-bold text-emerald-600">${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
    else if (displayValue < 0) return <span className="font-mono tracking-tight leading-none text-[13px] font-bold text-rose-600">${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
    else return <span className="font-mono tracking-tight leading-none text-[13px] font-bold text-slate-800">${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
  }

  // ROI always has color (positive=green, negative=red)
  if (!isSub && metricKey === 'roi') {
    if (displayValue > 0) return <span className="font-mono tracking-tight leading-none text-[13px] font-bold text-emerald-600">{(displayValue * 100).toFixed(2)}%</span>;
    else if (displayValue < 0) return <span className="font-mono tracking-tight leading-none text-[13px] font-bold text-rose-600">{(displayValue * 100).toFixed(2)}%</span>;
    else return <span className="font-mono tracking-tight leading-none text-[13px] font-bold text-slate-800">{(displayValue * 100).toFixed(2)}%</span>;
  }

  // Color mode for specific metrics
  let colorClasses = '';
  if (colorMode && !isSub) {
    if (metricKey === 'revenue') colorClasses = 'text-amber-500';      // 黄色
    else if (metricKey === 'spend') colorClasses = 'text-rose-500';    // 红色
    else if (metricKey === 'cpa') colorClasses = 'text-blue-500';      // 蓝色
    else if (metricKey === 'epa') colorClasses = 'text-amber-500';     // 黄色
    else if (metricKey === 'epc') colorClasses = 'text-amber-500';     // 黄色
    else if (metricKey === 'epv') colorClasses = 'text-amber-500';     // 黄色
  }

  const baseClasses = `font-mono tracking-tight leading-none ${isSub ? 'text-[12px] text-slate-500 font-medium' : `text-[13px] ${colorClasses} font-bold`}`;

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
  const fourteenDaysAgo = new Date(today);
  fourteenDaysAgo.setDate(fourteenDaysAgo.getDate() - 13);
  const thirtyDaysAgo = new Date(today);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 29);

  switch (range) {
    case 'Today':
      return { label: 'Today', dateString: formatDate(today), start: today, end: today };
    case 'Yesterday':
      return { label: 'Yesterday', dateString: formatDate(yesterday), start: yesterday, end: yesterday };
    case 'Last 7 Days':
      return { label: 'Last 7 Days', dateString: formatRange(sevenDaysAgo, today), start: sevenDaysAgo, end: today };
    case 'Last 14 Days':
      return { label: 'Last 14 Days', dateString: formatRange(fourteenDaysAgo, today), start: fourteenDaysAgo, end: today };
    case 'Last 30 Days':
      return { label: 'Last 30 Days', dateString: formatRange(thirtyDaysAgo, today), start: thirtyDaysAgo, end: today };
    case 'Custom':
      if (customStart && customEnd) {
        return { label: 'Custom', dateString: formatRange(customStart, customEnd), start: customStart, end: customEnd };
      }
      return { label: 'Custom', dateString: 'Select dates...', start: today, end: today };
    default:
      return { label: 'Yesterday', dateString: formatDate(yesterday), start: yesterday, end: yesterday };
  }
};

const DatePicker: React.FC<{ onRangeChange: (range: string, start?: Date, end?: Date) => void; currentDisplay: string; currentRange: string }> = ({ onRangeChange, currentDisplay, currentRange }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [showCalendar, setShowCalendar] = useState(false);
  const [calendarStart, setCalendarStart] = useState<Date | null>(null);
  const [calendarEnd, setCalendarEnd] = useState<Date | null>(null);
  const [selectingEnd, setSelectingEnd] = useState(false);
  const [currentMonth, setCurrentMonth] = useState(new Date());
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const clickOutside = (e: MouseEvent) => { if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) { setIsOpen(false); setShowCalendar(false); } };
    document.addEventListener('mousedown', clickOutside);
    return () => document.removeEventListener('mousedown', clickOutside);
  }, []);

  const handleQuickSelect = (r: string) => {
    const info = getRangeInfo(r);
    onRangeChange(r, info.start, info.end);
    setIsOpen(false);
  };

  const handleCustomRange = () => {
    setShowCalendar(true);
    setSelectingEnd(false);
    setCalendarStart(null);
    setCalendarEnd(null);
    setCurrentMonth(new Date());
  };

  const handleDateClick = (date: Date) => {
    if (!selectingEnd) {
      // First click: select start date
      setCalendarStart(date);
      setCalendarEnd(date);
      setSelectingEnd(true);
    } else {
      // Second click: select end date and apply
      const start = calendarStart || date;
      const end = date;
      setCalendarStart(start);
      setCalendarEnd(end);
      setSelectingEnd(false);
      onRangeChange('Custom', start, end);
      setShowCalendar(false);
      setIsOpen(false);
    }
  };

  const renderCalendar = () => {
    const year = currentMonth.getFullYear();
    const month = currentMonth.getMonth();
    const firstDay = new Date(year, month, 1);
    const lastDay = new Date(year, month + 1, 0);
    const startDate = new Date(firstDay);
    startDate.setDate(startDate.getDate() - firstDay.getDay());
    const days: Date[] = [];
    for (let i = 0; i < 42; i++) {
      const d = new Date(startDate);
      d.setDate(startDate.getDate() + i);
      days.push(d);
    }

    // Helper functions for date comparison
    const isStart = (d: Date) => calendarStart && d.toDateString() === calendarStart.toDateString();
    const isEnd = (d: Date) => calendarEnd && d.toDateString() === calendarEnd.toDateString();
    const isInRange = (d: Date) => {
      if (!calendarStart || !calendarEnd) return false;
      const date = d.getTime();
      return date >= calendarStart.getTime() && date <= calendarEnd.getTime();
    };
    const isCurrentMonth = (d: Date) => d.getMonth() === month;

    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const dayNames = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];

    // Format date for status display
    const formatStatusDate = (d: Date) => `${d.getMonth() + 1}/${d.getDate()}`;

    return (
      <div className="p-4">
        <div className="flex items-center justify-between mb-4">
          <button type="button" onClick={() => setCurrentMonth(new Date(year, month - 1))} className="p-1 hover:bg-slate-100 rounded"><i className="fas fa-chevron-left text-xs text-slate-500"></i></button>
          <span className="text-xs font-bold text-slate-700">{monthNames[month]} {year}</span>
          <button type="button" onClick={() => setCurrentMonth(new Date(year, month + 1))} className="p-1 hover:bg-slate-100 rounded"><i className="fas fa-chevron-right text-xs text-slate-500"></i></button>
        </div>
        <div className="grid grid-cols-7 gap-1 mb-2">
          {dayNames.map(d => <div key={d} className="text-center text-[10px] font-bold text-slate-400 py-1">{d}</div>)}
        </div>
        <div className="grid grid-cols-7 gap-1">
          {days.map((d, i) => {
            const start = isStart(d);
            const end = isEnd(d);
            const inRange = isInRange(d);
            const current = isCurrentMonth(d);
            let cellClass = "h-7 w-7 flex items-center justify-center text-[10px] rounded cursor-pointer transition-colors ";
            if (start) cellClass += "bg-indigo-600 text-white ";
            else if (end) cellClass += "bg-indigo-600 text-white ";
            else if (inRange) cellClass += "bg-indigo-100 text-indigo-700 ";
            else if (!current) cellClass += "text-slate-300 ";
            else cellClass += "text-slate-600 hover:bg-slate-100 ";
            return <button type="button" key={i} onClick={() => handleDateClick(d)} className={cellClass}>{d.getDate()}</button>;
          })}
        </div>
        <div className="mt-3 pt-3 border-t border-slate-100 flex items-center justify-between">
          <span className="text-[10px] text-slate-500">
            {selectingEnd
              ? (calendarStart ? `From: ${formatStatusDate(calendarStart)} → To: ?` : 'Select start date')
              : (calendarStart && calendarEnd
                  ? `${formatStatusDate(calendarStart)} - ${formatStatusDate(calendarEnd)}`
                  : 'Select start date')
            }
          </span>
        </div>
      </div>
    );
  };

  return (
    <div className="relative" ref={dropdownRef}>
      <button onClick={() => setIsOpen(!isOpen)} className="flex items-center gap-3 px-4 py-2 bg-white border border-slate-200 rounded-xl text-xs font-bold text-slate-600 hover:bg-slate-50 transition-all shadow-sm active:scale-95">
        <i className="far fa-calendar text-indigo-500"></i>
        <span>{currentDisplay}</span>
        <i className={`fas fa-chevron-down text-[10px] text-slate-400 transition-transform ${isOpen ? 'rotate-180' : ''}`}></i>
      </button>
      {isOpen && (
        <div className="absolute top-full mt-2 left-0 bg-white rounded-2xl shadow-2xl border border-slate-100 z-[9999] overflow-hidden">
          {!showCalendar ? (
            <div className="p-2 w-48">
              <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2 px-1">Quick Select</div>
              {['Today', 'Yesterday', 'Last 7 Days', 'Last 14 Days', 'Last 30 Days'].map(r => (
                <button key={r} onClick={() => handleQuickSelect(r)} className={`w-full text-left px-3 py-2 text-[11px] font-bold rounded-xl ${currentRange === r ? 'bg-indigo-100 text-indigo-700' : 'text-slate-600 hover:bg-indigo-50'}`}>
                  {r}
                </button>
              ))}
              <div className="border-t border-slate-100 mt-2 pt-2">
                <button onClick={handleCustomRange} className="w-full text-left px-3 py-2 text-[11px] font-bold text-indigo-600 hover:bg-indigo-50 rounded-xl flex items-center gap-2">
                  <i className="far fa-calendar-alt"></i> Custom Range...
                </button>
              </div>
            </div>
          ) : (
            <div className="w-72">{renderCalendar()}</div>
          )}
        </div>
      )}
    </div>
  );
};

// Daily Report Page Component
interface DailyReportPageProps {
  selectedRange: string;
  customDateStart: Date | undefined;
  customDateEnd: Date | undefined;
  onRangeChange: (range: string, start?: Date, end?: Date) => void;
  currentUser: UserPermission;
}

const DailyReportPage: React.FC<DailyReportPageProps> = ({ selectedRange, customDateStart, customDateEnd, onRangeChange, currentUser }) => {
  const [data, setData] = useState<Array<{
    date: string;
    media: string;
    impressions: number;
    clicks: number;
    conversions: number;
    revenue: number;
    spend_original: number;
    spend_manual: number;
    spend_final: number;
    m_imp: number;
    m_clicks: number;
    m_conv: number;
    is_locked?: number;
  }>>([]);
  const [summary, setSummary] = useState<any>(null);
  const [mediaList, setMediaList] = useState<Array<{ name: string }>>([]);
  const [selectedMedia, setSelectedMedia] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingSpend, setEditingSpend] = useState<{ date: string; media: string } | null>(null);
  const [lockedDates, setLockedDates] = useState<Set<string>>(new Set());
  const [showSyncModal, setShowSyncModal] = useState(false);

  // Calculate date range
  const dateInfo = useMemo(() => getRangeInfo(selectedRange, customDateStart, customDateEnd), [selectedRange, customDateStart, customDateEnd]);
  const startDate = dateInfo.start.toISOString().split('T')[0];
  const endDate = dateInfo.end.toISOString().split('T')[0];

  // Load data
  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      setError(null);
      try {
        const [dataResult, summaryResult, mediaResult] = await Promise.all([
          dailyReportApi.getData({ startDate, endDate, media: selectedMedia || undefined }),
          dailyReportApi.getSummary({ startDate, endDate, media: selectedMedia || undefined }),
          dailyReportApi.getMediaList(),
        ]);
        setData(dataResult);
        setSummary(summaryResult);
        setMediaList(mediaResult.media);
      } catch (err) {
        console.error('Error loading daily report:', err);
        setError(err instanceof Error ? err.message : 'Failed to load data');
      } finally {
        setLoading(false);
      }
    };
    loadData();
  }, [startDate, endDate, selectedMedia]);

  // Handle spend edit
  const handleSpendClick = (row: typeof data[0]) => {
    setEditingSpend({ date: row.date, media: row.media });
  };

  const handleSpendSave = async (newValue: string) => {
    if (!editingSpend) return;
    const spendValue = parseFloat(newValue);
    if (isNaN(spendValue)) {
      alert('Invalid value');
      return;
    }

    try {
      await dailyReportApi.updateSpend({
        date: editingSpend.date,
        media: editingSpend.media,
        spend_value: spendValue,
      });
      // Reload data
      const [dataResult, summaryResult] = await Promise.all([
        dailyReportApi.getData({ startDate, endDate, media: selectedMedia || undefined }),
        dailyReportApi.getSummary({ startDate, endDate, media: selectedMedia || undefined }),
      ]);
      setData(dataResult);
      setSummary(summaryResult);
      setEditingSpend(null);
    } catch (err) {
      alert('Failed to update spend: ' + (err instanceof Error ? err.message : 'Unknown error'));
    }
  };

  // Load locked dates
  useEffect(() => {
    const loadLockedDates = async () => {
      try {
        const result = await dailyReportApi.getLockedDates();
        setLockedDates(new Set(result.locked_dates));
      } catch (err) {
        console.error('Error loading locked dates:', err);
      }
    };
    loadLockedDates();
  }, []);

  // Handle sync data
  const handleSyncData = async () => {
    setSyncing(true);
    try {
      const result = await dailyReportApi.syncData({ startDate, endDate });
      alert(result.message);
      // Reload data
      const [dataResult, summaryResult] = await Promise.all([
        dailyReportApi.getData({ startDate, endDate, media: selectedMedia || undefined }),
        dailyReportApi.getSummary({ startDate, endDate, media: selectedMedia || undefined }),
      ]);
      setData(dataResult);
      setSummary(summaryResult);
    } catch (err) {
      alert('Sync failed: ' + (err instanceof Error ? err.message : 'Unknown error'));
    } finally {
      setSyncing(false);
      setShowSyncModal(false);
    }
  };

  // Handle lock/unlock date
  const handleToggleLock = async (date: string) => {
    const isCurrentlyLocked = lockedDates.has(date);
    const newLockState = !isCurrentlyLocked;

    if (!confirm(`${newLockState ? 'Lock' : 'Unlock'} date ${date}?`)) return;

    try {
      await dailyReportApi.lockDate({ date, lock: newLockState });
      // Update locked dates
      if (newLockState) {
        setLockedDates(prev => new Set(prev).add(date));
      } else {
        setLockedDates(prev => {
          const newSet = new Set(prev);
          newSet.delete(date);
          return newSet;
        });
      }
    } catch (err) {
      alert('Failed to toggle lock: ' + (err instanceof Error ? err.message : 'Unknown error'));
    }
  };

  // Calculate summary for filtered data
  const filteredSummary = useMemo(() => {
    const total = data.reduce((acc, row) => ({
      impressions: acc.impressions + row.impressions,
      clicks: acc.clicks + row.clicks,
      conversions: acc.conversions + row.conversions,
      spend: acc.spend + row.spend_final,
      revenue: acc.revenue + row.revenue,
    }), { impressions: 0, clicks: 0, conversions: 0, spend: 0, revenue: 0 });

    return {
      ...total,
      ctr: total.clicks / (total.impressions || 1),
      cvr: total.conversions / (total.clicks || 1),
      roi: (total.revenue - total.spend) / (total.spend || 1),
    };
  }, [data]);

  return (
    <div className="flex-1 flex flex-col bg-white min-w-0">
      {/* Summary Cards */}
      <div className="px-8 py-6 border-b border-slate-100 bg-slate-50">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-4">
            <h3 className="text-lg font-black uppercase italic tracking-tighter">Daily Report Summary</h3>
            {(currentUser.role === 'admin' || currentUser.role === 'ops') && (
              <button
                onClick={() => setShowSyncModal(true)}
                className="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-[10px] font-bold uppercase hover:bg-indigo-700 transition-all flex items-center gap-1"
                disabled={syncing}
              >
                <i className={`fas ${syncing ? 'fa-spinner fa-spin' : 'fa-sync-alt'}`}></i>
                {syncing ? 'Syncing...' : 'Sync Data'}
              </button>
            )}
          </div>
          <div className="flex items-center gap-3">
            <select
              value={selectedMedia}
              onChange={(e) => setSelectedMedia(e.target.value)}
              className="px-3 py-2 bg-white border border-slate-200 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20"
            >
              <option value="">All Media</option>
              {mediaList.map(m => <option key={m.name} value={m.name}>{m.name}</option>)}
            </select>
          </div>
        </div>
        <div className="grid grid-cols-6 gap-4">
          {[
            { label: 'Impressions', value: filteredSummary.impressions, type: 'number' as const, color: 'indigo' },
            { label: 'Clicks', value: filteredSummary.clicks, type: 'number' as const, color: 'blue' },
            { label: 'Conversions', value: filteredSummary.conversions, type: 'number' as const, color: 'purple' },
            { label: 'Spend', value: filteredSummary.spend, type: 'money' as const, color: 'rose' },
            { label: 'Revenue', value: filteredSummary.revenue, type: 'money' as const, color: 'emerald' },
            { label: 'ROI', value: filteredSummary.roi, type: 'percent' as const, color: 'amber' },
          ].map((card) => (
            <div key={card.label} className={`bg-${card.color}-50 border border-${card.color}-100 rounded-2xl p-4`}>
              <div className={`text-[10px] font-black uppercase text-${card.color}-500 tracking-widest mb-1`}>{card.label}</div>
              <div className={`text-xl font-black text-${card.color}-700`}>
                <MetricValue value={card.value} type={card.type} />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Data Table */}
      <div className="flex-1 overflow-auto">
        {loading ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-slate-400">Loading...</div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-rose-500">{error}</div>
          </div>
        ) : (
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-slate-50 text-[10px] font-black text-slate-400 uppercase tracking-widest border-b border-slate-200 sticky top-0 z-10">
                <th className="px-4 py-3">Date</th>
                <th className="px-4 py-3">Media</th>
                <th className="px-4 py-3 text-right">Impressions</th>
                <th className="px-4 py-3 text-right">Clicks</th>
                <th className="px-4 py-3 text-right">Conversions</th>
                <th className="px-4 py-3 text-right">Spend (Editable)</th>
                <th className="px-4 py-3 text-right">Revenue</th>
                <th className="px-4 py-3 text-right">ROI</th>
                {(currentUser.role === 'admin' || currentUser.role === 'ops') && (
                  <th className="px-4 py-3 text-center">Lock</th>
                )}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-50">
              {data.map((row, idx) => {
                const isLocked = lockedDates.has(row.date);
                return (
                <tr key={`${row.date}-${row.media}`} className={`hover:bg-indigo-50/40 transition-colors ${isLocked ? 'bg-amber-50/30' : ''}`}>
                  <td className="px-4 py-3 text-xs font-bold text-slate-600">{row.date}</td>
                  <td className="px-4 py-3 text-xs font-bold text-slate-800">{row.media}</td>
                  <td className="px-4 py-3 text-right"><MetricValue value={row.impressions} type="number" /></td>
                  <td className="px-4 py-3 text-right"><MetricValue value={row.clicks} type="number" /></td>
                  <td className="px-4 py-3 text-right"><MetricValue value={row.conversions} type="number" /></td>
                  <td
                    className="px-4 py-3 text-right cursor-pointer hover:bg-indigo-100 transition-colors group relative"
                    onClick={() => handleSpendClick(row)}
                  >
                    {editingSpend?.date === row.date && editingSpend?.media === row.media ? (
                      <input
                        type="number"
                        autoFocus
                        defaultValue={row.spend_manual}
                        onBlur={(e) => handleSpendSave(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') handleSpendSave((e.target as HTMLInputElement).value);
                          if (e.key === 'Escape') setEditingSpend(null);
                        }}
                        className="w-24 px-2 py-1 bg-white border border-indigo-500 rounded text-right text-xs font-bold outline-none"
                        onClick={(e) => e.stopPropagation()}
                      />
                    ) : (
                      <span className={row.spend_manual !== 0 ? 'text-amber-600 font-bold' : ''}>
                        <MetricValue value={row.spend_final} type="money" />
                      </span>
                    )}
                    <span className="absolute -top-1 -right-1 w-2 h-2 bg-indigo-500 rounded-full opacity-0 group-hover:opacity-100 transition-opacity"></span>
                  </td>
                  <td className="px-4 py-3 text-right"><MetricValue value={row.revenue} type="money" /></td>
                  <td className="px-4 py-3 text-right">
                    {(() => {
                      const roi = row.spend_final > 0 ? (row.revenue - row.spend_final) / row.spend_final : 0;
                      const roiClass = roi > 0 ? 'text-emerald-600' : roi < 0 ? 'text-rose-600' : 'text-slate-700';
                      return <span className={`font-mono font-bold text-[13px] ${roiClass}`}>{(roi * 100).toFixed(2)}%</span>;
                    })()}
                  </td>
                  {(currentUser.role === 'admin' || currentUser.role === 'ops') && (
                    <td className="px-4 py-3 text-center">
                      <button
                        onClick={() => handleToggleLock(row.date)}
                        className={`w-8 h-8 rounded-lg flex items-center justify-center transition-all ${
                          isLocked
                            ? 'bg-amber-100 text-amber-600 hover:bg-amber-200'
                            : 'bg-slate-100 text-slate-400 hover:bg-slate-200'
                        }`}
                        title={isLocked ? 'Unlock this date' : 'Lock this date'}
                      >
                        <i className={`fas ${isLocked ? 'fa-lock' : 'fa-lock-open'} text-xs`}></i>
                      </button>
                    </td>
                  )}
                </tr>
                );
              })}
            </tbody>
            <tfoot className="bg-indigo-600 sticky bottom-0 z-10 border-t-2 border-indigo-700">
              <tr>
                <td className="px-4 py-3 font-bold text-white text-xs" colSpan={2}>
                  <i className="fas fa-calculator mr-2"></i>TOTAL
                </td>
                <td className="px-4 py-3 text-right bg-indigo-600 text-white"><MetricValue value={filteredSummary.impressions} type="number" /></td>
                <td className="px-4 py-3 text-right bg-indigo-600 text-white"><MetricValue value={filteredSummary.clicks} type="number" /></td>
                <td className="px-4 py-3 text-right bg-indigo-600 text-white"><MetricValue value={filteredSummary.conversions} type="number" /></td>
                <td className="px-4 py-3 text-right bg-indigo-600 text-white"><MetricValue value={filteredSummary.spend} type="money" /></td>
                <td className="px-4 py-3 text-right bg-indigo-600 text-white"><MetricValue value={filteredSummary.revenue} type="money" /></td>
                <td className="px-4 py-3 text-right bg-indigo-600 text-white">
                  <span className={`font-mono font-bold ${filteredSummary.roi > 0 ? 'text-emerald-300' : filteredSummary.roi < 0 ? 'text-rose-300' : 'text-white'}`}>{(filteredSummary.roi * 100).toFixed(2)}%</span>
                </td>
                {(currentUser.role === 'admin' || currentUser.role === 'ops') && <td className="px-4 py-3 bg-indigo-600"></td>}
              </tr>
            </tfoot>
          </table>
        )}
      </div>

      {/* Sync Confirmation Modal */}
      {showSyncModal && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-6">
          <div className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onClick={() => setShowSyncModal(false)}></div>
          <div className="relative w-full max-w-md bg-white rounded-3xl shadow-2xl p-8 animate-in zoom-in duration-200">
            <h3 className="text-xl font-black uppercase italic tracking-tighter mb-4">
              <i className="fas fa-sync-alt mr-2 text-indigo-600"></i>Sync Data from Performance
            </h3>
            <p className="text-sm text-slate-600 mb-6">
              将从 Performance 表同步数据到 Daily Report 表。<br />
              <span className="text-amber-600 font-bold">注意：</span>已锁定的日期不会被覆盖。
            </p>
            <div className="bg-slate-50 rounded-xl p-4 mb-6">
              <div className="text-xs text-slate-500 mb-2">同步范围：</div>
              <div className="font-bold text-slate-700">{startDate} 至 {endDate}</div>
            </div>
            <div className="flex gap-3">
              <button
                onClick={() => setShowSyncModal(false)}
                className="flex-1 py-3 bg-slate-100 text-slate-600 rounded-xl font-bold text-sm uppercase tracking-widest hover:bg-slate-200 transition-all"
              >
                Cancel
              </button>
              <button
                onClick={handleSyncData}
                disabled={syncing}
                className="flex-1 py-3 bg-indigo-600 text-white rounded-xl font-bold text-sm uppercase tracking-widest shadow-lg shadow-indigo-100 hover:bg-indigo-700 transition-all active:scale-95 disabled:opacity-50"
              >
                {syncing ? 'Syncing...' : 'Confirm Sync'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

const LoginPage: React.FC<{ onLogin: (user: UserPermission) => void }> = ({ onLogin }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [useLocalAuth, setUseLocalAuth] = useState(false);  // Fallback to local auth

  // Check if user is already logged in
  useEffect(() => {
    const savedUser = tokenManager.getUser();
    if (savedUser) {
      onLogin(savedUser);
    }
  }, [onLogin]);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      if (useLocalAuth) {
        // Fallback to local auth
        const storedUsers: UserPermission[] = JSON.parse(localStorage.getItem('ad_tech_users') || '[]');
        const adminUser = { id: 'admin', name: 'Admin User', username: 'admin', password: 'password', email: 'admin@addata.ai', role: 'admin' as const, keywords: [] };
        const allUsers = [adminUser, ...storedUsers];

        const user = allUsers.find(u => u.username === username && u.password === password);
        if (user) onLogin(user);
        else setError('Invalid credentials');
      } else {
        // Use real API
        const user = await authApi.login(username, password);
        onLogin(user);
      }
    } catch (err) {
      console.error('Login error:', err);
      const errorMsg = err instanceof Error ? err.message : 'Login failed';
      setError(errorMsg);

      // Auto-fallback to local auth on API error
      if (!useLocalAuth) {
        setError('API unavailable - using local auth');
        setUseLocalAuth(true);
        setTimeout(() => {
          setLoading(false);
        }, 500);
        return;
      }
    } finally {
      setLoading(false);
    }
  };
  return (
    <div className="min-h-screen flex items-center justify-center bg-slate-100 p-6 font-sans">
      <div className="w-full max-w-md bg-white rounded-3xl shadow-2xl p-10 border border-slate-100">
        <div className="flex flex-col items-center mb-10 text-center">
          <div className="w-16 h-16 bg-indigo-600 rounded-2xl flex items-center justify-center text-white mb-6 font-bold text-2xl">EF</div>
          <h1 className="text-3xl font-extrabold text-slate-900 tracking-tight">EFLOW</h1>
          <p className="text-sm font-medium text-slate-500 mt-1">Safari System</p>
        </div>
        {error && <div className="mb-4 text-rose-500 text-sm font-bold text-center">{error}</div>}
        <form onSubmit={handleLogin} className="space-y-6">
          <input
            type="text"
            autoComplete="username"
            required
            className="w-full px-5 py-4 bg-slate-50 border border-slate-100 rounded-2xl"
            value={username}
            onChange={e => setUsername(e.target.value)}
            placeholder="Username"
          />
          <input
            type="password"
            autoComplete="current-password"
            required
            className="w-full px-5 py-4 bg-slate-50 border border-slate-100 rounded-2xl"
            value={password}
            onChange={e => setPassword(e.target.value)}
            placeholder="••••••••"
          />
          <button type="submit" disabled={loading} className="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-5 rounded-2xl transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
            {loading ? 'Authenticating...' : 'Authenticate'}
          </button>
        </form>
      </div>
    </div>
  );
};

const Dashboard: React.FC<{ currentUser: UserPermission; onLogout: () => void }> = ({ currentUser, onLogout }) => {
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<AdRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [useMock, setUseMock] = useState(false);  // Fallback to mock if API fails
  const [activeDims, setActiveDims] = useState<Dimension[]>(['platform', 'offer']);
  const [metrics, setMetrics] = useState<MetricConfig[]>(DEFAULT_METRICS);
  const [currentPage, setCurrentPage] = useState<'performance' | 'permissions' | 'daily_report'>('performance');
  const [activeFilters, setActiveFilters] = useState<Filter[]>([]);
  const [expandedDailyRows, setExpandedDailyRows] = useState<Set<string>>(new Set());
  const [expandedDimRows, setExpandedDimRows] = useState<Set<string>>(new Set());
  // Store daily data separately to avoid reference issues with flattened data
  const [dailyDataMap, setDailyDataMap] = useState<Map<string, DailyBreakdown[]>>(new Map());
  const [selectedRange, setSelectedRange] = useState('Yesterday');
  const [customDateStart, setCustomDateStart] = useState<Date | undefined>(undefined);
  const [customDateEnd, setCustomDateEnd] = useState<Date | undefined>(undefined);
  const [quickFilterText, setQuickFilterText] = useState('');
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [showColumnEditor, setShowColumnEditor] = useState(false);
  const [showViewList, setShowViewList] = useState(false);
  const viewsDropdownRef = useRef<HTMLDivElement>(null);

  // Pagination state
  const [paginationPage, setPaginationPage] = useState(1);
  const [rowsPerPage] = useState(20);

  // Sort state
  type SortColumn = 'revenue' | 'spend' | 'impressions' | 'clicks' | 'conversions' | 'ctr' | 'cvr' | 'roi' | 'cpa' | 'rpa' | 'epc' | 'epv' | 'm_epc' | 'm_epv' | 'm_cpc' | 'm_cpv' | 'm_imp' | 'm_clicks' | 'm_conv';
  type SortOrder = 'asc' | 'desc' | null;
  const [sortColumn, setSortColumn] = useState<SortColumn>('revenue');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [hideZeroImpressions, setHideZeroImpressions] = useState(true);
  const [colorMode, setColorMode] = useState(false);

  // ETL status state
  const [etlStatus, setEtlStatus] = useState<ETLStatus | null>(null);

  // Column width state
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({
    hierarchy: 300,
    ...Object.fromEntries(DEFAULT_METRICS.map(m => [m.key, 120]))
  });
  const [resizingColumn, setResizingColumn] = useState<string | null>(null);
  const tableRef = useRef<HTMLTableElement>(null);

  // Computed display string for the date picker
  const dateDisplayString = useMemo(() => {
    return getRangeInfo(selectedRange, customDateStart, customDateEnd).dateString;
  }, [selectedRange, customDateStart, customDateEnd]);

  // Reset pagination when filters or data changes
  useEffect(() => {
    setPaginationPage(1);
  }, [activeFilters, quickFilterText, selectedRange, customDateStart, customDateEnd]);

  // Reset mock mode when currentUser changes (user just logged in)
  useEffect(() => {
    if (useMock) {
      console.log('User logged in, switching back to live API');
      setUseMock(false);
      setError(null);
    }
  }, [currentUser.id]);

  // Load ETL status on mount and refresh periodically
  useEffect(() => {
    const loadEtlStatus = async () => {
      // Development: use mock data directly
      if (import.meta.env.DEV) {
        setEtlStatus({
          last_update: '2026.01.13 09:00',
          report_date: new Date().toISOString().split('T')[0],
          all_success: true
        });
        return;
      }

      // Production: call API
      try {
        const status = await dashboardApi.getEtlStatus();
        setEtlStatus(status);
      } catch (err) {
        console.error('Failed to load ETL status:', err);
      }
    };

    // Initial load
    loadEtlStatus();

    // Refresh every 5 minutes
    const interval = setInterval(loadEtlStatus, 5 * 60 * 1000);

    return () => clearInterval(interval);
  }, []);

  // Column resize handlers
  const handleResizeStart = (columnKey: string, e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setResizingColumn(columnKey);
  };

  useEffect(() => {
    const handleResizeMove = (e: MouseEvent) => {
      if (resizingColumn) {
        const table = tableRef.current;
        if (table) {
          const rect = table.getBoundingClientRect();
          const newWidth = e.clientX - rect.left;
          if (newWidth >= 80 && newWidth <= 500) {
            setColumnWidths(prev => ({ ...prev, [resizingColumn]: newWidth }));
          }
        }
      }
    };

    const handleResizeEnd = () => {
      setResizingColumn(null);
    };

    if (resizingColumn) {
      document.addEventListener('mousemove', handleResizeMove);
      document.addEventListener('mouseup', handleResizeEnd);
      return () => {
        document.removeEventListener('mousemove', handleResizeMove);
        document.removeEventListener('mouseup', handleResizeEnd);
      };
    }
  }, [resizingColumn]);

  // Filter and flatten data before pagination
  const filteredAndFlattenedData = useMemo(() => {
    // Sort function - sorts a single level of rows
    const sortRows = (rows: AdRow[]): AdRow[] => {
      if (!sortColumn || !sortOrder) return rows;
      return [...rows].sort((a, b) => {
        const aVal = a[sortColumn] as number;
        const bVal = b[sortColumn] as number;
        if (sortOrder === 'asc') {
          return aVal - bVal;
        } else {
          return bVal - aVal;
        }
      });
    };

    // Flatten while sorting each level independently
    const flatten = (rows: AdRow[]): AdRow[] => {
      // Sort the current level
      const sortedRows = sortRows(rows);
      const results: AdRow[] = [];

      sortedRows.forEach(row => {
        // Apply quickFilterText only to top-level rows (not expanded children)
        const matchesFilter = !quickFilterText || row.name.toLowerCase().includes(quickFilterText.toLowerCase());
        // Hide rows with zero impressions
        const hasImpressions = !hideZeroImpressions || row.impressions > 0;

        if (matchesFilter && hasImpressions) {
          results.push(row);
        }

        // Include expanded children - sort each child group independently
        if (expandedDimRows.has(row.id) && row.children) {
          let childRows = row.children || [];
          // Filter by impressions first
          childRows = childRows.filter(child => !hideZeroImpressions || child.impressions > 0);
          // Sort the children
          childRows = sortRows(childRows);
          childRows.forEach(child => {
            results.push(child);
            // Include grandchildren if also expanded - sort them too
            if (expandedDimRows.has(child.id) && child.children) {
              let grandchildRows = child.children.filter(gc => !hideZeroImpressions || gc.impressions > 0);
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

    const result = flatten(data);
    return result;
  }, [data, expandedDimRows, quickFilterText, hideZeroImpressions, sortColumn, sortOrder]);

  // Calculate summary data for all filtered rows
  const summaryData = useMemo(() => {
    const summary = {
      impressions: 0, clicks: 0, conversions: 0,
      spend: 0, revenue: 0, profit: 0, m_imp: 0, m_clicks: 0, m_conv: 0,
    };
    filteredAndFlattenedData.forEach(row => {
      summary.impressions += row.impressions || 0;
      summary.clicks += row.clicks || 0;
      summary.conversions += row.conversions || 0;
      summary.spend += row.spend || 0;
      summary.revenue += row.revenue || 0;
      summary.profit += row.profit || 0;
      summary.m_imp += row.m_imp || 0;
      summary.m_clicks += row.m_clicks || 0;
      summary.m_conv += row.m_conv || 0;
    });
    // Calculate derived metrics
    return {
      ...summary,
      ctr: summary.clicks / (summary.impressions || 1),
      cvr: summary.conversions / (summary.clicks || 1),
      roi: summary.revenue > 0 ? (summary.revenue - summary.spend) / (summary.spend || 1) : 0,
      cpa: summary.spend / (summary.conversions || 1),
      rpa: summary.revenue / (summary.conversions || 1),
      epc: summary.revenue / (summary.clicks || 1),
      epv: summary.revenue / (summary.impressions || 1),
      m_epc: summary.revenue / (summary.m_clicks || 1),
      m_epv: summary.revenue / (summary.m_imp || 1),
      m_cpc: summary.spend / (summary.m_clicks || 1),
      m_cpv: summary.spend / (summary.m_imp || 1),
    };
  }, [filteredAndFlattenedData]);

  // Paginated data
  const paginatedData = useMemo(() => {
    const startIndex = (paginationPage - 1) * rowsPerPage;
    const endIndex = startIndex + rowsPerPage;
    const result = filteredAndFlattenedData.slice(startIndex, endIndex);
    return result;
  }, [filteredAndFlattenedData, paginationPage, rowsPerPage]);

  const totalRows = filteredAndFlattenedData.length;
  const totalPages = Math.ceil(totalRows / rowsPerPage);

  // Sort handler
  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      // Toggle order: desc -> asc -> null -> desc
      if (sortOrder === 'desc') {
        setSortOrder('asc');
      } else if (sortOrder === 'asc') {
        setSortColumn('revenue');
        setSortOrder('desc');
      }
    } else {
      setSortColumn(column);
      setSortOrder('desc');
    }
  };

  // Get sort icon for a column
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
  // Permission management state
  const [users, setUsers] = useState<UserPermission[]>([]);
  const [showUserModal, setShowUserModal] = useState(false);
  const [editingUser, setEditingUser] = useState<UserPermission | null>(null);
  const [useLocalUsers, setUseLocalUsers] = useState(false);  // Fallback to local

  // Load users from API on mount (for admin)
  useEffect(() => {
    const loadUsers = async () => {
      if (currentUser.id === 'admin') {
        try {
          const apiUsers = await usersApi.getAllUsers();
          setUsers(apiUsers);
        } catch (err) {
          console.error('Failed to load users from API, using local:', err);
          // Fallback to local storage
          const localUsers = JSON.parse(localStorage.getItem('ad_tech_users') || '[]');
          setUsers(localUsers);
          setUseLocalUsers(true);
        }
      }
    };
    loadUsers();
  }, [currentUser.id]);

  const dragItem = useRef<number | null>(null);
  const dragOverItem = useRef<number | null>(null);

  // 用户专属的存储键
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

  const loadRootData = useCallback(async () => {
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
        // Fallback to mock data
        await new Promise(resolve => setTimeout(resolve, 300));
        rawData = generateMockReport(
          activeDims[currentLevel],
          currentLevel,
          activeFilters.map(f => f.value).join('|'),
          activeDims.slice(currentLevel + 1),
          selectedRange
        );
      } else {
        // Use real API
        rawData = await apiLoadRootData(activeDims, activeFilters, selectedRange, customDateStart, customDateEnd);
      }

      setData(rawData);
    } catch (err) {
      console.error('Error loading data:', err);
      const errorMsg = err instanceof Error ? err.message : 'Failed to load data';

      // Only show error if not using mock
      if (useMock) {
        setError(errorMsg);
        setLoading(false);
        return;
      }

      // Check if it's an auth error (401) - don't fallback to mock for auth errors
      if (errorMsg.includes('Not authenticated') || errorMsg.includes('401')) {
        setError('Please login to access data');
        setLoading(false);
        return;
      }

      setError(errorMsg);

      // Auto-fallback to mock on other API errors
      console.log('API unavailable, falling back to mock data');
      setUseMock(true);
      setLoading(false);
      return;
    } finally {
      if (!useMock) {
        setLoading(false);
      }
    }
  }, [activeDims, activeFilters, selectedRange, customDateStart, customDateEnd, currentUser, useMock]);

  useEffect(() => { if (currentPage === 'performance') loadRootData(); }, [loadRootData, currentPage]);

  // 从后端加载保存的视图列表
  useEffect(() => {
    const loadViews = async () => {
      try {
        const views = await viewsApi.getAllViews();
        setSavedViews(views);
        // 同步到 localStorage 作为缓存
        const userKey = getUserStorageKey(currentUser.id);
        localStorage.setItem(userKey, JSON.stringify(views));
      } catch (error) {
        console.error('Failed to load views from backend:', error);
        // 降级到 localStorage
        const userKey = getUserStorageKey(currentUser.id);
        const saved = localStorage.getItem(userKey);
        if (saved) {
          setSavedViews(JSON.parse(saved));
        }
      }
    };
    loadViews();
  }, [currentUser.id]);

  // 加载默认视图（只在首次登录时执行）
  useEffect(() => {
    const loadDefaultView = async () => {
      try {
        const defaultView = await viewsApi.getDefaultView();
        if (defaultView) {
          applyView(defaultView);
        } else {
          // 新用户没有默认视图，应用预设的初始视图
          // 维度：media, adset
          setActiveDims(['platform', 'sub_campaign_name']);
        }
      } catch (error) {
        console.error('Failed to load default view:', error);
        // 降级：应用预设的初始视图
        setActiveDims(['platform', 'sub_campaign_name']);
        // 降级：从 localStorage 读取
        const userKey = getUserStorageKey(currentUser.id);
        const saved = localStorage.getItem(userKey);
        if (saved) {
          const views: SavedView[] = JSON.parse(saved);
          const defaultView = views.find(v => v.isDefault);
          if (defaultView) {
            applyView(defaultView);
          }
        }
      }
    };
    // 只在组件挂载时加载一次（使用 ref 确保只执行一次）
    const hasLoadedDefault = { current: false };
    if (!hasLoadedDefault.current) {
      hasLoadedDefault.current = true;
      loadDefaultView();
    }
  }, []);

  useEffect(() => {
    const clickOutside = (e: MouseEvent) => { if (viewsDropdownRef.current && !viewsDropdownRef.current.contains(e.target as Node)) setShowViewList(false); };
    document.addEventListener('mousedown', clickOutside);
    return () => document.removeEventListener('mousedown', clickOutside);
  }, []);

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
      // 保存到后端
      const savedView = await viewsApi.createView(newView);
      setSavedViews(prev => [...prev, savedView]);
      // 同步到 localStorage 作为缓存
      const userKey = getUserStorageKey(currentUser.id);
      localStorage.setItem(userKey, JSON.stringify([...savedViews, savedView]));
    } catch (error) {
      console.error('Failed to save view to backend:', error);
      // 降级到 localStorage
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
      // 降级处理
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
      // 降级处理
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

    setActiveDims(newDims);
    setActiveFilters([]);
    draggedDimIndex.current = null;
  };

  const handleMetricReorder = () => {
    if (dragItem.current !== null && dragOverItem.current !== null) {
      const copyListItems = [...metrics];
      const dragItemContent = copyListItems[dragItem.current];
      copyListItems.splice(dragItem.current, 1);
      copyListItems.splice(dragOverItem.current, 0, dragItemContent);
      dragItem.current = dragOverItem.current;
      dragOverItem.current = null;
      setMetrics(copyListItems);
    }
  };

  const toggleDimExpansion = async (e: React.MouseEvent, row: AdRow) => {
    console.log('[toggleDimExpansion] CALLED - row.id:', row.id, 'row.level:', row.level, 'row.hasChild:', row.hasChild);
    e.preventDefault(); e.stopPropagation();
    if (!row.hasChild) {
      console.log('[toggleDimExpansion] Returning early because hasChild is false');
      return;
    }
    const nextExpanded = new Set(expandedDimRows);
    if (nextExpanded.has(row.id)) {
      nextExpanded.delete(row.id);
    } else {
      nextExpanded.add(row.id);
      if (!row.children || row.children.length === 0) {
        const nextLevel = row.level + 1;
        let children: AdRow[];

        // Build filters for this row's hierarchy - use filterPath if available (for child rows)
        const rowFilters = row.filterPath || row.id.split('|').map((v, i) => ({
          dimension: activeDims[i],
          value: v
        }));

        console.log('[toggleDimExpansion] row.id:', row.id, 'row.level:', row.level, 'rowFilters:', rowFilters);
        console.log('[toggleDimExpansion] row.filterPath:', row.filterPath);
        console.log('[toggleDimExpansion] activeDims:', activeDims);
        console.log('[toggleDimExpansion] activeDims.length:', activeDims.length, 'row.level + 1:', row.level + 1);

        if (useMock) {
          // Use mock data
          children = generateMockReport(
            activeDims[nextLevel],
            nextLevel,
            row.id,
            activeDims.slice(nextLevel + 1),
            selectedRange
          );
        } else {
          // Use real API
          try {
            // Pass row's filterPath to load child data
            children = await loadChildData(activeDims, rowFilters, selectedRange, row.id, customDateStart, customDateEnd);
            console.log('[toggleDimExpansion] children loaded:', children);
            if (children.length > 0) {
              console.log('[toggleDimExpansion] first child:', children[0]);
            }
          } catch (err) {
            console.error('Error loading child data:', err);
            children = [];
          }
        }

        // Apply permission filtering
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
            const indent = '  '.repeat(depth);
            console.log(`[update] Depth ${depth}: checking ${rows.length} rows, looking for id:`, row.id);
            return rows.map(r => {
              const match = r.id === row.id;
              if (match) {
                console.log(`[update] FOUND MATCH at depth ${depth}: ${r.id} -> setting children with ${children.length} items`);
              }
              return match
                ? { ...r, children }
                : (r.children ? { ...r, children: update(r.children, depth + 1) } : r);
            });
          };
          const updated = update(prev);
          console.log('[setData] Updated data, row.id:', row.id, 'children.length:', children.length);
          console.log('[setData] First child hasChild:', children[0]?.hasChild, 'first child id:', children[0]?.id);
          return updated;
        });
      }
    }
    setExpandedDimRows(nextExpanded);
  };

  // Load daily data for a specific row - works for both parent and child rows
  const toggleDailyBreakdown = async (e: React.MouseEvent, row: AdRow) => {
    e.preventDefault(); e.stopPropagation();
    console.log('[toggleDailyBreakdown] row.id:', row.id, 'row.filterPath:', row.filterPath);
    const next = new Set(expandedDailyRows);
    if (next.has(row.id)) {
      next.delete(row.id);
    } else {
      next.add(row.id);
      // Check if data is already loaded
      console.log('[toggleDailyBreakdown] dailyDataMap.has(row.id):', dailyDataMap.has(row.id));
      if (!dailyDataMap.has(row.id) && row.filterPath) {
        // Use filterPath from the row - this works for both parent and child rows
        console.log('[toggleDailyBreakdown] using filterPath:', row.filterPath);
        // Load daily data (last 7 days)
        apiLoadDailyData(row.filterPath, 'Last 7 Days', 7).then(dailyData => {
          console.log('[toggleDailyBreakdown] dailyData loaded:', dailyData);
          setDailyDataMap(prev => {
            const newMap = new Map(prev).set(row.id, dailyData);
            console.log('[toggleDailyBreakdown] dailyDataMap size after set:', newMap.size);
            console.log('[toggleDailyBreakdown] dailyDataMap.get(row.id):', newMap.get(row.id));
            return newMap;
          });
        }).catch(err => {
          console.error('Error loading daily data:', err);
        });
      }
    }
    setExpandedDailyRows(next);
  };

  // User CRUD
  const saveUser = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const keywords = (formData.get('keywords') as string).split(',').map(k => k.trim()).filter(Boolean);
    const role = formData.get('role') as string;
    const userData = {
      name: formData.get('name') as string,
      username: formData.get('username') as string,
      password: formData.get('password') as string,
      email: formData.get('email') as string,
      role: role,
      keywords: keywords
    };

    try {
      if (useLocalUsers) {
        // Fallback to local storage
        const newUser: UserPermission = {
          id: editingUser?.id || Date.now().toString(),
          ...userData
        };
        let updated;
        if (editingUser) updated = users.map(u => u.id === editingUser.id ? newUser : u);
        else updated = [...users, newUser];
        setUsers(updated);
        localStorage.setItem('ad_tech_users', JSON.stringify(updated));
      } else {
        // Use API
        if (editingUser) {
          const updatedUser = await usersApi.updateUser(editingUser.id, userData);
          setUsers(users.map(u => u.id === editingUser.id ? updatedUser : u));
        } else {
          const newUser = await usersApi.createUser(userData);
          setUsers([...users, newUser]);
        }
      }
      setShowUserModal(false);
      setEditingUser(null);
    } catch (err) {
      console.error('Failed to save user:', err);
      alert(err instanceof Error ? err.message : 'Failed to save user');
    }
  };

  const deleteUser = async (id: string) => {
    if (!confirm('Delete user?')) return;

    try {
      if (useLocalUsers) {
        // Fallback to local storage
        const updated = users.filter(u => u.id !== id);
        setUsers(updated);
        localStorage.setItem('ad_tech_users', JSON.stringify(updated));
      } else {
        // Use API
        await usersApi.deleteUser(id);
        setUsers(users.filter(u => u.id !== id));
      }
    } catch (err) {
      console.error('Failed to delete user:', err);
      alert(err instanceof Error ? err.message : 'Failed to delete user');
    }
  };

  const visibleMetrics = metrics.filter(m => m.visible);

  return (
    <div className="flex h-screen bg-white overflow-hidden text-slate-900 font-sans">
      <aside className={`bg-[#1e293b] text-slate-400 flex flex-col shrink-0 transition-all ${isSidebarOpen ? 'w-64' : 'w-20'}`}>
        <div className="p-6 border-b border-slate-700/50 flex items-center gap-3">
          <div className="w-8 h-8 bg-indigo-500 rounded-lg text-white flex items-center justify-center font-bold text-xs">EF</div>
          {isSidebarOpen && <span className="text-white font-black text-sm uppercase italic tracking-tighter">Data Insight</span>}
        </div>
        <nav className="flex-1 py-6">
          <button onClick={() => setCurrentPage('performance')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'performance' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
            <i className="fas fa-chart-bar w-5 text-center"></i>
            {isSidebarOpen && <span className="text-sm font-bold">Performance</span>}
          </button>
          {currentUser.role === 'admin' && (
            <button onClick={() => setCurrentPage('daily_report')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'daily_report' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
              <i className="fas fa-calendar-day w-5 text-center"></i>
              {isSidebarOpen && <span className="text-sm font-bold">Daily Report</span>}
            </button>
          )}
          {currentUser.role === 'admin' && (
            <button onClick={() => setCurrentPage('permissions')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'permissions' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
              <i className="fas fa-user-shield w-5 text-center"></i>
              {isSidebarOpen && <span className="text-sm font-bold">Permissions</span>}
            </button>
          )}
        </nav>
        <div className="p-4 border-t border-slate-700/50">
          <button onClick={onLogout} className="w-full flex items-center gap-4 px-2 py-3 hover:bg-slate-800 rounded-lg text-rose-400 transition-colors">
            <i className="fas fa-sign-out-alt w-5 text-center"></i>
            {isSidebarOpen && <span className="text-sm font-bold">Sign Out</span>}
          </button>
        </div>
      </aside>

      <main className="flex-1 flex flex-col min-w-0 bg-[#f8fafc]">
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 z-[100] shadow-sm shrink-0">
          <div className="flex items-center gap-4">
            <button onClick={() => setIsSidebarOpen(!isSidebarOpen)} className="p-2 hover:bg-slate-50 rounded-lg text-slate-400 transition-colors"><i className="fas fa-bars"></i></button>
            <h2 className="font-extrabold text-slate-800 tracking-tight ml-2 uppercase italic text-sm">
              {currentPage === 'performance' ? 'Analytics Data' : currentPage === 'daily_report' ? 'Daily Report' : 'Permissions'}
            </h2>
            {(currentPage === 'performance' || currentPage === 'daily_report') && (
              <DatePicker onRangeChange={(range, start, end) => { setSelectedRange(range); setCustomDateStart(start); setCustomDateEnd(end); }} currentDisplay={dateDisplayString} currentRange={selectedRange} />
            )}
            {/* Data source indicator */}
            {currentPage === 'performance' && (
              <div className={`flex items-center gap-1.5 px-3 py-1 rounded-lg text-[10px] font-bold ${useMock ? 'bg-amber-100 text-amber-700' : 'bg-emerald-100 text-emerald-700'}`}>
                <span className={`w-2 h-2 rounded-full ${useMock ? 'bg-amber-500' : 'bg-emerald-500 animate-pulse'}`}></span>
                <span>{useMock ? 'Mock Data' : 'Live API'}</span>
                <span className="w-px h-3 bg-current opacity-30"></span>
                <span className="font-normal">
                  {etlStatus?.last_update
                    ? `${etlStatus.all_success ? 'all update' : 'part update'} ${etlStatus.last_update}`
                    : 'null'
                  }
                </span>
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            {/* Error notification */}
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
                  <button onClick={() => setShowViewList(!showViewList)} className={`px-4 py-2 bg-white border ${showViewList ? 'border-indigo-500 ring-2 ring-indigo-500/10' : 'border-slate-200'} text-slate-700 rounded-xl text-xs font-bold shadow-sm flex items-center gap-2 hover:bg-slate-50 transition-all`}>
                    <i className="fas fa-bookmark text-amber-500"></i> Views
                  </button>
                  {showViewList && (
                    <div className="absolute top-full mt-2 right-0 w-80 bg-white rounded-2xl shadow-2xl border border-slate-100 z-[100] p-2 animate-in fade-in zoom-in duration-150">
                      <div className="flex items-center justify-between p-3 border-b border-slate-50 mb-1">
                        <span className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Global Views</span>
                        <button onClick={handleSaveView} className="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-[10px] font-black uppercase hover:bg-indigo-700 transition-all active:scale-95 shadow-md shadow-indigo-100"><i className="fas fa-plus mr-1"></i> Save Current</button>
                      </div>
                      <div className="max-h-72 overflow-y-auto custom-scrollbar p-1">
                        {savedViews.length > 0 ? savedViews.map(v => (
                          <div key={v.id} onClick={() => applyView(v)} className="flex items-center justify-between w-full px-3 py-3 text-[11px] font-bold text-slate-600 hover:bg-indigo-50 hover:text-indigo-700 rounded-xl cursor-pointer group transition-all mb-1 border border-transparent hover:border-indigo-100">
                            <div className="flex items-center gap-2 overflow-hidden">
                              <i className="fas fa-table-list text-slate-300 group-hover:text-indigo-400"></i>
                              <span className="truncate">{v.name}</span>
                              {v.isDefault && (
                                <span className="px-1.5 py-0.5 bg-slate-100 text-slate-400 rounded text-[9px] font-bold uppercase tracking-wider">default</span>
                              )}
                            </div>
                            <div className="flex items-center gap-1">
                              <button
                                onClick={(e) => setDefaultView(e, v.id)}
                                className={`w-7 h-7 flex items-center justify-center rounded-lg opacity-0 group-hover:opacity-100 transition-all ${v.isDefault ? 'text-amber-500 bg-amber-50' : 'text-slate-400 hover:text-amber-500 hover:bg-amber-50'}`}
                                title={v.isDefault ? 'Remove as default' : 'Set as default'}
                              >
                                <i className={`fas ${v.isDefault ? 'fa-star' : 'fa-regular fa-star'} text-[10px]`}></i>
                              </button>
                              <button onClick={(e) => deleteView(e, v.id)} className="w-7 h-7 flex items-center justify-center rounded-lg opacity-0 group-hover:opacity-100 text-slate-400 hover:text-rose-500 hover:bg-rose-50 transition-all"><i className="fas fa-trash-can text-[10px]"></i></button>
                            </div>
                          </div>
                        )) : <p className="py-10 text-center text-slate-400 text-[10px]">No saved views</p>}
                      </div>
                    </div>
                  )}
                </div>
                <button onClick={() => setShowColumnEditor(true)} className="px-4 py-2 bg-slate-900 text-white rounded-xl text-xs font-bold shadow-lg shadow-slate-200 flex items-center gap-2 hover:bg-slate-800 transition-colors"><i className="fas fa-columns"></i> Columns</button>
              </div>
            )}
          </div>
        </header>

        {currentPage === 'performance' ? (
          <>
            <div className="px-8 py-5 bg-white border-b border-slate-200 flex flex-col gap-4 z-30 shadow-sm shrink-0">
              <div className="flex items-center gap-4 overflow-x-auto no-scrollbar pb-1">
                <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest whitespace-nowrap">Pivot Layout:</span>
                <div className="flex items-center gap-2">
                  {activeDims.map((dim, idx) => (
                    <div
                      key={dim}
                      draggable
                      onDragStart={(e) => handleDimDragStart(e, idx)}
                      onDragOver={(e) => handleDimDragOver(e)}
                      onDrop={(e) => handleDimDrop(e, idx)}
                      className="flex items-center bg-indigo-50 border border-indigo-100 px-3 py-1.5 rounded-xl gap-2 shadow-sm animate-in fade-in slide-in-from-left-2 cursor-move hover:bg-indigo-100 transition-colors"
                    >
                      <i className="fas fa-grip-vertical text-indigo-300 text-xs"></i>
                      <span className="text-xs font-black text-indigo-700">{ALL_DIMENSIONS.find(d => d.value === dim)?.label}</span>
                      <button onClick={() => toggleDimension(dim)} className="ml-1 text-indigo-200 hover:text-rose-500 transition-colors"><i className="fas fa-times-circle text-xs"></i></button>
                    </div>
                  ))}
                  <div className="h-4 w-px bg-slate-200 mx-2"></div>
                  {ALL_DIMENSIONS.filter(d => !activeDims.includes(d.value)).map(d => (
                    <button key={d.value} onClick={() => toggleDimension(d.value)} className="px-3 py-1.5 border border-dashed border-slate-300 text-slate-400 rounded-xl text-[10px] font-bold hover:border-indigo-400 hover:text-indigo-500 transition-colors">+ {d.label}</button>
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
                  <label className="flex items-center gap-2 px-3 py-1 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors">
                    <input type="checkbox" checked={hideZeroImpressions} onChange={(e) => setHideZeroImpressions(e.target.checked)} className="w-3 h-3 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" />
                    <span className="text-[10px] font-bold text-slate-600">Hide Zero Impressions</span>
                  </label>
                  <label className="flex items-center gap-2 px-3 py-1 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors">
                    <input type="checkbox" checked={colorMode} onChange={(e) => setColorMode(e.target.checked)} className="w-3 h-3 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" />
                    <span className="text-[10px] font-bold text-slate-600">Color Mode</span>
                  </label>
                </div>
                <div className="flex gap-2 items-center">
                  <div className="relative w-64">
                     <i className="fas fa-search absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 text-xs"></i>
                     <input type="text" placeholder="Quick Search..." className="w-full pl-9 pr-4 py-2 bg-slate-50 border border-slate-200 rounded-xl text-xs focus:ring-2 focus:ring-indigo-500/20 outline-none" value={quickFilterText} onChange={(e) => setQuickFilterText(e.target.value)} />
                  </div>
                </div>
              </div>
            </div>

            <div className="flex-1 overflow-auto bg-white custom-scrollbar flex flex-col">
              <div className="flex-1 overflow-auto relative">
                {/* Loading Overlay */}
                {loading && (
                  <div className="absolute inset-0 bg-white/80 backdrop-blur-sm z-50 flex items-center justify-center">
                    <div className="flex flex-col items-center gap-3">
                      <div className="w-10 h-10 border-3 border-indigo-200 border-t-indigo-600 rounded-full animate-spin"></div>
                      <span className="text-sm font-bold text-slate-600">Loading...</span>
                    </div>
                  </div>
                )}
                <table ref={tableRef} className="w-full text-left border-collapse" style={{ minWidth: Object.values(columnWidths).reduce((a, b) => a + b, 0) + 200 }}>
                  <thead>
                    <tr className="bg-slate-50/95 text-[10px] font-black text-slate-400 uppercase tracking-widest border-b border-slate-200 shadow-sm sticky top-0 z-50 backdrop-blur-sm">
                      <th className="px-4 py-4 sticky left-0 bg-slate-50/95 z-[60] border-r border-slate-200 relative" style={{ width: columnWidths.hierarchy }}>
                        Grouping Hierarchy
                        <div
                          className={`absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-indigo-500 ${resizingColumn === 'hierarchy' ? 'bg-indigo-500' : 'bg-transparent'}`}
                          onMouseDown={(e) => handleResizeStart('hierarchy', e)}
                        ></div>
                      </th>
                      {visibleMetrics.map(m => (
                        <th key={m.key} className="px-4 py-4 text-right relative group" style={{ width: columnWidths[m.key] || 120 }}>
                          <div className="flex items-center justify-end gap-1 cursor-pointer hover:text-indigo-600" onClick={() => handleSort(m.key as SortColumn)}>
                            <span>{m.label}</span>
                            <span className="inline-flex w-3">{getSortIcon(m.key as SortColumn)}</span>
                          </div>
                          <div
                            className={`absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-indigo-500 ${resizingColumn === m.key ? 'bg-indigo-500' : 'bg-transparent'}`}
                            onMouseDown={(e) => handleResizeStart(m.key, e)}
                          ></div>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-50">
                    {paginatedData.map((row, idx) => {
                      const isExpanded = expandedDimRows.has(row.id);
                      return (
                      <React.Fragment key={row.id}>
                        <tr className="hover:bg-indigo-50/40 transition-all cursor-pointer group">
                          <td className="px-4 py-3 sticky left-0 bg-white z-10 border-r border-slate-50" style={{ paddingLeft: `${row.level * 20 + 32}px`, width: columnWidths.hierarchy }}>
                            <div className="flex items-center gap-2" onClick={() => {
                              const nextFilters = row.filterPath || row.id.split('|').map((v, i) => ({ dimension: activeDims[i], value: v }));
                              setActiveFilters(nextFilters);
                              setQuickFilterText('');
                            }}>
                              <button onClick={(e) => { e.stopPropagation(); toggleDailyBreakdown(e, row); }} className={`w-5 h-5 rounded-full flex items-center justify-center transition-colors ${expandedDailyRows.has(row.id) ? 'bg-indigo-600 shadow-sm' : 'bg-slate-100'}`}><div className={`w-1.5 h-1.5 rounded-full ${expandedDailyRows.has(row.id) ? 'bg-white' : 'bg-slate-400'}`}></div></button>
                              {row.hasChild && <button onClick={(e) => { e.stopPropagation(); toggleDimExpansion(e, row); }} className={`w-6 h-6 rounded flex items-center justify-center transition-all bg-slate-50 border border-slate-100 text-slate-400 ${isExpanded ? 'rotate-90' : ''}`}><i className="fas fa-chevron-right text-[10px]"></i></button>}
                              <div className="flex flex-col min-w-0">
                                <span className="text-[13px] font-black text-slate-800 truncate group-hover:text-indigo-600">{row.name}</span>
                                <span className="text-[9px] text-slate-400 font-bold uppercase tracking-wider">{ALL_DIMENSIONS.find(d => d.value === row.dimensionType)?.label}</span>
                              </div>
                            </div>
                          </td>
                          {visibleMetrics.map(m => <td key={m.key} className="px-4 py-3 text-right" style={{ width: columnWidths[m.key] || 120 }}><MetricValue value={row[m.key] as number} type={m.type} colorMode={colorMode} metricKey={m.key as string} /></td>)}
                        </tr>
                        {expandedDailyRows.has(row.id) && (() => {
                          const dailyData = dailyDataMap.get(row.id);
                          console.log('[Render] row.id:', row.id, 'expandedDailyRows.has:', expandedDailyRows.has(row.id), 'dailyData:', dailyData);
                          return dailyData?.slice(0, 7).map(day => (
                          <tr key={day.date} className="bg-slate-50/50">
                            <td className="px-4 py-2 sticky left-0 bg-slate-50 z-10 border-l-4 border-indigo-600/60 border-r border-slate-50" style={{ paddingLeft: `${row.level * 20 + 72}px`, width: columnWidths.hierarchy }}><span className="text-[12px] font-bold text-slate-500">{day.date}</span></td>
                            {visibleMetrics.map(m => <td key={m.key} className="px-4 py-2 text-right opacity-80" style={{ width: columnWidths[m.key] || 120 }}><MetricValue value={day[m.key as keyof DailyBreakdown] as number || 0} type={m.type} isSub colorMode={colorMode} metricKey={m.key as string} /></td>)}
                          </tr>
                          ));
                        })()}
                      </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* Summary Row - Integrated with Pagination */}
              {totalRows > 0 && (
                <>
                  {/* Summary Bar */}
                  <div className="border-t border-slate-200 bg-slate-50 px-6 py-2 flex items-center gap-6 shrink-0 overflow-x-auto">
                    <span className="text-[10px] font-black uppercase text-slate-400 tracking-widest shrink-0">Summary:</span>
                    <div className="flex items-center gap-4 text-xs">
                      <div className="flex items-center gap-1">
                        <span className="text-slate-400 font-bold">Imp:</span>
                        <span className="font-mono font-bold text-slate-700">{Math.floor(summaryData.impressions).toLocaleString()}</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <span className="text-slate-400 font-bold">Click:</span>
                        <span className="font-mono font-bold text-slate-700">{Math.floor(summaryData.clicks).toLocaleString()}</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <span className="text-slate-400 font-bold">Conv:</span>
                        <span className="font-mono font-bold text-slate-700">{Math.floor(summaryData.conversions).toLocaleString()}</span>
                      </div>
                      <div className="w-px h-4 bg-slate-200"></div>
                      <div className="flex items-center gap-1">
                        <span className="text-slate-400 font-bold">Spend:</span>
                        <span className="font-mono font-bold text-slate-700">${summaryData.spend.toFixed(2)}</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <span className="text-slate-400 font-bold">Rev:</span>
                        <span className="font-mono font-bold text-slate-700">${summaryData.revenue.toFixed(2)}</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <span className="text-slate-400 font-bold">Profit:</span>
                        <span className={`font-mono font-bold ${summaryData.profit > 0 ? 'text-emerald-600' : summaryData.profit < 0 ? 'text-rose-600' : 'text-slate-700'}`}>${summaryData.profit.toFixed(2)}</span>
                      </div>
                      <div className="w-px h-4 bg-slate-200"></div>
                      <div className="flex items-center gap-1">
                        <span className="text-slate-400 font-bold">ROI:</span>
                        <span className={`font-mono font-bold ${summaryData.roi > 0 ? 'text-emerald-600' : summaryData.roi < 0 ? 'text-rose-600' : 'text-slate-700'}`}>{(summaryData.roi * 100).toFixed(2)}%</span>
                      </div>
                    </div>
                  </div>

                  {/* Pagination Bar */}
                  <div className="border-t border-slate-200 bg-white px-6 py-3 flex items-center justify-between shrink-0">
                    <div className="text-[11px] text-slate-500">
                      Showing <span className="font-bold text-slate-700">{Math.min((paginationPage - 1) * rowsPerPage + 1, totalRows)}</span> to <span className="font-bold text-slate-700">{Math.min(paginationPage * rowsPerPage, totalRows)}</span> of <span className="font-bold text-slate-700">{totalRows}</span> rows
                    </div>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => setPaginationPage(1)}
                      disabled={paginationPage === 1}
                      className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      <i className="fas fa-angle-double-left"></i>
                    </button>
                    <button
                      onClick={() => setPaginationPage(p => Math.max(1, p - 1))}
                      disabled={paginationPage === 1}
                      className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      <i className="fas fa-chevron-left"></i>
                    </button>
                    {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      let pageNum;
                      if (totalPages <= 5) {
                        pageNum = i + 1;
                      } else if (paginationPage <= 3) {
                        pageNum = i + 1;
                      } else if (paginationPage >= totalPages - 2) {
                        pageNum = totalPages - 4 + i;
                      } else {
                        pageNum = paginationPage - 2 + i;
                      }
                      return (
                        <button
                          key={pageNum}
                          onClick={() => setPaginationPage(pageNum)}
                          className={`px-3 py-1 text-[11px] font-bold rounded ${paginationPage === pageNum ? 'bg-indigo-600 text-white' : 'hover:bg-slate-100 text-slate-600'}`}
                        >
                          {pageNum}
                        </button>
                      );
                    })}
                    <button
                      onClick={() => setPaginationPage(p => Math.min(totalPages, p + 1))}
                      disabled={paginationPage === totalPages}
                      className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      <i className="fas fa-chevron-right"></i>
                    </button>
                    <button
                      onClick={() => setPaginationPage(totalPages)}
                      disabled={paginationPage === totalPages}
                      className="px-2 py-1 text-[10px] font-bold rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      <i className="fas fa-angle-double-right"></i>
                    </button>
                  </div>
                </div>
                </>
              )}
            </div>
          </>
        ) : currentPage === 'daily_report' ? (
          <DailyReportPage
            selectedRange={selectedRange}
            customDateStart={customDateStart}
            customDateEnd={customDateEnd}
            onRangeChange={(range, start, end) => {
              setSelectedRange(range);
              setCustomDateStart(start);
              setCustomDateEnd(end);
            }}
            currentUser={currentUser}
          />
        ) : (
          <div className="flex-1 p-12 overflow-auto bg-slate-50/50">
             <div className="max-w-6xl mx-auto space-y-6">
                <div className="flex justify-between items-center mb-8">
                  <div>
                    <h3 className="text-2xl font-black italic uppercase tracking-tighter">System Permissions</h3>
                    <p className="text-slate-500 text-sm">Manage user roles and data visibility via keywords. OPS filters by Adset, Business filters by Offer.</p>
                  </div>
                  {currentUser.id === 'admin' && (
                    <button onClick={() => { setEditingUser(null); setShowUserModal(true); }} className="bg-indigo-600 text-white px-6 py-3 rounded-2xl font-black text-xs uppercase tracking-widest shadow-lg shadow-indigo-100 hover:bg-indigo-700 transition-all active:scale-95">Add New User</button>
                  )}
                </div>

                <div className="grid grid-cols-1 gap-4">
                  <div className="bg-indigo-600 text-white p-6 rounded-3xl shadow-xl flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className="w-12 h-12 bg-white/20 rounded-xl flex items-center justify-center text-xl font-bold">{currentUser.name.charAt(0)}</div>
                      <div>
                        <div className="font-black text-lg">{currentUser.name} (Current)</div>
                        <div className="text-xs text-indigo-100 font-bold uppercase tracking-widest">{currentUser.email}</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="px-3 py-1 bg-white/20 rounded-lg text-[10px] font-black uppercase inline-block">Role: {currentUser.role || 'admin'}</div>
                      <div className="text-[10px] text-indigo-100 mt-2 font-bold uppercase italic tracking-widest">Keywords: {currentUser.keywords?.join(', ') || 'ALL ACCESS'}</div>
                    </div>
                  </div>

                  {currentUser.id === 'admin' && users.map(u => (
                    <div key={u.id} className="bg-white p-6 rounded-3xl border border-slate-100 shadow-sm hover:shadow-md transition-all flex items-center justify-between group">
                      <div className="flex items-center gap-4">
                        <div className="w-12 h-12 bg-slate-100 rounded-xl flex items-center justify-center text-slate-400 font-bold group-hover:bg-indigo-50 group-hover:text-indigo-600 transition-colors">{u.name.charAt(0)}</div>
                        <div>
                          <div className="font-black text-slate-800">{u.name}</div>
                          <div className="text-xs text-slate-400 font-bold uppercase tracking-widest">{u.email}</div>
                        </div>
                      </div>
                      <div className="flex items-center gap-8">
                        <div className="text-right">
                          <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mb-1">Role</div>
                          <div className="px-2 py-0.5 bg-slate-50 border border-slate-100 text-slate-500 rounded text-[9px] font-bold">{u.role || 'ops'}</div>
                        </div>
                        <div className="text-right">
                          <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mb-1">Keywords</div>
                          <div className="flex gap-1 justify-end flex-wrap max-w-xs">
                             {u.keywords?.length > 0 ? u.keywords.map(kw => (
                               <span key={kw} className="px-2 py-0.5 bg-slate-50 border border-slate-100 text-slate-500 rounded text-[9px] font-bold">{kw}</span>
                             )) : <span className="text-[9px] text-slate-300 italic">All Access</span>}
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <button onClick={() => { setEditingUser(u); setShowUserModal(true); }} className="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center hover:bg-indigo-100 transition-colors"><i className="fas fa-edit text-xs"></i></button>
                          <button onClick={() => deleteUser(u.id)} className="w-10 h-10 bg-rose-50 text-rose-600 rounded-xl flex items-center justify-center hover:bg-rose-100 transition-colors"><i className="fas fa-trash-alt text-xs"></i></button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
             </div>
          </div>
        )}
      </main>

      {/* User Modal */}
      {showUserModal && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-6">
          <div className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onClick={() => setShowUserModal(false)}></div>
          <form onSubmit={saveUser} className="relative w-full max-w-lg bg-white rounded-3xl shadow-2xl p-8 animate-in zoom-in duration-200">
            <h3 className="text-2xl font-black uppercase italic tracking-tighter mb-6">{editingUser ? 'Edit User' : 'Create New Agent'}</h3>
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Display Name</label>
                  <input required name="name" defaultValue={editingUser?.name} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                </div>
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Email Address</label>
                  <input required type="email" name="email" defaultValue={editingUser?.email} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Username <span className="text-slate-300 font-normal">(min 3 chars)</span></label>
                  <input required name="username" minLength={3} defaultValue={editingUser?.username} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" placeholder="username" />
                </div>
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Password <span className="text-slate-300 font-normal">(min 6 chars)</span></label>
                  <input required type="password" name="password" minLength={6} defaultValue={editingUser?.password} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" placeholder="••••••" />
                </div>
              </div>
              <div className="space-y-1">
                <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Role</label>
                <select name="role" defaultValue={editingUser?.role || 'ops'} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20">
                  <option value="admin">Admin (Full Access)</option>
                  <option value="ops">OPS (Adset Filter)</option>
                  <option value="ops02">OPS02 (Platform Filter)</option>
                  <option value="business">Business (Offer Filter)</option>
                </select>
              </div>
              <div className="space-y-1">
                <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Keywords (Comma separated)</label>
                <input name="keywords" defaultValue={editingUser?.keywords?.join(', ') || ''} placeholder="e.g. ZP, Zp, zp" className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                <p className="text-[9px] text-slate-400 font-bold mt-1 italic">* OPS: Adset, OPS02: Platform, Business: Offer. Empty = all access.</p>
              </div>
            </div>
            <div className="mt-8 flex gap-3">
              <button type="button" onClick={() => setShowUserModal(false)} className="flex-1 py-4 bg-slate-100 text-slate-600 rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-slate-200 transition-all">Cancel</button>
              <button type="submit" className="flex-1 py-4 bg-indigo-600 text-white rounded-2xl font-black text-xs uppercase tracking-widest shadow-xl shadow-indigo-100 hover:bg-indigo-700 transition-all active:scale-[0.98]">Save Permission</button>
            </div>
          </form>
        </div>
      )}

      {showColumnEditor && (
        <div className="fixed inset-0 z-[100] flex justify-end">
          <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm" onClick={() => setShowColumnEditor(false)}></div>
          <div className="relative w-[720px] bg-white h-full shadow-2xl flex flex-col p-8 animate-in slide-in-from-right duration-200">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-2xl font-black uppercase italic tracking-tighter">Manage Columns</h3>
              <button onClick={() => setShowColumnEditor(false)} className="w-10 h-10 flex items-center justify-center rounded-full hover:bg-slate-100 text-slate-400 transition-colors"><i className="fas fa-times"></i></button>
            </div>
            <div className="flex-1 flex min-h-0 gap-6">
              <div className="flex-1 flex flex-col min-h-0 bg-slate-50/50 rounded-2xl border border-slate-100 p-5 overflow-y-auto custom-scrollbar">
                {(['Basic', 'Calculated'] as const).map(groupName => (
                  <div key={groupName} className="mb-6">
                    <span className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-3 block">{groupName} Metrics</span>
                    <div className="space-y-2">
                      {metrics.filter(m => m.group === groupName).map(m => (
                        <label key={m.key} className={`flex items-center gap-3 p-3 border rounded-xl cursor-pointer transition-all ${m.visible ? 'border-indigo-500 bg-indigo-50' : 'border-slate-100 bg-white'}`}>
                          <input type="checkbox" checked={m.visible} onChange={() => setMetrics(prev => prev.map(p => p.key === m.key ? { ...p, visible: !p.visible } : p))} className="w-4 h-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-50" />
                          <span className="text-xs font-bold text-slate-700">{m.label}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
              <div className="w-[280px] bg-indigo-50/20 rounded-2xl border border-indigo-100 p-5 flex flex-col overflow-y-auto custom-scrollbar">
                <span className="text-[10px] font-black uppercase tracking-widest text-indigo-600 mb-4 block">Selected Order</span>
                {metrics.filter(m => m.visible).map((m, idx) => (
                   <div key={m.key} draggable onDragStart={() => dragItem.current = idx} onDragEnter={() => dragOverItem.current = idx} onDragEnd={handleMetricReorder} onDragOver={e => e.preventDefault()} className="p-3 bg-white border border-indigo-200 rounded-xl flex items-center gap-3 shadow-sm mb-2 cursor-grab">
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

const App: React.FC = () => {
  const [user, setUser] = useState<UserPermission | null>(null);
  const handleLogout = () => {
    authApi.logout();  // Clear token and user from localStorage
    setUser(null);
  };
  if (!user) return <LoginPage onLogin={setUser} />;
  return <Dashboard currentUser={user} onLogout={handleLogout} />;
};

export default App;
