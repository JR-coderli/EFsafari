
import { GoogleGenAI } from "@google/genai";
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { createPortal } from 'react-dom';
import { AdRow, Dimension, MetricConfig, SavedView, UserPermission, DailyBreakdown } from './types';
import { generateMockReport } from './mockData';
import { loadRootData as apiLoadRootData, loadChildData, loadDailyData as apiLoadDailyData } from './src/api/hooks';
import { authApi, usersApi, tokenManager } from './src/api/auth';
import { dailyReportApi, dashboardApi, onConnectionStatusChange, type ConnectionStatus } from './src/api/client';
import { viewsApi } from './src/api/views';
import DailyReport from './components/DailyReport';
import HourlyReport from './components/HourlyReport';

interface Filter {
  dimension: Dimension;
  value: string;
}

interface ETLStatus {
  last_update: string | null;
  report_date: string | null;
  all_success: boolean;
}

const calculateMetrics = (data: { impressions: number; clicks: number; conversions: number; spend: number; revenue: number; m_imp: number; m_clicks: number; m_conv: number }) => {
  return {
    ctr: data.clicks / (data.impressions || 1),
    cvr: data.conversions / (data.clicks || 1),
    roi: data.spend > 0 ? (data.revenue - data.spend) / data.spend : 0,
    cpa: data.spend / (data.conversions || 1),
    rpa: data.revenue / (data.conversions || 1),
    epc: data.revenue / (data.clicks || 1),
    epv: data.revenue / (data.impressions || 1),
    m_epc: data.revenue / (data.m_clicks || 1),
    m_epv: data.revenue / (data.m_imp || 1),
    m_cpc: data.spend / (data.m_clicks || 1),
    m_cpv: data.spend / (data.m_imp || 1),
    m_cpa: data.spend / (data.m_conv || 1),
    m_epa: data.revenue / (data.m_conv || 1)
  };
};

const ALL_DIMENSIONS: { value: Dimension; label: string }[] = [
  { value: 'platform', label: 'Media' },
  { value: 'advertiser', label: 'Advertiser' },
  { value: 'offer', label: 'Offer' },
  { value: 'lander', label: 'Lander' },
  { value: 'campaign_name', label: 'Campaign' },
  { value: 'sub_campaign_name', label: 'Adset' },
  { value: 'creative_name', label: 'Ads' },
  { value: 'date', label: 'Date' },
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
  { key: 'm_epc', label: 'm_EPC', visible: false, type: 'money', group: 'Calculated' },
  { key: 'm_epv', label: 'm_EPV', visible: false, type: 'money', group: 'Calculated' },
  { key: 'm_cpa', label: 'm_CPA', visible: false, type: 'money', group: 'Calculated' },
  { key: 'm_epa', label: 'm_EPA', visible: false, type: 'money', group: 'Calculated' },
];

const MetricValue: React.FC<{ value: number; type: 'money' | 'percent' | 'number' | 'profit'; isSub?: boolean; colorMode?: boolean; metricKey?: string }> = ({ value, type, isSub, colorMode, metricKey }) => {
  const displayValue = isFinite(value) ? value : 0;

  // Profit always has color (positive=green, negative=red) - regardless of isSub
  if (type === 'profit') {
    const colorClass = displayValue > 0 ? 'text-emerald-600' : displayValue < 0 ? 'text-rose-600' : 'text-slate-800';
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    return <span className={`font-mono tracking-tight leading-none font-bold ${colorClass} ${sizeClass}`}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
  }

  // ROI always has color (positive=green, negative=red) - regardless of isSub
  if (metricKey === 'roi') {
    const colorClass = displayValue > 0 ? 'text-emerald-600' : displayValue < 0 ? 'text-rose-600' : 'text-slate-800';
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    return <span className={`font-mono tracking-tight leading-none font-bold ${colorClass} ${sizeClass}`}>{(displayValue * 100).toFixed(2)}%</span>;
  }

  // Color mode for specific metrics (only for parent rows)
  let colorClasses = '';
  if (colorMode && !isSub) {
    if (metricKey === 'revenue') colorClasses = 'text-amber-500';      // 黄色
    else if (metricKey === 'spend') colorClasses = 'text-rose-500';    // 红色
    else if (metricKey === 'cpa') colorClasses = 'text-blue-500';      // 蓝色
    else if (metricKey === 'epa') colorClasses = 'text-amber-500';     // 黄色
    else if (metricKey === 'epc') colorClasses = 'text-amber-500';     // 黄色
    else if (metricKey === 'epv') colorClasses = 'text-amber-500';     // 黄色
    else if (metricKey === 'm_cpa') colorClasses = 'text-blue-500';    // 蓝色
    else if (metricKey === 'm_epa') colorClasses = 'text-amber-500';   // 黄色
  }

  const baseClasses = `font-mono tracking-tight leading-none ${isSub ? 'text-[14px] text-slate-700 font-bold' : `text-[14px] ${colorClasses} font-bold`}`;

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
  // This month: from 1st to today
  const thisMonthStart = new Date(today.getFullYear(), today.getMonth(), 1);

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
              {['Today', 'Yesterday', 'Last 7 Days', 'Last 14 Days', 'Last 30 Days', 'This Month'].map(r => (
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
  // Debug: Log currentUser on mount
  console.log('Dashboard mounted with currentUser:', JSON.stringify(currentUser, null, 2));
  console.log('currentUser.showRevenue:', currentUser.showRevenue, 'type:', typeof currentUser.showRevenue);
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<AdRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [useMock, setUseMock] = useState(false);  // Fallback to mock if API fails
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('connected');
  // 初始不设置维度，等待默认视图加载完成后才设置
  const [activeDims, setActiveDims] = useState<Dimension[]>([]);
  const [editingDimIndex, setEditingDimIndex] = useState<number | null>(null);
  const [dropdownPosition, setDropdownPosition] = useState<{ top: number; left: number } | null>(null);
  const [metrics, setMetrics] = useState<MetricConfig[]>(DEFAULT_METRICS);
  type PageType = 'performance' | 'permissions' | 'daily_report' | 'hourly';
  const [currentPage, setCurrentPage] = useState<PageType>('performance');
  // Performance 子菜单状态
  const [performanceSubPage, setPerformanceSubPage] = useState<'dates' | 'hourly'>('dates');
  // Performance 菜单展开状态
  const [performanceMenuOpen, setPerformanceMenuOpen] = useState(true);
  const [activeFilters, setActiveFilters] = useState<Filter[]>([]);
  const [expandedDailyRows, setExpandedDailyRows] = useState<Set<string>>(new Set());
  const [loadingDailyRows, setLoadingDailyRows] = useState<Set<string>>(new Set());
  const [loadingDimRows, setLoadingDimRows] = useState<Set<string>>(new Set());
  const [expandedDimRows, setExpandedDimRows] = useState<Set<string>>(new Set());
  // Store daily data separately to avoid reference issues with flattened data
  const [dailyDataMap, setDailyDataMap] = useState<Map<string, DailyBreakdown[]>>(new Map());
  // Performance page: default to Yesterday
  const [selectedRange, setSelectedRange] = useState('Yesterday');
  const [customDateStart, setCustomDateStart] = useState<Date | undefined>(undefined);
  const [customDateEnd, setCustomDateEnd] = useState<Date | undefined>(undefined);
  // Hourly Insight page: default to Today
  const [hourlyRange, setHourlyRange] = useState('Today');
  const [hourlyDateStart, setHourlyDateStart] = useState<Date | undefined>(undefined);
  const [hourlyDateEnd, setHourlyDateEnd] = useState<Date | undefined>(undefined);
  // Daily Report page: default to This Month
  const [dailyReportRange, setDailyReportRange] = useState('This Month');
  const [dailyReportStart, setDailyReportStart] = useState<Date | undefined>(undefined);
  const [dailyReportEnd, setDailyReportEnd] = useState<Date | undefined>(undefined);
  const [quickFilterText, setQuickFilterText] = useState('');
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [showColumnEditor, setShowColumnEditor] = useState(false);
  const [showViewList, setShowViewList] = useState(false);
  const viewsDropdownRef = useRef<HTMLDivElement>(null);

  // Context menu state
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; text: string } | null>(null);
  const contextMenuRef = useRef<HTMLDivElement>(null);

  // Pagination state
  const [paginationPage, setPaginationPage] = useState(1);
  const [rowsPerPage] = useState(20);

  // Sort state
  type SortColumn = 'revenue' | 'spend' | 'impressions' | 'clicks' | 'conversions' | 'ctr' | 'cvr' | 'roi' | 'cpa' | 'rpa' | 'epc' | 'epv' | 'm_epc' | 'm_epv' | 'm_cpc' | 'm_cpv' | 'm_cpa' | 'm_epa' | 'm_imp' | 'm_clicks' | 'm_conv';
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

  // 追踪初始化是否完成（用于防止初始化期间重复请求）
  const [isInitialized, setIsInitialized] = useState(false);
  const hasLoadedDataAfterInit = useRef(false);

  // Computed display string for the date picker - varies by page
  const dateDisplayString = useMemo(() => {
    if (currentPage === 'daily_report') {
      return getRangeInfo(dailyReportRange, dailyReportStart, dailyReportEnd).dateString;
    }
    if (currentPage === 'hourly') {
      return getRangeInfo(hourlyRange, hourlyDateStart, hourlyDateEnd).dateString;
    }
    return getRangeInfo(selectedRange, customDateStart, customDateEnd).dateString;
  }, [currentPage, selectedRange, customDateStart, customDateEnd, dailyReportRange, dailyReportStart, dailyReportEnd, hourlyRange, hourlyDateStart, hourlyDateEnd]);

  // Reset pagination when filters or data changes
  useEffect(() => {
    setPaginationPage(1);
  }, [activeFilters, quickFilterText, selectedRange, customDateStart, customDateEnd]);

  // Reset mock mode when currentUser changes (user just logged in)
  useEffect(() => {
    if (useMock) {
      setUseMock(false);
      setError(null);
    }
  }, [currentUser.id]);

  // Subscribe to connection status changes
  useEffect(() => {
    const unsubscribe = onConnectionStatusChange((status) => {
      setConnectionStatus(status);
    });
    return unsubscribe;
  }, []);

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

  // Get row text for copy - only return the dimension name
  const getRowText = (row: RowData): string => {
    return row.name;
  };

  // Context menu handlers
  const handleContextMenu = (e: React.MouseEvent, text: string) => {
    e.preventDefault();
    setContextMenu({ x: e.clientX, y: e.clientY, text });
  };

  const handleCopyText = () => {
    if (contextMenu) {
      navigator.clipboard.writeText(contextMenu.text);
      setContextMenu(null);
    }
  };

  const closeContextMenu = () => setContextMenu(null);

  // Dimension replacement
  const replaceDimension = (index: number, newDim: Dimension) => {
    if (activeDims.includes(newDim)) return; // Prevent duplicates
    const newDims = [...activeDims];
    newDims[index] = newDim;

    // 清理无效的 filters：只保留那些在新维度顺序中仍然处于正确位置的 filters
    const validFilters = activeFilters.filter((filter, filterIndex) => {
      // filter的dimension必须在新维度顺序的对应位置上
      return newDims[filterIndex] === filter.dimension;
    });

    setActiveDims(newDims);
    setActiveFilters(validFilters);
    setEditingDimIndex(null);
    setDropdownPosition(null);
  };

  // Open dropdown and calculate position
  const openDimDropdown = (idx: number, e: React.MouseEvent) => {
    const target = e.currentTarget as HTMLElement;
    const rect = target.getBoundingClientRect();
    // Find the table header to account for its height
    const tableHeader = document.querySelector('thead');
    const headerHeight = tableHeader ? tableHeader.getBoundingClientRect().height : 0;
    // Position below the clicked element, accounting for sticky header
    setDropdownPosition({
      top: Math.max(rect.bottom + 4, headerHeight + 4),
      left: rect.left
    });
    setEditingDimIndex(idx);
  };

  // Close dropdown on click outside (using event delegation)
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      // Check if click is outside any dimension dropdown
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

  // Close context menu on click outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (contextMenuRef.current && !contextMenuRef.current.contains(e.target as Node)) {
        closeContextMenu();
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [contextMenu]);

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
    // Special handling for 'date' dimension: always sort by date (name) descending
    const sortRows = (rows: AdRow[]): AdRow[] => {
      if (rows.length === 0) return rows;

      // Check if this level is 'date' dimension - use date descending sort
      const isDateDimension = rows[0].dimensionType === 'date';
      if (isDateDimension) {
        return [...rows].sort((a, b) => {
          // Sort by name (date string) descending
          return b.name.localeCompare(a.name);
        });
      }

      // For non-date dimensions, use the selected sort
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
        // 空格分隔的关键词 = OR 查询（例如 "im mx" 匹配包含 im 或 mx 的行）
        const searchTerms = quickFilterText.toLowerCase().trim().split(/\s+/).filter(k => k);
        const matchesFilter = searchTerms.length === 0 || searchTerms.some(term => row.name.toLowerCase().includes(term));
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
          // Sort the children (will use date sort if child dimension is 'date')
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
        // 重置为默认排序列（使用第一个可见的列）
        const defaultSortColumn = visibleMetrics.find(m => m.key === 'spend')?.key || visibleMetrics[0]?.key || 'spend';
        setSortColumn(defaultSortColumn as SortColumn);
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
  const dragItemKey = useRef<string | null>(null);
  const dragOverItemKey = useRef<string | null>(null);

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
    if (currentPage !== 'performance') return;
    // 等待初始化完成
    if (!isInitialized) return;
    // activeDims 为空说明还在初始化
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

      // Don't auto-fallback to mock - let retry mechanism handle it
      // Users can manually enable mock mode via UI if needed
      setLoading(false);
      return;
    } finally {
      if (!useMock) {
        setLoading(false);
      }
    }
  }, [activeDims, activeFilters, selectedRange, customDateStart, customDateEnd, currentUser, useMock, currentPage, isInitialized]);

  // Trigger loadRootData when dependencies change（初始化完成后）
  useEffect(() => {
    if (currentPage !== 'performance') return;
    // 如果还没初始化，不执行（初始化完成后会手动触发）
    if (!hasLoadedDataAfterInit.current) return;
    loadRootData();
  }, [loadRootData, currentPage]);

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
  const hasLoadedDefaultView = useRef(false);
  const loadRootDataRef = useRef(loadRootData);

  // Keep the ref updated
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
          // 新用户没有默认视图，应用预设的初始视图
          setActiveDims(['platform', 'sub_campaign_name']);
        }
      } catch (error) {
        console.error('Failed to load default view:', error);
        // 降级：应用预设的初始视图
        setActiveDims(['platform', 'sub_campaign_name']);
      }
      // 所有维度设置完成后，标记为已初始化
      setIsInitialized(true);
      // 标记初始化后数据已加载
      hasLoadedDataAfterInit.current = true;
      // 手动触发一次数据加载
      loadRootDataRef.current();
    };
    loadDefaultView();
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
    // Don't need to manually call loadRootData - the useEffect will handle it
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

    // 保留有效的 filters：只保留那些在新维度顺序中仍然处于正确位置的 filters
    // 例如：原层级 platform->sub_campaign->offer，当前filter [{platform: Facebook}, {sub_campaign: AdSet1}]
    // 调换后 platform->offer->sub_campaign，则只保留 [{platform: Facebook}]，因为offer层级没有filter
    const validFilters = activeFilters.filter((filter, index) => {
      // filter的dimension必须在新维度顺序的对应位置上
      return newDims[index] === filter.dimension;
    });

    setActiveDims(newDims);
    setActiveFilters(validFilters);
    draggedDimIndex.current = null;
  };

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
    e.preventDefault(); e.stopPropagation();
    if (!row.hasChild) {
      return;
    }
    const nextExpanded = new Set(expandedDimRows);
    if (nextExpanded.has(row.id)) {
      // Collapsing - just remove from expanded
      nextExpanded.delete(row.id);
      setExpandedDimRows(nextExpanded);
    } else {
      // Expanding - add to expanded and load data if needed
      nextExpanded.add(row.id);
      setExpandedDimRows(nextExpanded);
      if (!row.children || row.children.length === 0) {
        // Add to loading state
        setLoadingDimRows(prev => new Set(prev).add(row.id));

        const nextLevel = row.level + 1;
        let children: AdRow[];

        // Build filters for this row's hierarchy - use filterPath if available (for child rows)
        const rowFilters = row.filterPath || row.id.split('|').map((v, i) => ({
          dimension: activeDims[i],
          value: v
        }));

        if (useMock) {
          // Use mock data
          children = generateMockReport(
            activeDims[nextLevel],
            nextLevel,
            row.id,
            activeDims.slice(nextLevel + 1),
            selectedRange
          );
          // Remove from loading state immediately for mock data
          setLoadingDimRows(prev => {
            const nextLoading = new Set(prev);
            nextLoading.delete(row.id);
            return nextLoading;
          });
        } else {
          // Use real API
          try {
            // Pass row's filterPath to load child data
            children = await loadChildData(activeDims, rowFilters, selectedRange, row.id, customDateStart, customDateEnd);
          } catch (err) {
            console.error('Error loading child data:', err);
            children = [];
          } finally {
            // Remove from loading state
            setLoadingDimRows(prev => {
              const nextLoading = new Set(prev);
              nextLoading.delete(row.id);
              return nextLoading;
            });
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

  // Load daily data for a specific row - works for both parent and child rows
  const toggleDailyBreakdown = async (e: React.MouseEvent, row: AdRow) => {
    e.preventDefault(); e.stopPropagation();
    const next = new Set(expandedDailyRows);
    if (next.has(row.id)) {
      // Collapsing - just remove from expanded
      next.delete(row.id);
      setExpandedDailyRows(next);
    } else {
      // Expanding - add to expanded and load data if needed
      next.add(row.id);
      setExpandedDailyRows(next);
      if (!dailyDataMap.has(row.id) && row.filterPath) {
        // Add to loading state
        setLoadingDailyRows(prev => new Set(prev).add(row.id));
        // Use filterPath from the row - this works for both parent and child rows
        // Load daily data (last 7 days)
        apiLoadDailyData(row.filterPath, 'Last 7 Days', 7).then(dailyData => {
          setDailyDataMap(prev => new Map(prev).set(row.id, dailyData));
        }).catch(err => {
          console.error('Error loading daily data:', err);
        }).finally(() => {
          // Remove from loading state
          setLoadingDailyRows(prev => {
            const nextLoading = new Set(prev);
            nextLoading.delete(row.id);
            return nextLoading;
          });
        });
      }
    }
  };

  // User CRUD
  const saveUser = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const keywords = (formData.get('keywords') as string).split(',').map(k => k.trim()).filter(Boolean);
    const role = formData.get('role') as string;

    // 直接使用 editingUser 的 showRevenue 值，因为 checkbox 是受控组件
    // 如果没有 editingUser（新建用户），默认为 true
    const showRevenue = editingUser?.showRevenue !== false;

    const userData = {
      name: formData.get('name') as string,
      username: formData.get('username') as string,
      password: formData.get('password') as string,
      email: formData.get('email') as string,
      role: role,
      keywords: keywords,
      showRevenue: showRevenue
    };
    console.log('Saving user:', { id: editingUser?.id, showRevenue, editingUserShowRevenue: editingUser?.showRevenue });

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
        console.log('Calling API with userData:', userData);
        if (editingUser) {
          const updatedUser = await usersApi.updateUser(editingUser.id, userData);
          console.log('API returned updatedUser:', JSON.stringify(updatedUser, null, 2));
          console.log('updatedUser.showRevenue:', updatedUser.showRevenue, 'type:', typeof updatedUser.showRevenue);
          setUsers(users.map(u => u.id === editingUser.id ? updatedUser : u));
        } else {
          const newUser = await usersApi.createUser(userData);
          console.log('API returned newUser:', JSON.stringify(newUser, null, 2));
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

  const visibleMetrics = metrics.filter(m => {
    if (!m.visible) return false;
    // 如果用户没有权限查看收入，隐藏 revenue 相关列
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

      <aside className={`bg-[#1e293b] text-slate-400 flex flex-col shrink-0 transition-all ${isSidebarOpen ? 'w-64' : 'w-20'}`}>
        <div className="p-6 border-b border-slate-700/50 flex items-center gap-3">
          <div className="w-8 h-8 bg-indigo-500 rounded-lg text-white flex items-center justify-center font-bold text-xs">EF</div>
          {isSidebarOpen && <span className="text-white font-black text-sm uppercase italic tracking-tighter">Data Insight</span>}
        </div>
        <nav className="flex-1 py-6">
          {/* Performance - 可展开的一级菜单 */}
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
            {/* 二级菜单 */}
            {performanceMenuOpen && isSidebarOpen && (
              <div className="ml-6 border-l border-slate-700/50">
                <button
                  onClick={() => { setCurrentPage('performance'); setPerformanceSubPage('dates'); }}
                  className={`w-full flex items-center gap-4 px-6 py-3 transition-colors ${currentPage === 'performance' && performanceSubPage === 'dates' ? 'text-indigo-400 bg-slate-800/50' : 'text-slate-500 hover:bg-slate-800/30'}`}
                >
                  <span className="w-2"></span>
                  <i className="fas fa-calendar-alt w-4 text-center text-xs"></i>
                  <span className="text-xs font-bold">Dates Report</span>
                </button>
                {currentUser.showRevenue !== false && (
                  <button
                    onClick={() => { setCurrentPage('hourly'); setPerformanceSubPage('hourly'); }}
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
              {currentPage === 'performance' && performanceSubPage === 'dates' ? 'Dates Report' :
               currentPage === 'hourly' ? 'Hourly Insight' :
               currentPage === 'daily_report' ? 'Daily Report' : 'Permissions'}
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
                  }
                }}
                currentDisplay={dateDisplayString}
                currentRange={currentPage === 'daily_report' ? dailyReportRange : currentPage === 'hourly' ? hourlyRange : selectedRange}
              />
            )}
            {/* Data source indicator */}
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
                  {activeDims.map((dim, idx) => {
                    const dimLabel = ALL_DIMENSIONS.find(d => d.value === dim)?.label;
                    return (
                    <div key={dim} className="relative" data-dim-dropdown="true">
                      <div
                        draggable
                        onDragStart={(e) => handleDimDragStart(e, idx)}
                        onDragOver={(e) => handleDimDragOver(e)}
                        onDrop={(e) => handleDimDrop(e, idx)}
                        className="flex items-center bg-indigo-50 border border-indigo-100 px-3 py-1.5 rounded-xl gap-2 shadow-sm animate-in fade-in slide-in-from-left-2 cursor-move hover:bg-indigo-100 transition-colors"
                      >
                        <i className="fas fa-grip-vertical text-indigo-300 text-xs"></i>
                        <span
                          onClick={(e) => openDimDropdown(idx, e)}
                          className="text-xs font-black text-indigo-700 cursor-pointer hover:text-indigo-900 underline decoration-dotted underline-offset-2"
                        >
                          {dimLabel}
                        </span>
                        <button onClick={(e) => { e.stopPropagation(); toggleDimension(dim); }} className="ml-1 text-indigo-200 hover:text-rose-500 transition-colors"><i className="fas fa-times-circle text-xs"></i></button>
                      </div>
                      {/* Dimension replacement dropdown - using Portal to render to body */}
                      {editingDimIndex === idx && dropdownPosition && createPortal(
                        <div
                          className="fixed z-[99999] bg-white rounded-xl shadow-2xl border border-slate-200 py-2 min-w-[180px] animate-in fade-in zoom-in duration-150"
                          style={{
                            top: `${dropdownPosition.top}px`,
                            left: `${dropdownPosition.left}px`
                          }}
                          data-dim-dropdown="true"
                        >
                          <div className="px-3 py-2 border-b border-slate-100">
                            <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">Replace <span className="text-indigo-600">{dimLabel}</span> with:</div>
                          </div>
                          {ALL_DIMENSIONS.filter(d => d.value !== dim).map(d => (
                            <button
                              key={d.value}
                              onClick={(e) => { e.stopPropagation(); replaceDimension(idx, d.value); }}
                              className={`w-full px-4 py-2 text-left text-xs font-medium hover:bg-indigo-50 transition-colors flex items-center gap-2 ${activeDims.includes(d.value) ? 'text-slate-300 cursor-not-allowed' : 'text-slate-700 hover:text-indigo-700'}`}
                            >
                              <i className={`fas fa-circle text-[6px] ${activeDims.includes(d.value) ? 'text-slate-300' : 'text-indigo-400'}`}></i>
                              {d.label}
                              {activeDims.includes(d.value) && <span className="ml-auto text-[9px] text-slate-400">(active)</span>}
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
                      <div className="w-10 h-10 border-[3px] border-indigo-200 border-t-indigo-600 rounded-full animate-spin"></div>
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
                        <th key={m.key} className="px-4 py-4 text-right relative group" style={{ width: columnWidths[m.key] || 90 }}>
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
                    {(() => {
                      // 计算每个行的斑马纹状态（按层级独立）
                      let childIndex = 0; // 子级行独立计数器
                      const dataWithStripe = paginatedData.map((row, idx) => {
                        let isEvenRow = false;
                        if (row.level === 0) {
                          // 父级：按全局索引
                          isEvenRow = idx % 2 === 0;
                        } else {
                          // 子级：独立计数计算斑马纹
                          isEvenRow = childIndex % 2 === 1; // 1,3,5...为true（黄色）
                          childIndex++;
                        }
                        return { ...row, isEvenRow };
                      });

                      return dataWithStripe.map((row) => {
                      const isExpanded = expandedDimRows.has(row.id);
                      const isEvenRow = row.isEvenRow;
                      const isChild = row.level > 0;
                      // 子级维度行：奇数用浅黄色，偶数用白色
                      const cellBgClass = isChild ? (isEvenRow ? 'bg-white' : 'bg-yellow-50') : (isEvenRow ? 'bg-slate-100' : 'bg-white');
                      // 子级样式：紧凑行、不加粗、左边框作为区域边界
                      const pyClass = isChild ? 'py-1.5' : 'py-3';
                      const nameClass = isChild ? 'text-[12px] font-semibold text-slate-800' : 'text-[13px] font-black text-slate-800';
                      const labelClass = isChild ? 'text-[8px] text-slate-400 uppercase tracking-wider' : 'text-[9px] text-slate-400 font-bold uppercase tracking-wider';
                      const borderClass = isChild ? 'border-l-4 border-indigo-300' : '';
                      return (
                      <React.Fragment key={row.id}>
                        <tr className="group" onContextMenu={(e) => handleContextMenu(e, getRowText(row))}>
                          <td className={`px-4 sticky left-0 z-10 border-r border-slate-200 group-hover:bg-violet-50 transition-colors ${cellBgClass} ${pyClass} ${borderClass}`} style={{ paddingLeft: `${row.level * 20 + 32}px`, width: columnWidths.hierarchy }}>
                            <div className="flex items-center gap-2 cursor-pointer" onClick={() => {
                              const nextFilters = row.filterPath || row.id.split('|').map((v, i) => ({ dimension: activeDims[i], value: v }));
                              setActiveFilters(nextFilters);
                              setQuickFilterText('');
                            }}>
                              <button onClick={(e) => { e.stopPropagation(); toggleDailyBreakdown(e, row); }} className={`w-5 h-5 rounded-full flex items-center justify-center transition-colors ${expandedDailyRows.has(row.id) ? 'bg-indigo-600 shadow-sm' : loadingDailyRows.has(row.id) ? 'bg-amber-400' : 'bg-slate-100'}`}>
                                {loadingDailyRows.has(row.id) ? (
                                  <svg className="animate-spin h-2.5 w-2.5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                  </svg>
                                ) : (
                                  <div className={`w-1.5 h-1.5 rounded-full ${expandedDailyRows.has(row.id) ? 'bg-white' : 'bg-slate-400'}`}></div>
                                )}
                              </button>
                              {row.hasChild && <button onClick={(e) => { e.stopPropagation(); toggleDimExpansion(e, row); }} className={`w-6 h-6 rounded flex items-center justify-center transition-all ${loadingDimRows.has(row.id) ? 'bg-amber-100 border-amber-200' : 'bg-slate-50 border border-slate-100'} ${loadingDimRows.has(row.id) ? '' : isExpanded ? 'rotate-90' : ''}`}>
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
                                  {/* Lander 跳转图标 */}
                                  {(() => {
                                    if (row.dimensionType === 'lander') {
                                      if ((row as any).landerUrl) {
                                        const url = (row as any).landerUrl;
                                        const urlWithParam = url.includes('?') ? `${url}&w=1` : `${url}?w=1`;
                                        return (
                                          <a
                                            href={urlWithParam}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            onClick={(e) => e.stopPropagation()}
                                            className="shrink-0 w-5 h-5 flex items-center justify-center rounded-full hover:bg-indigo-100 transition-colors z-10"
                                            title="Open Lander"
                                          >
                                            <i className="fas fa-external-link-alt text-[10px] text-indigo-400 hover:text-indigo-600"></i>
                                          </a>
                                        );
                                      }
                                    }
                                    return null;
                                  })()}
                                </div>
                                <span className={labelClass}>{ALL_DIMENSIONS.find(d => d.value === row.dimensionType)?.label}</span>
                              </div>
                            </div>
                          </td>
                          {visibleMetrics.map(m => <td key={m.key} className={`px-4 ${pyClass} text-right group-hover:bg-violet-50 transition-colors ${cellBgClass}`} style={{ width: columnWidths[m.key] || 90 }}><MetricValue value={row[m.key] as number} type={m.type} colorMode={colorMode} metricKey={m.key as string} isSub={isChild} /></td>)}
                        </tr>
                        {expandedDailyRows.has(row.id) && (() => {
                          const dailyData = dailyDataMap.get(row.id);
                          return dailyData?.slice(0, 7).map((day, dayIdx) => (
                          <tr key={day.date} className="bg-violet-50 hover:bg-violet-100">
                            <td className="px-4 py-2 sticky left-0 bg-violet-50 z-10 border-l-4 border-indigo-600/60 border-r border-violet-100" style={{ paddingLeft: `${row.level * 20 + 72}px`, width: columnWidths.hierarchy }}><span className="text-[12px] font-bold text-violet-700">{day.date}</span></td>
                            {visibleMetrics.map(m => <td key={m.key} className="px-4 py-2 text-right opacity-80 bg-violet-50" style={{ width: columnWidths[m.key] || 90 }}><MetricValue value={day[m.key as keyof DailyBreakdown] as number || 0} type={m.type} isSub colorMode={colorMode} metricKey={m.key as string} /></td>)}
                          </tr>
                          ));
                        })()}
                      </React.Fragment>
                      );
                    });
                    })()}

                    {/* Summary Row - same table for perfect column alignment */}
                    {totalRows > 0 && (
                      <tr className="bg-slate-100 border-t-2 border-slate-300 sticky bottom-0 z-40 shadow-[0_-2px_4px_rgba(0,0,0,0.1)]">
                        <td className="px-4 py-2 sticky left-0 bg-slate-100 z-10 border-r border-slate-300 border-l-4 border-slate-400" style={{ paddingLeft: '52px', width: columnWidths.hierarchy }}>
                          <span className="text-[11px] font-black uppercase text-slate-500 tracking-widest">Summary</span>
                        </td>
                        {visibleMetrics.map(m => {
                          let value: number;
                          let displayValue: React.ReactNode;

                          // Calculate summary value for each metric
                          if (m.key === 'impressions') value = summaryData.impressions;
                          else if (m.key === 'clicks') value = summaryData.clicks;
                          else if (m.key === 'conversions') value = summaryData.conversions;
                          else if (m.key === 'spend') value = summaryData.spend;
                          else if (m.key === 'revenue') value = summaryData.revenue;
                          else if (m.key === 'profit') value = summaryData.profit;
                          else if (m.key === 'ctr') value = summaryData.ctr * 100;
                          else if (m.key === 'cvr') value = summaryData.cvr * 100;
                          else if (m.key === 'roi') value = summaryData.roi * 100;
                          else if (m.key === 'cpa') value = summaryData.cpa;
                          else if (m.key === 'epa') value = summaryData.rpa;
                          else if (m.key === 'rpa') value = summaryData.rpa;
                          else if (m.key === 'epc') value = summaryData.epc;
                          else if (m.key === 'epv') value = summaryData.epv;
                          else if (m.key === 'm_epc') value = summaryData.m_epc;
                          else if (m.key === 'm_epv') value = summaryData.m_epv;
                          else if (m.key === 'm_cpc') value = summaryData.m_cpc;
                          else if (m.key === 'm_cpv') value = summaryData.m_cpv;
                          else if (m.key === 'm_cpa') value = summaryData.m_cpa;
                          else if (m.key === 'm_epa') value = summaryData.m_epa;
                          else if (m.key === 'm_imp') value = summaryData.m_imp;
                          else if (m.key === 'm_clicks') value = summaryData.m_clicks;
                          else if (m.key === 'm_conv') value = summaryData.m_conv;
                          else value = 0;

                          // Format display based on metric type
                          if (m.type === 'money' || m.type === 'profit') {
                            displayValue = <span className={`font-mono tracking-tight leading-none text-[14px] font-bold ${m.key === 'profit' || m.type === 'profit' ? (value > 0 ? 'text-emerald-600' : value < 0 ? 'text-rose-600' : '') : m.key === 'spend' ? 'text-rose-600' : m.key === 'revenue' ? 'text-amber-600' : ''}`}>${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
                          } else if (m.type === 'percent') {
                            const colorForValue = (v: number) => {
                              if (m.key === 'roi') return v > 0 ? 'text-emerald-600' : v < 0 ? 'text-rose-600' : '';
                              return '';
                            };
                            displayValue = <span className={`font-mono tracking-tight leading-none text-[14px] font-bold ${colorForValue(value)}`}>{value.toFixed(2)}%</span>;
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
                          <button onClick={() => {
                            console.log('Editing user:', u);
                            setEditingUser(u);
                            setShowUserModal(true);
                          }} className="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center hover:bg-indigo-100 transition-colors"><i className="fas fa-edit text-xs"></i></button>
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
              <div className="flex items-center gap-3 px-4 py-3 bg-indigo-50 rounded-xl border border-indigo-100">
                <input
                  type="checkbox"
                  name="showRevenue"
                  id="showRevenue"
                  checked={editingUser?.showRevenue !== false}
                  onChange={(e) => {
                    console.log('Checkbox changed:', e.target.checked, 'editingUser.showRevenue:', editingUser?.showRevenue);
                    // 更新 editingUser 的 showRevenue 值
                    if (editingUser) {
                      setEditingUser({ ...editingUser, showRevenue: e.target.checked });
                    }
                  }}
                  className="w-5 h-5 rounded border-slate-300 text-indigo-600 focus:ring-indigo-50"
                />
                <label htmlFor="showRevenue" className="text-xs font-bold text-slate-700 cursor-pointer select-none">
                  Show Revenue Columns <span className="text-slate-400 font-normal">(Revenue, Profit, EPA, EPC, etc.)</span>
                </label>
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
                      {metrics.filter(m => m.group === groupName).map(m => {
                        const revenueKeys = ['revenue', 'profit', 'epa', 'epc', 'epv', 'roi', 'm_epc', 'm_epv', 'm_epa'];
                        const isRevenueMetric = revenueKeys.includes(m.key);
                        const isHiddenByPermission = currentUser.showRevenue === false && isRevenueMetric;

                        if (isHiddenByPermission) return null;  // 完全隐藏没有权限的列

                        return (
                          <label key={m.key} className={`flex items-center gap-3 p-3 border rounded-xl cursor-pointer transition-all ${m.visible ? 'border-indigo-500 bg-indigo-50' : 'border-slate-100 bg-white'}`}>
                            <input type="checkbox" checked={m.visible} onChange={() => setMetrics(prev => prev.map(p => p.key === m.key ? { ...p, visible: !p.visible } : p))} className="w-4 h-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-50" />
                            <span className="text-xs font-bold text-slate-700">{m.label}</span>
                          </label>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
              <div className="w-[280px] bg-indigo-50/20 rounded-2xl border border-indigo-100 p-5 flex flex-col overflow-y-auto custom-scrollbar">
                <span className="text-[10px] font-black uppercase tracking-widest text-indigo-600 mb-4 block">Selected Order</span>
                {metrics.filter(m => {
                  if (!m.visible) return false;
                  const revenueKeys = ['revenue', 'profit', 'epa', 'epc', 'epv', 'roi', 'm_epc', 'm_epv', 'm_epa'];
                  return !(currentUser.showRevenue === false && revenueKeys.includes(m.key));
                }).map((m, idx) => (
                   <div key={m.key} draggable onDragStart={() => dragItemKey.current = m.key} onDragEnter={() => dragOverItemKey.current = m.key} onDragEnd={handleMetricReorder} onDragOver={e => e.preventDefault()} className="p-3 bg-white border border-indigo-200 rounded-xl flex items-center gap-3 shadow-sm mb-2 cursor-grab">
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
  // Use key to force component remount on user change (security: prevents data leakage)
  return <Dashboard key={user.id} currentUser={user} onLogout={handleLogout} />;
};

export default App;
