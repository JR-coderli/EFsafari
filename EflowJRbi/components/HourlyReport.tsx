import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { UserPermission } from '../types';
import MetricValue from './MetricValue';
import { useColumnResize } from '../hooks/useColumnResize';
import { getDateInTimezone, formatDateString } from '../utils/dateHelpers';

interface HourlyDataRow {
  id: string;
  name: string;
  level: number;
  dimensionType: string;
  hour: number | null;
  timezone: string | null;
  impressions: number;
  clicks: number;
  conversions: number;
  spend: number;
  revenue: number;
  profit: number;
  ctr: number;
  cvr: number;
  roi: number;
  cpa: number;
  rpa: number;
  epc: number;
  epv: number;
  epa: number;
  hasChild: boolean;
}

interface HourlyDataResponse {
  data: HourlyDataRow[];
  total: number;
  dateRange: { start_date: string; end_date: string };
}

interface MetricConfig {
  key: string;
  label: string;
  visible: boolean;
  type: 'money' | 'percent' | 'number' | 'profit';
  group: string;
}

// 下钻路径项
interface DrillPathItem {
  dimension: string;
  value: string;
  label: string;
}

const DEFAULT_METRICS: MetricConfig[] = [
  { key: 'spend', label: 'Spend', visible: true, type: 'money', group: 'Basic' },
  { key: 'conversions', label: 'Conversions', visible: true, type: 'number', group: 'Basic' },
  { key: 'cpa', label: 'CPA', visible: true, type: 'money', group: 'Calculated' },
  { key: 'revenue', label: 'Revenue', visible: true, type: 'money', group: 'Basic' },
  { key: 'profit', label: 'Profit', visible: true, type: 'profit' as const, group: 'Basic' },
  { key: 'epa', label: 'EPA', visible: true, type: 'money', group: 'Calculated' },
  { key: 'roi', label: 'ROI', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'ctr', label: 'CTR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'cvr', label: 'CVR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'epc', label: 'EPC', visible: true, type: 'money', group: 'Calculated' },
  { key: 'impressions', label: 'Impressions', visible: true, type: 'number', group: 'Basic' },
  { key: 'clicks', label: 'Clicks', visible: true, type: 'number', group: 'Basic' },
];

const ALL_DIMENSIONS = [
  { value: 'hour', label: 'Hour' },
  { value: 'platform', label: 'Media' },
  { value: 'adset', label: 'Adset' },
  { value: 'offer', label: 'Offer' },
  { value: 'advertiser', label: 'Advertiser' },
  { value: 'campaign', label: 'Campaign' },
];

const TIMEZONES = [
  { value: 'UTC', label: 'UTC+0' },
  { value: 'Asia/Shanghai', label: 'UTC+8' },
  { value: 'EST', label: 'EST/UTC-5' },
  { value: 'PST', label: 'PST/UTC-8' },
];


interface Props {
  currentUser: UserPermission;
  // 日期字符串（单一数据源，格式 YYYY-MM-DD）
  dateStr: string;
  onDateChange?: (dateStr: string) => void;
  // 时区变化回调，通知父组件当前选定的时区
  onTimezoneChange?: (timezone: string) => void;
  // 初始时区（从父组件传入，用于同步）
  initialTimezone?: string;
}

type SortField = 'name' | 'impressions' | 'clicks' | 'conversions' | 'spend' | 'revenue' | 'profit' | 'ctr' | 'cvr' | 'roi' | 'cpa' | 'rpa' | 'epc' | 'epv';

export default function HourlyReport({ currentUser, dateStr, onDateChange, onTimezoneChange, initialTimezone }: Props) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<HourlyDataRow[]>([]);
  const [activeDims, setActiveDims] = useState<string[]>(['platform', 'hour']);
  const [drillPath, setDrillPath] = useState<DrillPathItem[]>([]);
  // 使用父组件传入的初始时区，如果没有则默认为 UTC
  const [timezone, setTimezone] = useState(initialTimezone || 'UTC');
  const [metrics, setMetrics] = useState<MetricConfig[]>(DEFAULT_METRICS);
  const [quickFilterText, setQuickFilterText] = useState('');
  const [hideZeroImpressions, setHideZeroImpressions] = useState(true);  // 隐藏 impressions < 20 且 revenue = 0 的数据
  const [etlStatus, setEtlStatus] = useState<{ utc: any; utc8: any; est: any; pst: any } | null>(null);
  const [sortBy, setSortBy] = useState<SortField>('name');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [refreshing, setRefreshing] = useState(false);
  // 防止在时区切换时重复触发 loadData
  const [isTimezoneChanging, setIsTimezoneChanging] = useState(false);
  const loadDataRef = useRef<(() => Promise<void>) | null>(null);
  // 请求序列号，防止旧请求覆盖新请求
  const requestIdRef = useRef(0);

  // 列宽拖动功能
  const { columnWidths, setColumnWidths, resizingColumn, handleResizeStart } = useColumnResize({
    dimension: 300,
    ...Object.fromEntries(DEFAULT_METRICS.map(m => [m.key, 120]))
  });

  // initialTimezone 同步：当父组件传入的 initialTimezone 变化时，同步本地 timezone 状态
  useEffect(() => {
    if (initialTimezone && initialTimezone !== timezone) {
      setTimezone(initialTimezone);
    }
  }, [initialTimezone]);

  // 拖动相关状态
  const [draggedMetricIndex, setDraggedMetricIndex] = useState<number | null>(null);
  const [dragOverIndex, setDragOverIndex] = useState<number | null>(null);

  // 维度下拉框相关状态
  const [editingDimIndex, setEditingDimIndex] = useState<number | null>(null);
  const [dropdownPosition, setDropdownPosition] = useState<{ top: number; left: number } | null>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const normalizeDrillPath = (nextDims: string[], path: DrillPathItem[]) => {
    const normalized: DrillPathItem[] = [];
    for (let i = 0; i < path.length && i < nextDims.length; i++) {
      if (path[i].dimension !== nextDims[i]) break;
      normalized.push(path[i]);
    }
    return normalized;
  };

  const token = localStorage.getItem('addata_access_token') || '';

  // 时区切换时，保留筛选路径（platform 等维度），但清除 hour 筛选（不同时区的 hour 值不同）
  const handleTimezoneChange = (newTimezone: string) => {
    // 防止重复触发
    if (isTimezoneChanging) return;

    // 通知父组件时区变化（父组件会根据 range 决定是否更新日期）
    onTimezoneChange?.(newTimezone);

    // 设置标志，防止重复触发
    setIsTimezoneChanging(true);

    // 过滤掉 hour 维度的筛选（因为不同时区的 hour 值不同）
    const filteredPath = drillPath.filter(item => item.dimension !== 'hour');

    // 更新本地状态
    setTimezone(newTimezone);
    setDrillPath(filteredPath);

    // 等待状态更新后清除标志（让统一 effect 触发数据加载）
    setTimeout(() => {
      setIsTimezoneChanging(false);
    }, 100);
  };

  // 辅助函数：为指定时区和日期加载数据（目前未被使用，保留备用）
  const loadDataForTimezone = useCallback(async (tz: string, path: DrillPathItem[], dimension: string) => {
    const filters = path.map(item => ({
      dimension: item.dimension,
      value: item.value
    }));

    const params = new URLSearchParams({
      start_date: dateStr,
      end_date: dateStr,
      group_by: dimension,
      timezone: tz,
      limit: '1000',
    });

    if (filters.length > 0) {
      params.append('filters', JSON.stringify(filters));
    }

    const response = await fetch(`/api/hourly/data?${params}`, {
      headers: { 'Authorization': `Bearer ${token}` },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const result: HourlyDataResponse = await response.json();
    setData(result.data);
  }, [dateStr, token]);

  // 当前层级维度：根据下钻路径计算
  const currentDimensionIndex = drillPath.length;
  const currentDimension = activeDims[currentDimensionIndex];
  const hasNextLevel = currentDimensionIndex < activeDims.length - 1;

  // 初始化默认排序：只有当前显示的维度是 hour 时才按 hour 排序，否则按 revenue 降序
  useEffect(() => {
    if (currentDimension === 'hour') {
      setSortBy('name'); // name 对于 hour 维度就是小时
      setSortOrder('desc');
    } else {
      setSortBy('revenue');
      setSortOrder('desc');
    }
  }, [currentDimension]);

  // Load data
  const loadData = useCallback(async () => {
    // 生成新的请求 ID，防止旧请求覆盖新请求
    const currentRequestId = ++requestIdRef.current;

    setLoading(true);
    setError(null);

    try {
      const filters = drillPath.map(item => ({
        dimension: item.dimension,
        value: item.value
      }));

      const params = new URLSearchParams({
        start_date: dateStr,
        end_date: dateStr,
        group_by: currentDimension || 'hour',
        timezone: timezone,
        limit: '1000',
      });

      if (filters.length > 0) {
        params.append('filters', JSON.stringify(filters));
      }

      const response = await fetch(`/api/hourly/data?${params}`, {
        headers: { 'Authorization': `Bearer ${token}` },
      });

      // 检查是否是最新的请求，如果不是则忽略响应
      if (currentRequestId !== requestIdRef.current) {
        return;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const result: HourlyDataResponse = await response.json();
      setData(result.data);
    } catch (err) {
      // 只在请求是最新的时才设置错误
      if (currentRequestId === requestIdRef.current) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
      }
    } finally {
      // 只在请求是最新的时才清除 loading
      if (currentRequestId === requestIdRef.current) {
        setLoading(false);
      }
    }
  }, [dateStr, drillPath, currentDimension, timezone, token]);

  // 将最新的 loadData 函数存储到 ref 中，以便 useEffect 始终访问到最新版本
  loadDataRef.current = loadData;

  // Load ETL status
  const loadEtlStatus = async () => {
    try {
      const response = await fetch('/api/hourly/status', {
        headers: { 'Authorization': `Bearer ${token}` },
      });
      if (response.ok) {
        const status = await response.json();
        setEtlStatus(status);
      }
    } catch (err) {
      console.error('Error loading ETL status:', err);
    }
  };

  // Load saved metrics order
  const loadMetricsOrder = async () => {
    try {
      const response = await fetch('/api/hourly/metrics-order', {
        headers: { 'Authorization': `Bearer ${token}` },
      });
      if (response.ok) {
        const data = await response.json();
        if (data.metrics && Array.isArray(data.metrics) && data.metrics.length > 0) {
          // 根据保存的顺序重新排列 metrics
          const orderedMetrics = data.metrics
            .map(key => DEFAULT_METRICS.find(m => m.key === key))
            .filter((m): m is MetricConfig => m !== undefined);
          // 添加没有被保存的指标
          const remainingMetrics = DEFAULT_METRICS.filter(m => !data.metrics.includes(m.key));
          setMetrics([...orderedMetrics, ...remainingMetrics]);
        }
      }
    } catch (err) {
      console.error('Error loading metrics order:', err);
    }
  };

  // Save metrics order
  const saveMetricsOrder = async (newMetrics: MetricConfig[]) => {
    try {
      const metricKeys = newMetrics.map(m => m.key);
      await fetch('/api/hourly/metrics-order', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(metricKeys),
      });
    } catch (err) {
      console.error('Error saving metrics order:', err);
    }
  };

  // Handle drag start
  const handleDragStart = (index: number) => {
    setDraggedMetricIndex(index);
  };

  // Handle drag over
  const handleDragOver = (index: number) => {
    if (draggedMetricIndex === null || draggedMetricIndex === index) return;
    setDragOverIndex(index);
  };

  // Handle drop
  const handleDrop = async (dropIndex: number) => {
    if (draggedMetricIndex === null || draggedMetricIndex === dropIndex) return;

    const newMetrics = [...metrics];
    const [draggedItem] = newMetrics.splice(draggedMetricIndex, 1);
    newMetrics.splice(dropIndex, 0, draggedItem);

    setMetrics(newMetrics);
    await saveMetricsOrder(newMetrics);

    setDraggedMetricIndex(null);
    setDragOverIndex(null);
  };

  // Handle drag end
  const handleDragEnd = () => {
    setDraggedMetricIndex(null);
    setDragOverIndex(null);
  };

  // Handle manual refresh - 清除缓存并重新加载数据（不触发 ETL）
  const handleRefresh = async () => {
    setRefreshing(true);

    try {
      // 清除后端缓存
      const response = await fetch('/api/hourly/reload', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
        },
      });

      if (response.ok) {
        // 缓存清除成功，重新加载数据
        await loadData();
        // 刷新 ETL 状态
        await loadEtlStatus();
      } else {
        const errorData = await response.json();
        console.error('Reload failed:', errorData);
        // 即使清除缓存失败，也尝试重新加载数据
        await loadData();
      }
    } catch (err) {
      console.error('Error reloading:', err);
      // 出错时也尝试重新加载数据
      await loadData();
    } finally {
      setRefreshing(false);
    }
  };

  // 格式化最后更新时间（始终显示 UTC+8 的更新时间）
  const getLastUpdateDisplay = () => {
    if (!etlStatus) return 'No data';

    // 始终使用 utc8 (Asia/Shanghai) 的更新时间
    const status = etlStatus.utc8;

    if (!status) return 'No data';

    return `Update ${status.last_update || 'Unknown'}`;
  };

  // 当组件首次挂载或关键依赖变化时加载数据
  useEffect(() => {
    // 如果正在切换时区，不触发 loadData（由 handleTimezoneChange 处理）
    if (isTimezoneChanging) {
      return;
    }
    // 使用 ref 确保调用的是最新的 loadData 函数
    loadDataRef.current?.();
  }, [drillPath, activeDims, timezone, dateStr, isTimezoneChanging]);

  // 初始化时加载保存的指标顺序
  useEffect(() => {
    loadMetricsOrder();
  }, []);

  useEffect(() => {
    loadEtlStatus();
    const interval = setInterval(loadEtlStatus, 30 * 1000); // 每30秒刷新一次状态
    return () => clearInterval(interval);
  }, [timezone]);

  // 点击外部关闭维度下拉框
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      // 检查是否点击在下拉框或维度按钮内
      const dropdowns = document.querySelectorAll('[data-dim-dropdown]');
      let clickedInside = false;
      dropdowns.forEach(dropdown => {
        if (dropdown.contains(target)) clickedInside = true;
      });
      if (!clickedInside) {
        closeDimDropdown();
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // 点击行下钻
  const handleRowClick = (row: HourlyDataRow) => {
    if (!hasNextLevel) return;

    const pathItem: DrillPathItem = {
      dimension: currentDimension || 'hour',
      value: row.name,
      label: row.name
    };

    setDrillPath([...drillPath, pathItem]);
  };

  // 返回到指定层级
  const handleBreadcrumbClick = (index: number) => {
    setDrillPath(drillPath.slice(0, index));
  };

  // 移动维度顺序
  const moveDimension = (index: number, direction: 'left' | 'right') => {
    const newDims = [...activeDims];
    if (direction === 'left' && index > 0) {
      [newDims[index - 1], newDims[index]] = [newDims[index], newDims[index - 1]];
    } else if (direction === 'right' && index < newDims.length - 1) {
      [newDims[index], newDims[index + 1]] = [newDims[index + 1], newDims[index]];
    }
    setActiveDims(newDims);
    setDrillPath([]); // 重置下钻路径
  };

  // 打开维度下拉框
  const openDimDropdown = (index: number, e: React.MouseEvent) => {
    const target = e.currentTarget as HTMLElement;
    const rect = target.getBoundingClientRect();
    setDropdownPosition({
      top: rect.bottom + 4,
      left: rect.left
    });
    setEditingDimIndex(index);
  };

  // 关闭维度下拉框
  const closeDimDropdown = () => {
    setEditingDimIndex(null);
    setDropdownPosition(null);
  };

  // 替换维度
  const replaceDimension = (index: number, newDimValue: string) => {
    const newDims = [...activeDims];
    newDims[index] = newDimValue;
    setActiveDims(newDims);
    setDrillPath(normalizeDrillPath(newDims, drillPath));
    closeDimDropdown();
  };

  // 处理列排序
  const handleSort = (field: SortField) => {
    if (sortBy === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortOrder('desc');
    }
  };

  // Filter and sort data
  const filteredData = useMemo(() => {
    let result = data.filter(row => {
      // 隐藏 impressions < 20 且 revenue = 0 的数据
      if (hideZeroImpressions && row.impressions < 20 && row.revenue === 0) return false;
      if (quickFilterText && !row.name.toLowerCase().includes(quickFilterText.toLowerCase())) return false;
      return true;
    });

    // 排序
    result = [...result].sort((a, b) => {
      let aVal: any, bVal: any;

      if (sortBy === 'name') {
        // hour 维度特殊处理
        if (currentDimension === 'hour' && a.hour !== null && b.hour !== null) {
          aVal = a.hour;
          bVal = b.hour;
        } else {
          aVal = a.name.toLowerCase();
          bVal = b.name.toLowerCase();
        }
      } else {
        aVal = a[sortBy as keyof HourlyDataRow];
        bVal = b[sortBy as keyof HourlyDataRow];
      }

      if (typeof aVal === 'string') {
        return sortOrder === 'asc'
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      }

      return sortOrder === 'asc' ? aVal - bVal : bVal - aVal;
    });

    return result;
  }, [data, quickFilterText, hideZeroImpressions, sortBy, sortOrder, currentDimension]);

  // Summary
  const summary = useMemo(() => {
    const totals = filteredData.reduce((acc, row) => ({
      impressions: acc.impressions + row.impressions,
      clicks: acc.clicks + row.clicks,
      conversions: acc.conversions + row.conversions,
      spend: acc.spend + row.spend,
      revenue: acc.revenue + row.revenue,
    }), { impressions: 0, clicks: 0, conversions: 0, spend: 0, revenue: 0 });
    // 派生指标基于汇总值计算
    const profit = totals.revenue - totals.spend;
    const ctr = totals.clicks / (totals.impressions || 1);
    const cvr = totals.conversions / (totals.clicks || 1);
    const roi = (totals.revenue - totals.spend) / (totals.spend || 1);
    const cpa = totals.spend / (totals.conversions || 1);
    const epc = totals.revenue / (totals.clicks || 1);
    const epa = totals.revenue / (totals.conversions || 1);  // EPA = revenue / conversions
    return { ...totals, profit, ctr, cvr, roi, cpa, epc, epa };
  }, [filteredData]);

  const visibleMetrics = metrics.filter(m => m.visible);

  // 获取当前维度标签
  const getCurrentDimensionLabel = () => {
    const dim = ALL_DIMENSIONS.find(d => d.value === currentDimension);
    return dim?.label || currentDimension || 'Unknown';
  };

  return (
    <div className="flex flex-col h-full bg-[#f8fafc]">
      {/* Header */}
      <div className="px-8 py-4 bg-white border-b border-slate-200 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="font-extrabold text-slate-800 tracking-tight text-sm uppercase italic">Hourly Insight</h2>
          <div className="flex items-center gap-2">
            <label className="text-[10px] font-bold text-slate-500">Timezone:</label>
            <select
              value={timezone}
              onChange={(e) => handleTimezoneChange(e.target.value)}
              className="px-3 py-1.5 bg-slate-50 border border-slate-200 rounded-lg text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20"
            >
              {TIMEZONES.map(tz => (
                <option key={tz.value} value={tz.value}>{tz.label}</option>
              ))}
            </select>
          </div>
          {/* 当前日期显示（根据选定时区） */}
          <div className="flex items-center gap-1.5 px-3 py-1 rounded-lg text-[10px] font-bold bg-indigo-50 text-indigo-600">
            <i className="fas fa-calendar text-[9px]"></i>
            <span>{formatDateString(dateStr)}</span>
          </div>
          {/* 上次更新时间 */}
          <div className="flex items-center gap-1.5 px-3 py-1 rounded-lg text-[10px] font-bold bg-slate-100 text-slate-600">
            <span className="w-2 h-2 rounded-full bg-indigo-400"></span>
            <span>{getLastUpdateDisplay()}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {/* 手动同步数据按钮 */}
          <button
            type="button"
            onClick={(e) => {
              e.preventDefault();
              handleRefresh();
            }}
            disabled={refreshing}
            className={`px-3 py-1.5 rounded-lg text-xs font-bold flex items-center gap-1.5 transition-all ${
              refreshing
                ? 'bg-slate-100 text-slate-400 cursor-not-allowed'
                : 'bg-indigo-50 text-indigo-600 hover:bg-indigo-100 hover:text-indigo-700'
            }`}
          >
            <i className={`fas fa-sync-alt ${refreshing ? 'animate-spin' : ''}`}></i>
            <span>{refreshing ? 'Refreshing...' : 'Refresh'}</span>
          </button>
          <div className="relative w-64">
            <input
              type="text"
              placeholder="Quick Search..."
              className="w-full pl-9 pr-4 py-2 bg-slate-50 border border-slate-200 rounded-xl text-xs outline-none focus:ring-2 focus:ring-indigo-500/20"
              value={quickFilterText}
              onChange={(e) => setQuickFilterText(e.target.value)}
            />
          </div>
        </div>
      </div>

      {/* Dimensions Bar */}
      <div className="px-6 py-2 bg-white border-b border-slate-100 flex items-center gap-3 overflow-x-auto shrink-0">
        <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest whitespace-nowrap">Drill:</span>
        <div className="flex items-center gap-2">
          {activeDims.map((dim, index) => {
            const dimInfo = ALL_DIMENSIONS.find(d => d.value === dim);
            const isPassed = index < drillPath.length;
            const isCurrent = index === drillPath.length;
            const isFuture = index > drillPath.length;
            const isEditing = editingDimIndex === index;

            return (
              <div
                key={dim}
                className="relative"
                data-dim-dropdown="true"
              >
                <div
                  className={`flex items-center px-2 py-1 rounded-lg gap-1 border ${
                    isPassed
                      ? 'bg-emerald-50 border-emerald-200'
                      : isCurrent
                      ? 'bg-indigo-50 border-indigo-200'
                      : 'bg-slate-50 border border-slate-200'
                  } ${isEditing ? 'ring-2 ring-indigo-400' : ''}`}
                >
                  {/* 左移箭头 */}
                  {index > 0 && (
                    <button
                      onClick={(e) => { e.stopPropagation(); moveDimension(index, 'left'); }}
                      className="text-slate-400 hover:text-indigo-600 px-0.5"
                      title="Move left"
                    >
                      <i className="fas fa-chevron-left text-[9px]"></i>
                    </button>
                  )}
                  {/* 可点击的维度标签 */}
                  <button
                    onClick={(e) => openDimDropdown(index, e)}
                    className={`text-[11px] font-bold cursor-pointer hover:text-indigo-600 ${
                      isPassed
                        ? 'text-emerald-600'
                        : isCurrent
                        ? 'text-indigo-700'
                        : 'text-slate-500'
                    }`}
                  >
                    {dimInfo?.label || dim}
                  </button>
                  {isPassed && <span className="text-emerald-500 text-[10px]">✓</span>}
                  {/* 右移箭头 */}
                  {index < activeDims.length - 1 && (
                    <button
                      onClick={(e) => { e.stopPropagation(); moveDimension(index, 'right'); }}
                      className="text-slate-400 hover:text-indigo-600 px-0.5"
                      title="Move right"
                    >
                      <i className="fas fa-chevron-right text-[9px]"></i>
                    </button>
                  )}
                  {/* 删除按钮 */}
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      const newDims = activeDims.filter(d => d !== dim);
                      setActiveDims(newDims);
                      setDrillPath(normalizeDrillPath(newDims, drillPath));
                      closeDimDropdown();
                    }}
                    className="text-slate-300 hover:text-rose-500 px-0.5"
                    title="Remove"
                  >
                    <i className="fas fa-times text-[9px]"></i>
                  </button>
                </div>
                {/* Dimension replacement dropdown */}
                {isEditing && dropdownPosition && createPortal(
                  <div
                    className="fixed z-[99999] bg-white rounded-xl shadow-2xl border border-slate-200 py-2 min-w-[180px] animate-in fade-in zoom-in duration-150"
                    style={{
                      top: `${dropdownPosition.top}px`,
                      left: `${dropdownPosition.left}px`
                    }}
                    data-dim-dropdown="true"
                  >
                    <div className="px-3 py-2 border-b border-slate-100">
                      <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">
                        Replace <span className="text-indigo-600">{dimInfo?.label || dim}</span> with:
                      </div>
                    </div>
                    {ALL_DIMENSIONS.filter(d => d.value !== dim).map(d => (
                      <button
                        key={d.value}
                        onClick={() => replaceDimension(index, d.value)}
                        className={`w-full px-4 py-2 text-left text-xs font-bold hover:bg-indigo-50 transition-colors ${
                          activeDims.includes(d.value) ? 'text-slate-400 cursor-not-allowed' : 'text-slate-700'
                        }`}
                        disabled={activeDims.includes(d.value)}
                      >
                        {activeDims.includes(d.value) && <span className="mr-2">✓</span>}
                        {d.label}
                      </button>
                    ))}
                  </div>,
                  document.body
                )}
              </div>
            );
          })}
          <div className="h-3 w-px bg-slate-200 mx-1"></div>
          {ALL_DIMENSIONS.filter(d => !activeDims.includes(d.value)).map(d => (
            <button
              key={d.value}
              onClick={() => {
                setActiveDims([...activeDims, d.value]);
                // 保持 drillPath，不清空父级筛选
              }}
              className="px-2 py-1 border border-dashed border-slate-300 text-slate-400 rounded-lg text-[10px] font-bold hover:border-indigo-400 hover:text-indigo-500 transition-colors"
            >
              + {d.label}
            </button>
          ))}
        </div>
        <label className="flex flex-col gap-0.5 px-2 py-1 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 ml-auto" title="Hide rows with impressions < 20 and revenue = 0">
          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={hideZeroImpressions}
              onChange={(e) => setHideZeroImpressions(e.target.checked)}
              className="w-3 h-3 rounded border-slate-300 text-indigo-600"
            />
            <span className="text-[10px] font-bold text-slate-600">Hide Low</span>
          </div>
          <span className="text-[8px] text-slate-400 leading-tight">Default filter imp&lt;20 &amp; rev=0</span>
        </label>
      </div>

      {/* 面包屑导航 - 显示下钻路径 */}
      <div className="px-6 py-2 bg-white border-b border-slate-200 flex items-center gap-2 shrink-0">
        <span className="text-[10px] font-bold text-slate-400 mr-1">Path:</span>
        <div className="flex items-center gap-1">
          <button
            onClick={() => handleBreadcrumbClick(0)}
            className={`px-3 py-1.5 rounded text-xs font-bold transition-all ${
              drillPath.length === 0
                ? 'bg-indigo-500 text-white shadow-sm'
                : 'text-slate-500 hover:bg-slate-100 hover:text-slate-700'
            }`}
          >
            All
          </button>
          {drillPath.map((item, index) => (
            <React.Fragment key={index}>
              <span className="text-indigo-300 text-xs font-bold">›</span>
              <button
                onClick={() => handleBreadcrumbClick(index + 1)}
                className={`px-3 py-1.5 rounded text-xs font-bold transition-all max-w-32 truncate ${
                  index === drillPath.length - 1
                    ? 'bg-indigo-500 text-white shadow-sm'
                    : 'text-slate-500 hover:bg-slate-100 hover:text-slate-700'
                }`}
              >
                {item.label}
              </button>
            </React.Fragment>
          ))}
        </div>
        <div className="ml-auto flex items-center gap-2">
          <span className="text-[10px] font-bold text-slate-400">Viewing:</span>
          <span className="px-2 py-1 bg-indigo-100 text-indigo-700 rounded text-[11px] font-bold">
            {getCurrentDimensionLabel()}
          </span>
          {hasNextLevel && (
            <span className="text-[10px] text-slate-400">
              → Click to drill into {ALL_DIMENSIONS.find(d => d.value === activeDims[drillPath.length + 1])?.label}
            </span>
          )}
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto p-4">
        {loading && (
          <div className="flex items-center justify-center h-full">
            <div className="flex flex-col items-center gap-3">
              <div className="w-10 h-10 border-4 border-indigo-200 border-t-indigo-600 rounded-full animate-spin"></div>
              <span className="text-sm font-bold text-slate-600">Loading...</span>
            </div>
          </div>
        )}

        {error && (
          <div className="flex items-center justify-center h-full">
            <div className="px-6 py-4 bg-rose-100 text-rose-700 rounded-xl text-sm font-bold">
              <i className="fas fa-exclamation-triangle mr-2"></i>
              {error}
            </div>
          </div>
        )}

        {!loading && !error && (
          <table className="w-full text-left border-collapse bg-white rounded-2xl shadow-sm overflow-hidden" style={{ tableLayout: 'fixed', minWidth: Object.values(columnWidths).reduce((a, b) => a + b, 0) + 200 }}>
            {/* 汇总行 - 在最上方 */}
            {filteredData.length > 0 && (
              <thead className="bg-slate-100 border-b border-slate-200">
                <tr className="font-bold">
                  <td className="px-4 py-3 text-left relative" style={{ width: columnWidths.dimension }}>
                    <span className="text-[11px] font-black uppercase text-slate-700 tracking-widest">
                      Total {drillPath.length > 0 && `(${getCurrentDimensionLabel()})`}
                    </span>
                  </td>
                  {visibleMetrics.map(m => {
                    const value = summary[m.key as keyof typeof summary] || 0;
                    return (
                      <td key={m.key} className="px-4 py-3 text-right relative" style={{ width: columnWidths[m.key] || 120 }}>
                        <MetricValue value={value} type={m.type} />
                      </td>
                    );
                  })}
                </tr>
              </thead>
            )}
            {/* 列头 */}
            <thead>
              <tr className="bg-slate-50 text-[10px] font-black text-slate-400 uppercase tracking-widest border-b border-slate-200">
                <th
                  className="px-4 py-3 text-left cursor-pointer hover:bg-slate-100 hover:text-indigo-600 transition-colors select-none relative group"
                  onClick={() => handleSort('name')}
                  style={{ width: columnWidths.dimension }}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-1">
                      Name
                      {sortBy === 'name' && (
                        <i className={`fas fa-sort-${sortOrder === 'asc' ? 'up' : 'down'} text-indigo-500`}></i>
                      )}
                    </div>
                    <div
                      className={`absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-indigo-500 opacity-0 group-hover:opacity-100 ${resizingColumn === 'dimension' ? 'bg-indigo-500 opacity-100' : ''}`}
                      onMouseDown={(e) => handleResizeStart('dimension', e)}
                    ></div>
                  </div>
                </th>
                {visibleMetrics.map((m, index) => {
                  const isDragging = draggedMetricIndex === index;
                  const isDragOver = dragOverIndex === index;
                  return (
                    <th
                      key={m.key}
                      draggable
                      onDragStart={() => handleDragStart(index)}
                      onDragOver={(e) => { e.preventDefault(); handleDragOver(index); }}
                      onDragEnd={handleDragEnd}
                      onDrop={() => handleDrop(index)}
                      className={`px-4 py-3 text-right cursor-pointer transition-colors select-none relative group ${
                        isDragging ? 'opacity-50' : ''
                      } ${isDragOver ? 'bg-indigo-100' : 'hover:bg-slate-100 hover:text-indigo-600'}`}
                      onClick={(e) => {
                        // 只在非拖动时触发排序
                        if (!isDragging) handleSort(m.key as SortField);
                      }}
                      style={{ width: columnWidths[m.key] || 120 }}
                    >
                      <div className="flex items-center gap-1 justify-end">
                        <i className="fas fa-grip-vertical text-slate-300 cursor-move hover:text-slate-500"></i>
                        {m.label}
                        {sortBy === m.key && (
                          <i className={`fas fa-sort-${sortOrder === 'asc' ? 'up' : 'down'} text-indigo-500`}></i>
                        )}
                      </div>
                      <div
                        className={`absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-indigo-500 opacity-0 group-hover:opacity-100 ${resizingColumn === m.key ? 'bg-indigo-500 opacity-100' : ''}`}
                        onMouseDown={(e) => handleResizeStart(m.key, e)}
                      ></div>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-50">
              {filteredData.map(row => (
                <tr
                  key={row.id}
                  className={`group transition-colors ${
                    hasNextLevel
                      ? 'hover:bg-violet-50 cursor-pointer'
                      : 'hover:bg-slate-50 cursor-default'
                  }`}
                  onClick={() => hasNextLevel && handleRowClick(row)}
                >
                  <td className="px-4 py-3" style={{ width: columnWidths.dimension }}>
                    <div className="flex items-center gap-2">
                      {hasNextLevel && (
                        <span className="text-indigo-300 group-hover:text-indigo-500 shrink-0">
                          <i className="fas fa-chevron-right text-[10px]"></i>
                        </span>
                      )}
                      <div className="flex flex-col">
                        <span className="text-[13px] font-bold text-slate-800">{row.name}</span>
                        <span className="text-[8px] text-slate-400 uppercase tracking-wider">
                          {ALL_DIMENSIONS.find(d => d.value === row.dimensionType)?.label || row.dimensionType}
                        </span>
                      </div>
                    </div>
                  </td>
                  {visibleMetrics.map(m => (
                    <td key={m.key} className="px-4 py-3 text-right group-hover:bg-violet-50" style={{ width: columnWidths[m.key] || 120 }}>
                      <MetricValue value={row[m.key as keyof HourlyDataRow] as number} type={m.type} />
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}

        {!loading && !error && filteredData.length === 0 && (
          <div className="flex items-center justify-center h-full">
            <div className="text-slate-400 text-sm">No data found</div>
          </div>
        )}
      </div>
    </div>
  );
}
