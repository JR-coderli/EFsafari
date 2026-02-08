import React from 'react';

/**
 * MetricValue Component
 *
 * 统一的指标值显示组件，支持多种显示类型和样式选项
 * 用于消除 App.tsx、DailyReport.tsx 和 HourlyReport.tsx 中的重复代码
 */

export interface MetricValueProps {
  value: number;
  type: 'money' | 'percent' | 'number' | 'profit';
  isSub?: boolean;
  colorMode?: boolean;
  metricKey?: string;
  isManualEdited?: boolean;
}

const MetricValue: React.FC<MetricValueProps> = ({
  value,
  type,
  isSub,
  colorMode,
  metricKey,
  isManualEdited
}) => {
  const displayValue = isFinite(value) ? value : 0;

  // 手动编辑的值使用琥珀色（Daily Report 特有功能）
  if (isManualEdited) {
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    if (type === 'money' || type === 'profit') {
      return <span className={`font-mono tracking-tight leading-none font-bold text-amber-600 ${sizeClass}`}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
    }
    if (type === 'percent') {
      return <span className={`font-mono tracking-tight leading-none font-bold text-amber-600 ${sizeClass}`}>{(displayValue * 100).toFixed(2)}%</span>;
    }
    return <span className={`font-mono tracking-tight leading-none font-bold text-amber-600 ${sizeClass}`}>{Math.floor(displayValue).toLocaleString()}</span>;
  }

  // Profit 总是有颜色（正数=绿色，负数=红色）
  if (type === 'profit') {
    const colorClass = displayValue > 0 ? 'text-emerald-600' : displayValue < 0 ? 'text-rose-600' : 'text-slate-800';
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    return <span className={`font-mono tracking-tight leading-none font-bold ${colorClass} ${sizeClass}`}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
  }

  // ROI 总是有颜色（正数=绿色，负数=红色）
  if (metricKey === 'roi') {
    const colorClass = displayValue > 0 ? 'text-emerald-600' : displayValue < 0 ? 'text-rose-600' : 'text-slate-800';
    const sizeClass = isSub ? 'text-[13px]' : 'text-[14px]';
    return <span className={`font-mono tracking-tight leading-none font-bold ${colorClass} ${sizeClass}`}>{(displayValue * 100).toFixed(2)}%</span>;
  }

  // Color Mode: 为特定指标应用颜色（Daily Report 特有功能）
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

  if (type === 'money' || type === 'profit') {
    return <span className={baseClasses}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
  }
  if (type === 'percent') {
    return <span className={baseClasses}>{(displayValue * 100).toFixed(2)}%</span>;
  }
  return <span className={baseClasses}>{Math.floor(displayValue).toLocaleString()}</span>;
};

export default MetricValue;
