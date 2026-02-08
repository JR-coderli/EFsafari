/**
 * 日期工具函数模块
 *
 * 统一管理日期格式化和范围计算函数
 * 用于消除 App.tsx 和 DailyReport.tsx 中的重复代码
 */

export interface RangeInfo {
  label: string;
  dateString: string;
  start: Date;
  end: Date;
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/**
 * 格式化单个日期
 * @param date 日期对象
 * @returns 格式化的日期字符串，如 "Jan 15, 2025"
 */
export function formatDate(date: Date): string {
  return `${MONTHS[date.getMonth()]} ${date.getDate()}, ${date.getFullYear()}`;
}

/**
 * 格式化日期范围
 * @param start 开始日期
 * @param end 结束日期
 * @returns 格式化的日期范围字符串
 */
export function formatRange(start: Date, end: Date): string {
  if (start.toDateString() === end.toDateString()) {
    return `${MONTHS[start.getMonth()]} ${start.getDate()}, ${start.getFullYear()}`;
  }
  if (start.getFullYear() === end.getFullYear()) {
    if (start.getMonth() === end.getMonth()) {
      return `${MONTHS[start.getMonth()]} ${start.getDate()} - ${end.getDate()}, ${start.getFullYear()}`;
    }
    return `${MONTHS[start.getMonth()]} ${start.getDate()} - ${MONTHS[end.getMonth()]} ${end.getDate()}, ${start.getFullYear()}`;
  }
  return `${MONTHS[start.getMonth()]} ${start.getDate()}, ${start.getFullYear()} - ${MONTHS[end.getMonth()]} ${end.getDate()}, ${end.getFullYear()}`;
}

/**
 * 获取日期范围信息
 * @param range 范围类型（Today、Yesterday、Last 7 Days、This Month、Custom 等）
 * @param customStart 自定义开始日期（当 range 为 Custom 时使用）
 * @param customEnd 自定义结束日期（当 range 为 Custom 时使用）
 * @returns 日期范围信息对象
 */
export function getRangeInfo(range: string, customStart?: Date, customEnd?: Date): RangeInfo {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  const sevenDaysAgo = new Date(today);
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
  const fourteenDaysAgo = new Date(today);
  fourteenDaysAgo.setDate(fourteenDaysAgo.getDate() - 13);
  const thirtyDaysAgo = new Date(today);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 29);
  // 本月：从1号到今天
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
}

/**
 * 将日期格式化为 API 需要的字符串格式 (YYYY-MM-DD)
 * @param date 日期对象
 * @returns 格式化的日期字符串
 */
export function formatDateForApi(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}
