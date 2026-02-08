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

// 时区偏移映射（小时）
export const TIMEZONE_OFFSETS: Record<string, number> = {
  'UTC': 0,
  'Asia/Shanghai': 8,
  'EST': -5,
  'PST': -8,
};

/**
 * 获取指定时区下的当前日期（YYYY-MM-DD 格式）
 * @param timezone 时区，如 'UTC', 'Asia/Shanghai', 'EST', 'PST'
 * @returns 指定时区下的日期字符串
 *
 * 说明：
 * - now.getTime() 返回的是 UTC 时间戳（毫秒）
 * - 我们需要计算这个 UTC 时间戳在目标时区下对应的是哪一天
 * - 例如：UTC 时间是 2026-02-08 16:00:00，在 UTC+8 时区下是 2026-02-09 00:00:00
 */
export function getDateInTimezone(timezone: string): string {
  const now = new Date();
  const offsetHours = TIMEZONE_OFFSETS[timezone] ?? 0;

  // now.getTime() 已经是 UTC 时间戳
  const utcTimestamp = now.getTime();

  // 计算目标时区的时间戳（加上时区偏移）
  const targetTimestamp = utcTimestamp + (offsetHours * 3600000);

  // 转换为 ISO 字符串并提取日期部分
  return new Date(targetTimestamp).toISOString().split('T')[0];
}

/**
 * 将日期字符串格式化为显示格式
 * @param dateString YYYY-MM-DD 格式的日期字符串
 * @returns 格式化的日期字符串，如 "Feb 9, 2026"
 */
export function formatDateString(dateString: string): string {
  const [year, month, day] = dateString.split('-').map(Number);
  return `${MONTHS[month - 1]} ${day}, ${year}`;
}

/**
 * 将日期字符串范围格式化为显示格式
 * @param startDateStr 开始日期字符串 (YYYY-MM-DD)
 * @param endDateStr 结束日期字符串 (YYYY-MM-DD)
 * @returns 格式化的日期范围字符串
 */
export function formatDateStringRange(startDateStr: string, endDateStr: string): string {
  if (startDateStr === endDateStr) {
    return formatDateString(startDateStr);
  }

  const [startYear, startMonth, startDay] = startDateStr.split('-').map(Number);
  const [endYear, endMonth, endDay] = endDateStr.split('-').map(Number);

  if (startYear === endYear) {
    if (startMonth === endMonth) {
      return `${MONTHS[startMonth - 1]} ${startDay} - ${endDay}, ${startYear}`;
    }
    return `${MONTHS[startMonth - 1]} ${startDay} - ${MONTHS[endMonth - 1]} ${endDay}, ${startYear}`;
  }
  return `${MONTHS[startMonth - 1]} ${startDay}, ${startYear} - ${MONTHS[endMonth - 1]} ${endDay}, ${endYear}`;
}

/**
 * 将日期字符串（YYYY-MM-DD）加上指定的天数
 * @param dateString 日期字符串
 * @param days 要加的天数（可为负数）
 * @returns 新的日期字符串
 */
export function addDays(dateString: string, days: number): string {
  const [year, month, day] = dateString.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  date.setUTCDate(date.getUTCDate() + days);
  const newYear = date.getUTCFullYear();
  const newMonth = String(date.getUTCMonth() + 1).padStart(2, '0');
  const newDay = String(date.getUTCDate()).padStart(2, '0');
  return `${newYear}-${newMonth}-${newDay}`;
}

/**
 * 获取日期范围信息
 * @param range 范围类型（Today、Yesterday、Last 7 Days、This Month、Custom 等）
 * @param customStart 自定义开始日期（当 range 为 Custom 时使用）
 * @param customEnd 自定义结束日期（当 range 为 Custom 时使用）
 * @param timezone 可选的时区，用于计算 Today/Yesterday
 * @returns 日期范围信息对象
 */
export function getRangeInfo(range: string, customStart?: Date, customEnd?: Date, timezone?: string): RangeInfo {
  // 确定基准日期（YYYY-MM-DD 格式）
  let baseDateStr: string;

  if (timezone) {
    // 使用时区感知的今天
    baseDateStr = getDateInTimezone(timezone);
  } else {
    // 使用本地时区的今天
    const now = new Date();
    const year = String(now.getFullYear());
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    baseDateStr = `${year}-${month}-${day}`;
  }

  // 计算各种相对日期
  const yesterdayStr = addDays(baseDateStr, -1);
  const sevenDaysAgoStr = addDays(baseDateStr, -6);
  const fourteenDaysAgoStr = addDays(baseDateStr, -13);
  const thirtyDaysAgoStr = addDays(baseDateStr, -29);

  // 本月第一天
  const [year, month,] = baseDateStr.split('-').map(Number);
  const thisMonthStartStr = `${year}-${String(month).padStart(2, '0')}-01`;

  // 根据 range 返回相应信息
  // 为了兼容现有代码，start 和 end 仍然是 Date 对象（表示本地时区）
  // 但 dateString 使用的是正确的时区感知日期
  const baseDate = new Date(baseDateStr + 'T00:00:00Z');
  const yesterdayDate = new Date(yesterdayStr + 'T00:00:00Z');
  const sevenDaysAgoDate = new Date(sevenDaysAgoStr + 'T00:00:00Z');
  const fourteenDaysAgoDate = new Date(fourteenDaysAgoStr + 'T00:00:00Z');
  const thirtyDaysAgoDate = new Date(thirtyDaysAgoStr + 'T00:00:00Z');
  const thisMonthStartDate = new Date(thisMonthStartStr + 'T00:00:00Z');

  switch (range) {
    case 'Today':
      return {
        label: 'Today',
        dateString: formatDateString(baseDateStr),
        start: baseDate,
        end: baseDate
      };
    case 'Yesterday':
      return {
        label: 'Yesterday',
        dateString: formatDateString(yesterdayStr),
        start: yesterdayDate,
        end: yesterdayDate
      };
    case 'Last 7 Days':
      return {
        label: 'Last 7 Days',
        dateString: formatDateStringRange(sevenDaysAgoStr, baseDateStr),
        start: sevenDaysAgoDate,
        end: baseDate
      };
    case 'Last 14 Days':
      return {
        label: 'Last 14 Days',
        dateString: formatDateStringRange(fourteenDaysAgoStr, baseDateStr),
        start: fourteenDaysAgoDate,
        end: baseDate
      };
    case 'Last 30 Days':
      return {
        label: 'Last 30 Days',
        dateString: formatDateStringRange(thirtyDaysAgoStr, baseDateStr),
        start: thirtyDaysAgoDate,
        end: baseDate
      };
    case 'This Month':
      return {
        label: 'This Month',
        dateString: formatDateStringRange(thisMonthStartStr, baseDateStr),
        start: thisMonthStartDate,
        end: baseDate
      };
    case 'Custom':
      if (customStart && customEnd) {
        return {
          label: 'Custom',
          dateString: formatDateStringRange(
            customStart.toISOString().split('T')[0],
            customEnd.toISOString().split('T')[0]
          ),
          start: customStart,
          end: customEnd
        };
      }
      return {
        label: 'Custom',
        dateString: 'Select dates...',
        start: baseDate,
        end: baseDate
      };
    default:
      return {
        label: 'Yesterday',
        dateString: formatDateString(yesterdayStr),
        start: yesterdayDate,
        end: yesterdayDate
      };
  }
}

/**
 * 获取日期范围信息 - 返回字符串版本（用于 Hourly Report）
 * @param range 范围类型
 * @param timezone 时区
 * @returns 包含日期字符串的范围信息
 */
export function getRangeInfoStrings(range: string, timezone?: string): {
  label: string;
  dateString: string;
  startDate: string;
  endDate: string;
} {
  // 确定基准日期（YYYY-MM-DD 格式）
  let baseDateStr: string;

  if (timezone) {
    baseDateStr = getDateInTimezone(timezone);
  } else {
    const now = new Date();
    const year = String(now.getFullYear());
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    baseDateStr = `${year}-${month}-${day}`;
  }

  const yesterdayStr = addDays(baseDateStr, -1);
  const sevenDaysAgoStr = addDays(baseDateStr, -6);
  const fourteenDaysAgoStr = addDays(baseDateStr, -13);
  const thirtyDaysAgoStr = addDays(baseDateStr, -29);
  const [year, month,] = baseDateStr.split('-').map(Number);
  const thisMonthStartStr = `${year}-${String(month).padStart(2, '0')}-01`;

  switch (range) {
    case 'Today':
      return {
        label: 'Today',
        dateString: formatDateString(baseDateStr),
        startDate: baseDateStr,
        endDate: baseDateStr
      };
    case 'Yesterday':
      return {
        label: 'Yesterday',
        dateString: formatDateString(yesterdayStr),
        startDate: yesterdayStr,
        endDate: yesterdayStr
      };
    case 'Last 7 Days':
      return {
        label: 'Last 7 Days',
        dateString: formatDateStringRange(sevenDaysAgoStr, baseDateStr),
        startDate: sevenDaysAgoStr,
        endDate: baseDateStr
      };
    case 'Last 14 Days':
      return {
        label: 'Last 14 Days',
        dateString: formatDateStringRange(fourteenDaysAgoStr, baseDateStr),
        startDate: fourteenDaysAgoStr,
        endDate: baseDateStr
      };
    case 'Last 30 Days':
      return {
        label: 'Last 30 Days',
        dateString: formatDateStringRange(thirtyDaysAgoStr, baseDateStr),
        startDate: thirtyDaysAgoStr,
        endDate: baseDateStr
      };
    case 'This Month':
      return {
        label: 'This Month',
        dateString: formatDateStringRange(thisMonthStartStr, baseDateStr),
        startDate: thisMonthStartStr,
        endDate: baseDateStr
      };
    default:
      return {
        label: 'Yesterday',
        dateString: formatDateString(yesterdayStr),
        startDate: yesterdayStr,
        endDate: yesterdayStr
      };
  }
}

/**
 * 格式化单个日期（保留用于兼容）
 * @param date 日期对象
 * @returns 格式化的日期字符串，如 "Jan 15, 2025"
 */
export function formatDate(date: Date): string {
  return `${MONTHS[date.getMonth()]} ${date.getDate()}, ${date.getFullYear()}`;
}

/**
 * 格式化日期范围（保留用于兼容）
 * @param start 开始日期
 * @param end 结束日期
 * @returns 格式化的日期范围字符串
 */
export function formatRange(start: Date, end: Date): string {
  const startStr = start.toISOString().split('T')[0];
  const endStr = end.toISOString().split('T')[0];
  return formatDateStringRange(startStr, endStr);
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

/**
 * 将 Date 对象按本地日历转换为 YYYY-MM-DD 字符串
 * 与 toISOString().split('T')[0] 不同，本函数使用本地时区而非 UTC
 * @param date 日期对象
 * @returns 本地日历的日期字符串
 */
export function toDateStringLocal(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}
