/**
 * 简单的 Hash 路由工具
 * 用于保持页面状态在 URL 中，刷新后不丢失
 */

export type PageType = 'performance' | 'permissions' | 'daily_report' | 'hourly' | 'config';

const hashMap: Record<PageType, string> = {
  performance: '#/performance',
  daily_report: '#/daily_report',
  hourly: '#/hourly',
  permissions: '#/permissions',
  config: '#/config',
};

const reverseHashMap: Record<string, PageType> = {
  '#/performance': 'performance',
  '#/daily_report': 'daily_report',
  '#/hourly': 'hourly',
  '#/permissions': 'permissions',
  '#/config': 'config',
};

/**
 * 从当前 hash 获取页面类型
 */
export function getPageFromHash(): PageType {
  const hash = window.location.hash || '#/performance';
  return reverseHashMap[hash] || 'performance';
}

/**
 * 导航到指定页面（更新 hash）
 */
export function navigateTo(page: PageType) {
  window.location.hash = hashMap[page];
}

/**
 * 设置当前页面的 hash
 */
export function setHashForPage(page: PageType) {
  window.location.hash = hashMap[page];
}
