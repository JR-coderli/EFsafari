/**
 * API Client for AdData Dashboard Backend
 *
 * Handles all HTTP communication with the FastAPI backend.
 * Includes automatic retry with exponential backoff for transient failures.
 */

// Connection status type
export type ConnectionStatus = 'connected' | 'retrying' | 'failed';

// API base URL - uses proxy in development, direct URL in production
const API_BASE_URL = import.meta.env.PROD
  ? '/api'  // Production: relative path
  : '/api'; // Development: proxied to backend

// Get auth token from localStorage
function getAuthToken(): string | null {
  return localStorage.getItem('addata_access_token');
}

// Retry configuration
const MAX_RETRIES = 5;
const BASE_DELAY = 1000; // 1 second
const MAX_DELAY = 16000; // 16 seconds

// Connection state management
class ConnectionState {
  private currentStatus: ConnectionStatus = 'connected';
  private listeners: Set<(status: ConnectionStatus) => void> = new Set();

  getStatus(): ConnectionStatus {
    return this.currentStatus;
  }

  setStatus(status: ConnectionStatus) {
    this.currentStatus = status;
    this.listeners.forEach(cb => cb(status));
  }

  subscribe(callback: (status: ConnectionStatus) => void): () => void {
    this.listeners.add(callback);
    // Return unsubscribe function
    return () => this.listeners.delete(callback);
  }
}

// Global connection state
const connectionState = new ConnectionState();

// Export functions to access connection status
export function getConnectionStatus(): ConnectionStatus {
  return connectionState.getStatus();
}

export function onConnectionStatusChange(callback: (status: ConnectionStatus) => void): () => void {
  return connectionState.subscribe(callback);
}

// Helper: delay with promise
function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper: calculate exponential backoff delay
function getRetryDelay(attempt: number): number {
  const delay = BASE_DELAY * Math.pow(2, attempt);
  return Math.min(delay, MAX_DELAY);
}

// Check if error is retryable
function isRetryableError(error: Error): boolean {
  const msg = error.message.toLowerCase();

  // Don't retry authentication errors
  if (msg.includes('401') || msg.includes('not authenticated') || msg.includes('unauthorized')) {
    return false;
  }

  // Don't retry client errors (4xx except 408, 429)
  if (msg.includes('http 4') && !msg.includes('408') && !msg.includes('429')) {
    return false;
  }

  // Retry on network errors, timeout, 5xx, 408, 429
  return true;
}

// Core request function with retry logic
async function requestWithRetry<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE_URL}${endpoint}`;

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...options.headers as Record<string, string>,
  };

  // Add auth token if available
  const token = getAuthToken();
  if (token && endpoint !== '/auth/login') {
    headers['Authorization'] = `Bearer ${token}`;
  }

  const defaultOptions: RequestInit = {
    ...options,
    headers,
  };

  let lastError: Error | null = null;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, defaultOptions);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        const errorMsg = errorData.detail || errorData.error || `HTTP ${response.status}: ${response.statusText}`;
        throw new Error(errorMsg);
      }

      // Success - update status and return data
      connectionState.setStatus('connected');
      return response.json();

    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      // Check if error is retryable
      if (!isRetryableError(lastError)) {
        // Not retryable - throw immediately
        throw lastError;
      }

      // Last attempt failed - mark as failed
      if (attempt === MAX_RETRIES - 1) {
        connectionState.setStatus('failed');
        throw lastError;
      }

      // Show retrying status
      connectionState.setStatus('retrying');

      // Wait before retry (exponential backoff)
      const retryDelay = getRetryDelay(attempt);
      await delay(retryDelay);
    }
  }

  // Should never reach here, but TypeScript needs it
  connectionState.setStatus('failed');
  throw lastError || new Error('Request failed');
}

// Legacy request function (for backward compatibility, no retry)
async function request<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  return requestWithRetry<T>(endpoint, options);
}

// Query parameter builder
function buildQueryParams(params: Record<string, string | number | boolean | undefined>): string {
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      searchParams.append(key, String(value));
    }
  });
  return searchParams.toString();
}

/**
 * Dashboard API endpoints
 */
export const dashboardApi = {
  /**
   * Health check
   */
  async health() {
    return request<{ status: string; clickhouse: string; database: string; table: string; row_count?: number }>('/dashboard/health');
  },

  /**
   * Get available platforms
   */
  async getPlatforms() {
    return request<{ platforms: Array<{ name: string; displayName?: string }> }>('/dashboard/platforms');
  },

  /**
   * Get aggregated data
   */
  async getData(params: {
    startDate: string;
    endDate: string;
    groupBy: string[];
    filters?: Array<{ dimension: string; value: string }>;
    limit?: number;
  }) {
    const queryParams = buildQueryParams({
      start_date: params.startDate,
      end_date: params.endDate,
      group_by: params.groupBy.join(','),
      filters: params.filters && params.filters.length > 0
        ? JSON.stringify(params.filters)
        : undefined,
      limit: params.limit ?? 1000,
    });

    return request<{
      data: Array<{
        id: string;
        name: string;
        level: number;
        dimensionType: string;
        impressions: number;
        clicks: number;
        conversions: number;
        spend: number;
        revenue: number;
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
        hasChild: boolean;
      }>;
      total: number;
      dateRange: { start_date: string; end_date: string };
    }>(`/dashboard/data?${queryParams}`);
  },

  /**
   * Get daily breakdown data
   */
  async getDailyData(params: {
    startDate: string;
    endDate: string;
    filters: Array<{ dimension: string; value: string }>;
    limit?: number;
  }) {
    const queryParams = buildQueryParams({
      start_date: params.startDate,
      end_date: params.endDate,
      filters: JSON.stringify(params.filters),
      limit: params.limit ?? 100,
    });

    return request<Array<{
      date: string;
      impressions: number;
      clicks: number;
      conversions: number;
      spend: number;
      revenue: number;
      m_imp: number;
      m_clicks: number;
      m_conv: number;
    }>>(`/dashboard/daily?${queryParams}`);
  },

  /**
   * Get aggregate summary metrics
   */
  async getAggregate(params: {
    startDate: string;
    endDate: string;
    filters?: Array<{ dimension: string; value: string }>;
  }) {
    const queryParams = buildQueryParams({
      start_date: params.startDate,
      end_date: params.endDate,
      filters: params.filters && params.filters.length > 0
        ? JSON.stringify(params.filters)
        : undefined,
    });

    return request<{
      impressions: number;
      clicks: number;
      conversions: number;
      spend: number;
      revenue: number;
      m_imp: number;
      m_clicks: number;
      m_conv: number;
      ctr: number;
      cvr: number;
      roi: number;
      cpa: number;
    }>(`/dashboard/aggregate?${queryParams}`);
  },

  /**
   * Get available dimensions
   */
  async getDimensions() {
    return request<{ dimensions: Array<{ value: string; label: string }> }>('/dashboard/dimensions');
  },

  /**
   * Get available metrics
   */
  async getMetrics() {
    return request<{ metrics: Array<{ key: string; label: string; type: string; group: string }> }>('/dashboard/metrics');
  },

  /**
   * Get ETL status
   */
  async getEtlStatus() {
    return request<{
      last_update: string | null;
      report_date: string | null;
      all_success: boolean;
    }>('/dashboard/etl-status');
  },
};

/**
 * Daily Report API endpoints
 */
export const dailyReportApi = {
  /**
   * Get available dimensions (media and date only)
   */
  async getDimensions() {
    return request<{ dimensions: Array<{ value: string; label: string }> }>('/daily-report/dimensions');
  },

  /**
   * Get hierarchy data (media -> date structure)
   */
  async getHierarchy(params: {
    startDate: string;
    endDate: string;
  }) {
    const queryParams = buildQueryParams({
      start_date: params.startDate,
      end_date: params.endDate,
    });

    return request<{
      dimensions: string[];
      hierarchy: Record<string, {
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
        };
        _children: Record<string, unknown>;
      }>;
      startDate: string;
      endDate: string;
    }>(`/daily-report/hierarchy?${queryParams}`);
  },

  /**
   * Get daily report data (with hierarchy support)
   */
  async getData(params: {
    startDate: string;
    endDate: string;
    media?: string;
  }) {
    const queryParams = buildQueryParams({
      start_date: params.startDate,
      end_date: params.endDate,
      media: params.media,
    });

    return request<Array<{
      id: string;
      name: string;
      level: number;
      dimensionType: string;
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
      hasChild: boolean;
      filterPath: Array<{ dimension: string; value: string }>;
      spend_manual: number;
    }>>(`/daily-report/data?${queryParams}`);
  },

  /**
   * Update spend for a specific date and media
   */
  async updateSpend(data: {
    date: string;
    media: string;
    spend_value: number;
  }) {
    return request<{ success: boolean; message: string }>('/daily-report/update-spend', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  /**
   * Get daily report summary
   */
  async getSummary(params: {
    startDate: string;
    endDate: string;
    media?: string;
  }) {
    const queryParams = buildQueryParams({
      start_date: params.startDate,
      end_date: params.endDate,
      media: params.media,
    });

    return request<{
      impressions: number;
      clicks: number;
      conversions: number;
      revenue: number;
      spend: number;
      m_imp: number;
      m_clicks: number;
      m_conv: number;
      ctr: number;
      cvr: number;
      roi: number;
      cpa: number;
    }>(`/daily-report/summary?${queryParams}`);
  },

  /**
   * Get available media list
   */
  async getMediaList() {
    return request<{ media: Array<{ name: string }> }>('/daily-report/media-list');
  },

  /**
   * Health check
   */
  async health() {
    return request<{ status: string; service: string }>('/daily-report/health');
  },

  /**
   * Sync data from Performance table
   */
  async syncData(params: {
    startDate: string;
    endDate: string;
  }) {
    return request<{ success: boolean; message: string; row_count: number }>('/daily-report/sync', {
      method: 'POST',
      body: JSON.stringify({
        start_date: params.startDate,
        end_date: params.endDate,
      }),
    });
  },

  /**
   * Lock/Unlock a specific date
   */
  async lockDate(params: {
    date: string;
    lock: boolean;
  }) {
    return request<{ success: boolean; message: string }>('/daily-report/lock-date', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  },

  /**
   * Get locked dates list
   */
  async getLockedDates() {
    return request<{ locked_dates: string[] }>('/daily-report/locked-dates');
  },
};

export default dashboardApi;
