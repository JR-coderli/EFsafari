/**
 * API Client for AdData Dashboard Backend
 *
 * Handles all HTTP communication with the FastAPI backend.
 */

// API base URL - uses proxy in development, direct URL in production
const API_BASE_URL = import.meta.env.PROD
  ? '/api'  // Production: relative path
  : '/api'; // Development: proxied to backend

// Get auth token from localStorage
function getAuthToken(): string | null {
  return localStorage.getItem('addata_access_token');
}

// Request wrapper with error handling
async function request<T>(
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

  try {
    const response = await fetch(url, defaultOptions);

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(
        errorData.detail || errorData.error || `HTTP ${response.status}: ${response.statusText}`
      );
    }

    return response.json();
  } catch (error) {
    if (error instanceof TypeError) {
      // Network error or CORS issue
      throw new Error('Failed to connect to API server. Please ensure the backend is running.');
    }
    throw error;
  }
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
};

/**
 * Daily Report API endpoints
 */
export const dailyReportApi = {
  /**
   * Get daily report data
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
