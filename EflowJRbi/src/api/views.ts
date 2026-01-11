/**
 * Saved Views API client
 */

import { SavedView, Dimension } from '../../types';
import { tokenManager } from './auth';

interface ApiSavedView {
  id: string;
  user_id: string;
  name: string;
  dimensions: string[];
  visible_metrics: string[];
  color_mode: boolean;
  is_default: boolean;
  created_at: string;
  updated_at: string | null;
}

function toFrontendView(apiView: ApiSavedView): SavedView {
  return {
    id: apiView.id,
    name: apiView.name,
    dimensions: apiView.dimensions as Dimension[],
    visibleMetrics: apiView.visible_metrics,
    colorMode: apiView.color_mode,
    userId: apiView.user_id,
    createdAt: apiView.created_at,
    isDefault: apiView.is_default
  };
}

function toApiView(view: SavedView): Omit<ApiSavedView, 'id' | 'user_id' | 'created_at' | 'updated_at'> {
  return {
    name: view.name,
    dimensions: view.dimensions,
    visible_metrics: view.visibleMetrics,
    color_mode: view.colorMode || false,
    is_default: view.isDefault || false
  };
}

export const viewsApi = {
  /**
   * Get all views for current user
   */
  async getAllViews(): Promise<SavedView[]> {
    const token = tokenManager.getToken();
    const response = await fetch('/api/views', {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error('Failed to fetch views');
    }

    const data = await response.json();
    return data.views.map(toFrontendView);
  },

  /**
   * Create a new view
   */
  async createView(view: SavedView): Promise<SavedView> {
    const token = tokenManager.getToken();
    const response = await fetch('/api/views', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(toApiView(view)),
    });

    if (!response.ok) {
      throw new Error('Failed to create view');
    }

    const data: ApiSavedView = await response.json();
    return toFrontendView(data);
  },

  /**
   * Get the default view
   */
  async getDefaultView(): Promise<SavedView | null> {
    const token = tokenManager.getToken();
    const response = await fetch('/api/views/default', {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      return null;
    }

    const data = await response.json();
    return data.view ? toFrontendView(data.view) : null;
  },

  /**
   * Set a view as default
   */
  async setDefaultView(viewId: string): Promise<SavedView> {
    const token = tokenManager.getToken();
    const response = await fetch(`/api/views/${viewId}/set-default`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error('Failed to set default view');
    }

    const data: ApiSavedView = await response.json();
    return toFrontendView(data);
  },

  /**
   * Update a view
   */
  async updateView(view: SavedView): Promise<SavedView> {
    const token = tokenManager.getToken();
    const response = await fetch(`/api/views/${view.id}`, {
      method: 'PUT',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(toApiView(view)),
    });

    if (!response.ok) {
      throw new Error('Failed to update view');
    }

    const data: ApiSavedView = await response.json();
    return toFrontendView(data);
  },

  /**
   * Delete a view
   */
  async deleteView(viewId: string): Promise<void> {
    const token = tokenManager.getToken();
    const response = await fetch(`/api/views/${viewId}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      throw new Error('Failed to delete view');
    }
  },
};
