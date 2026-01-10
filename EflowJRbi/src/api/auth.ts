/**
 * Authentication API client for user login and token management.
 */

interface LoginRequest {
  username: string;
  password: string;
}

interface LoginResponse {
  access_token: string;
  token_type: string;
  user: {
    id: string;
    name: string;
    username: string;
    email: string;
    allowed_keywords: string[];
    created_at: string;
    updated_at: string | null;
  };
}

interface ApiUser {
  id: string;
  name: string;
  username: string;
  email: string;
  allowed_keywords: string[];
  created_at: string;
  updated_at: string | null;
}

interface User {
  id: string;
  name: string;
  username: string;
  password?: string;
  email: string;
  allowedKeywords: string[];
}

// Convert API user to frontend User
function toFrontendUser(apiUser: ApiUser): User {
  return {
    id: apiUser.id,
    name: apiUser.name,
    username: apiUser.username,
    email: apiUser.email,
    allowedKeywords: apiUser.allowed_keywords,
  };
}

/**
 * Token storage management
 */
const TOKEN_KEY = 'addata_access_token';
const USER_KEY = 'addata_user';

export const tokenManager = {
  getToken(): string | null {
    return localStorage.getItem(TOKEN_KEY);
  },

  setToken(token: string): void {
    localStorage.setItem(TOKEN_KEY, token);
  },

  removeToken(): void {
    localStorage.removeItem(TOKEN_KEY);
  },

  getUser(): User | null {
    const userJson = localStorage.getItem(USER_KEY);
    if (userJson) {
      try {
        return JSON.parse(userJson);
      } catch {
        return null;
      }
    }
    return null;
  },

  setUser(user: User): void {
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  },

  removeUser(): void {
    localStorage.removeItem(USER_KEY);
  },

  clear(): void {
    this.removeToken();
    this.removeUser();
  },

  isAuthenticated(): boolean {
    return !!this.getToken();
  },
};

/**
 * Auth API endpoints
 */
export const authApi = {
  /**
   * Login with username and password
   */
  async login(username: string, password: string): Promise<User> {
    const response = await fetch('/api/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ username, password }),
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Login failed' }));
      throw new Error(error.detail || 'Login failed');
    }

    const data: LoginResponse = await response.json();

    // Store token and user
    tokenManager.setToken(data.access_token);
    const user = toFrontendUser(data.user);
    tokenManager.setUser(user);

    return user;
  },

  /**
   * Verify current token and get user info
   */
  async verifyToken(): Promise<User> {
    const token = tokenManager.getToken();
    if (!token) {
      throw new Error('No token found');
    }

    const response = await fetch('/api/auth/me', {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      tokenManager.clear();
      throw new Error('Invalid token');
    }

    const data: ApiUser = await response.json();
    const user = toFrontendUser(data);
    tokenManager.setUser(user);

    return user;
  },

  /**
   * Logout
   */
  logout(): void {
    tokenManager.clear();
  },

  /**
   * Get authorization header
   */
  getAuthHeader(): { Authorization: string } | {} {
    const token = tokenManager.getToken();
    if (token) {
      return { Authorization: `Bearer ${token}` };
    }
    return {};
  },
};

/**
 * Users API endpoints (admin only)
 */
export const usersApi = {
  /**
   * Get all users (admin only)
   */
  async getAllUsers(): Promise<User[]> {
    const token = tokenManager.getToken();
    const response = await fetch('/api/users', {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error('Failed to fetch users');
    }

    const data = await response.json();
    return data.users.map(toFrontendUser);
  },

  /**
   * Create a new user (admin only)
   */
  async createUser(user: {
    name: string;
    username: string;
    email: string;
    password: string;
    allowedKeywords: string[];
  }): Promise<User> {
    const token = tokenManager.getToken();
    const response = await fetch('/api/users', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: user.name,
        username: user.username,
        email: user.email,
        password: user.password,
        allowed_keywords: user.allowedKeywords,
      }),
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Failed to create user' }));
      throw new Error(error.detail || 'Failed to create user');
    }

    const data: ApiUser = await response.json();
    return toFrontendUser(data);
  },

  /**
   * Update a user (admin only)
   */
  async updateUser(
    userId: string,
    updates: {
      name?: string;
      email?: string;
      password?: string;
      allowedKeywords?: string[];
    }
  ): Promise<User> {
    const token = tokenManager.getToken();

    const body: Record<string, unknown> = {};
    if (updates.name !== undefined) body.name = updates.name;
    if (updates.email !== undefined) body.email = updates.email;
    if (updates.password !== undefined) body.password = updates.password;
    if (updates.allowedKeywords !== undefined) body.allowed_keywords = updates.allowedKeywords;

    const response = await fetch(`/api/users/${userId}`, {
      method: 'PUT',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Failed to update user' }));
      throw new Error(error.detail || 'Failed to update user');
    }

    const data: ApiUser = await response.json();
    return toFrontendUser(data);
  },

  /**
   * Delete a user (admin only)
   */
  async deleteUser(userId: string): Promise<void> {
    const token = tokenManager.getToken();
    const response = await fetch(`/api/users/${userId}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      throw new Error('Failed to delete user');
    }
  },
};

export default authApi;
