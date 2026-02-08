import React, { useState, useEffect } from 'react';
import { UserPermission } from '../types';
import { authApi, tokenManager } from '../src/api/auth';

export interface LoginPageProps {
  onLogin: (user: UserPermission) => void;
}

const LoginPage: React.FC<LoginPageProps> = ({ onLogin }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [useLocalAuth, setUseLocalAuth] = useState(false);

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
        const storedUsers: UserPermission[] = JSON.parse(localStorage.getItem('ad_tech_users') || '[]');
        const adminUser = {
          id: 'admin',
          name: 'Admin User',
          username: 'admin',
          password: 'password',
          email: 'admin@addata.ai',
          role: 'admin' as const,
          keywords: []
        };
        const allUsers = [adminUser, ...storedUsers];

        const user = allUsers.find(u => u.username === username && u.password === password);
        if (user) onLogin(user);
        else setError('Invalid credentials');
      } else {
        const user = await authApi.login(username, password);
        onLogin(user);
      }
    } catch (err) {
      console.error('Login error:', err);
      const errorMsg = err instanceof Error ? err.message : 'Login failed';
      setError(errorMsg);

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
            placeholder="* * * * * * * *"
          />
          <button type="submit" disabled={loading} className="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-5 rounded-2xl transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
            {loading ? 'Authenticating...' : 'Authenticate'}
          </button>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;
