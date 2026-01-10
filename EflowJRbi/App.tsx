
import { GoogleGenAI } from "@google/genai";
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { AdRow, Dimension, MetricConfig, SavedView, UserPermission, DailyBreakdown } from './types';
import { generateMockReport } from './mockData';
import { loadRootData as apiLoadRootData, loadChildData, loadDailyData as apiLoadDailyData } from './src/api/hooks';
import { authApi, usersApi, tokenManager } from './src/api/auth';

interface Filter {
  dimension: Dimension;
  value: string;
}

const calculateMetrics = (data: { impressions: number; clicks: number; conversions: number; spend: number; revenue: number; m_imp: number; m_clicks: number }) => {
  return {
    ctr: data.clicks / (data.impressions || 1),
    cvr: data.conversions / (data.clicks || 1),
    roi: (data.revenue - data.spend) / (data.spend || 1),
    cpa: data.spend / (data.conversions || 1),
    rpa: data.revenue / (data.conversions || 1),
    epc: data.revenue / (data.clicks || 1),
    epv: data.revenue / (data.impressions || 1),
    m_epc: data.revenue / (data.m_clicks || 1),
    m_epv: data.revenue / (data.m_imp || 1),
    m_cpc: data.spend / (data.m_clicks || 1),
    m_cpv: data.spend / (data.m_imp || 1)
  };
};

const ALL_DIMENSIONS: { value: Dimension; label: string }[] = [
  { value: 'platform', label: 'Media' },
  { value: 'advertiser', label: 'Advertiser' },
  { value: 'offer', label: 'Offer' },
  { value: 'campaign_name', label: 'Campaign' },
  { value: 'sub_campaign_name', label: 'Adset' },
  { value: 'creative_name', label: 'Ads' },
];

const DEFAULT_METRICS: MetricConfig[] = [
  { key: 'spend', label: 'Spend', visible: true, type: 'money', group: 'Basic' },
  { key: 'conversions', label: 'Conversions', visible: true, type: 'number', group: 'Basic' },
  { key: 'revenue', label: 'Revenue', visible: true, type: 'money', group: 'Basic' },
  { key: 'impressions', label: 'Impressions', visible: true, type: 'number', group: 'Basic' },
  { key: 'clicks', label: 'Clicks', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_imp', label: 'm_imp', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_clicks', label: 'm_clicks', visible: true, type: 'number', group: 'Basic' },
  { key: 'm_conv', label: 'm_conv', visible: true, type: 'number', group: 'Basic' },
  { key: 'ctr', label: 'CTR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'cvr', label: 'CVR', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'roi', label: 'ROI', visible: true, type: 'percent', group: 'Calculated' },
  { key: 'cpa', label: 'CPA', visible: true, type: 'money', group: 'Calculated' },
];

const MetricValue: React.FC<{ value: number; type: 'money' | 'percent' | 'number'; isSub?: boolean }> = ({ value, type, isSub }) => {
  const baseClasses = `font-mono tracking-tight leading-none ${isSub ? 'text-[12px] text-slate-500 font-medium' : 'text-[13px] text-slate-800 font-bold'}`;
  const displayValue = isFinite(value) ? value : 0;
  if (type === 'money') return <span className={baseClasses}>${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>;
  if (type === 'percent') return <span className={baseClasses}>{(displayValue * 100).toFixed(2)}%</span>;
  return <span className={baseClasses}>{Math.floor(displayValue).toLocaleString()}</span>;
};

const DatePicker: React.FC<{ onRangeChange: (range: string) => void }> = ({ onRangeChange }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [selectedRange, setSelectedRange] = useState('2026 Data');
  const [dateString, setDateString] = useState('2026');
  const dropdownRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const clickOutside = (e: MouseEvent) => { if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) setIsOpen(false); };
    document.addEventListener('mousedown', clickOutside);
    return () => document.removeEventListener('mousedown', clickOutside);
  }, []);
  const handleSelect = (r: string) => {
    setSelectedRange(r);
    if (r === 'Today') setDateString('Jan 9, 2026');
    else if (r === 'Yesterday') setDateString('Jan 8, 2026');
    else if (r === 'Last 7 Days') setDateString('Jan 3 - Jan 9, 2026');
    else if (r === '2026 Data') setDateString('2026');
    setIsOpen(false);
    onRangeChange(r);
  };
  return (
    <div className="relative" ref={dropdownRef}>
      <button onClick={() => setIsOpen(!isOpen)} className="flex items-center gap-3 px-4 py-2 bg-white border border-slate-200 rounded-xl text-xs font-bold text-slate-600 hover:bg-slate-50 transition-all shadow-sm active:scale-95">
        <i className="far fa-calendar text-indigo-500"></i>
        <span>{selectedRange}: {dateString}</span>
        <i className={`fas fa-chevron-down text-[10px] text-slate-400 transition-transform ${isOpen ? 'rotate-180' : ''}`}></i>
      </button>
      {isOpen && (
        <div className="absolute top-full mt-2 left-0 w-48 bg-white rounded-2xl shadow-2xl border border-slate-100 z-[100] p-2">
           {['2026 Data', 'Today', 'Yesterday', 'Last 7 Days', 'Last 14 Days', 'Last 30 Days'].map(r => (
             <button key={r} onClick={() => handleSelect(r)} className="w-full text-left px-3 py-2 text-[11px] font-bold text-slate-600 hover:bg-indigo-50 rounded-xl">{r}</button>
           ))}
        </div>
      )}
    </div>
  );
};

const LoginPage: React.FC<{ onLogin: (user: UserPermission) => void }> = ({ onLogin }) => {
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('password');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [useLocalAuth, setUseLocalAuth] = useState(false);  // Fallback to local auth

  // Check if user is already logged in
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
        // Fallback to local auth
        const storedUsers: UserPermission[] = JSON.parse(localStorage.getItem('ad_tech_users') || '[]');
        const adminUser = { id: 'admin', name: 'Admin User', username: 'admin', password: 'password', email: 'admin@addata.ai', allowedKeywords: [] };
        const allUsers = [adminUser, ...storedUsers];

        const user = allUsers.find(u => u.username === username && u.password === password);
        if (user) onLogin(user);
        else setError('Invalid credentials');
      } else {
        // Use real API
        const user = await authApi.login(username, password);
        onLogin(user);
      }
    } catch (err) {
      console.error('Login error:', err);
      const errorMsg = err instanceof Error ? err.message : 'Login failed';
      setError(errorMsg);

      // Auto-fallback to local auth on API error
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
          <div className="w-16 h-16 bg-indigo-600 rounded-2xl flex items-center justify-center text-white mb-6"><i className="fas fa-chart-line text-3xl"></i></div>
          <h1 className="text-3xl font-extrabold text-slate-900 tracking-tight">AdData AI</h1>
        </div>
        {error && <div className="mb-4 text-rose-500 text-sm font-bold text-center">{error}</div>}
        <form onSubmit={handleLogin} className="space-y-6">
          <input type="text" required className="w-full px-5 py-4 bg-slate-50 border border-slate-100 rounded-2xl" value={username} onChange={e => setUsername(e.target.value)} placeholder="Username" />
          <input type="password" required className="w-full px-5 py-4 bg-slate-50 border border-slate-100 rounded-2xl" value={password} onChange={e => setPassword(e.target.value)} placeholder="••••••••" />
          <button type="submit" disabled={loading} className="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-5 rounded-2xl transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
            {loading ? 'Authenticating...' : 'Authenticate'}
          </button>
        </form>
      </div>
    </div>
  );
};

const Dashboard: React.FC<{ currentUser: UserPermission; onLogout: () => void }> = ({ currentUser, onLogout }) => {
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<AdRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [useMock, setUseMock] = useState(false);  // Fallback to mock if API fails
  const [activeDims, setActiveDims] = useState<Dimension[]>(['platform', 'offer']);
  const [metrics, setMetrics] = useState<MetricConfig[]>(DEFAULT_METRICS);
  const [currentPage, setCurrentPage] = useState<'performance' | 'permissions'>('performance');
  const [activeFilters, setActiveFilters] = useState<Filter[]>([]);
  const [expandedDailyRows, setExpandedDailyRows] = useState<Set<string>>(new Set());
  const [expandedDimRows, setExpandedDimRows] = useState<Set<string>>(new Set());
  const [selectedRange, setSelectedRange] = useState('2026 Data');
  const [quickFilterText, setQuickFilterText] = useState('');
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [showColumnEditor, setShowColumnEditor] = useState(false);
  const [showViewList, setShowViewList] = useState(false);
  const viewsDropdownRef = useRef<HTMLDivElement>(null);
  
  // Permission management state
  const [users, setUsers] = useState<UserPermission[]>([]);
  const [showUserModal, setShowUserModal] = useState(false);
  const [editingUser, setEditingUser] = useState<UserPermission | null>(null);
  const [useLocalUsers, setUseLocalUsers] = useState(false);  // Fallback to local

  // Load users from API on mount (for admin)
  useEffect(() => {
    const loadUsers = async () => {
      if (currentUser.id === 'admin') {
        try {
          const apiUsers = await usersApi.getAllUsers();
          setUsers(apiUsers);
        } catch (err) {
          console.error('Failed to load users from API, using local:', err);
          // Fallback to local storage
          const localUsers = JSON.parse(localStorage.getItem('ad_tech_users') || '[]');
          setUsers(localUsers);
          setUseLocalUsers(true);
        }
      }
    };
    loadUsers();
  }, [currentUser.id]);

  const dragItem = useRef<number | null>(null);
  const dragOverItem = useRef<number | null>(null);

  const STORAGE_KEY = 'ad_tech_saved_views_v3';

  const [savedViews, setSavedViews] = useState<SavedView[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      return saved ? JSON.parse(saved) : [];
    } catch (e) {
      return [];
    }
  });

  const loadRootData = useCallback(async () => {
    setLoading(true);
    setError(null);
    setExpandedDailyRows(new Set());
    setExpandedDimRows(new Set());

    try {
      const currentLevel = activeFilters.length;
      if (currentLevel >= activeDims.length) {
        setData([]);
        setLoading(false);
        return;
      }

      let rawData: AdRow[];

      if (useMock) {
        // Fallback to mock data
        await new Promise(resolve => setTimeout(resolve, 300));
        rawData = generateMockReport(
          activeDims[currentLevel],
          currentLevel,
          activeFilters.map(f => f.value).join('|'),
          activeDims.slice(currentLevel + 1),
          selectedRange
        );
      } else {
        // Use real API
        rawData = await apiLoadRootData(activeDims, activeFilters, selectedRange);
      }

      // Permission filtering based on adset (sub_campaign_name) keywords
      if (currentUser.allowedKeywords && currentUser.allowedKeywords.length > 0) {
        rawData = rawData.filter(row => {
          if (row.dimensionType === 'sub_campaign_name') {
            return currentUser.allowedKeywords.some(kw =>
              row.name.toLowerCase().includes(kw.toLowerCase())
            );
          }
          return true;
        });
      }

      setData(rawData);
    } catch (err) {
      console.error('Error loading data:', err);
      const errorMsg = err instanceof Error ? err.message : 'Failed to load data';
      setError(errorMsg);

      // Auto-fallback to mock on API error
      if (!useMock) {
        console.log('API unavailable, falling back to mock data');
        setUseMock(true);
        // Retry with mock
        setTimeout(() => {
          loadRootData();
        }, 100);
        return;
      }
    } finally {
      setLoading(false);
    }
  }, [activeDims, activeFilters, selectedRange, currentUser, useMock]);

  useEffect(() => { if (currentPage === 'performance') loadRootData(); }, [loadRootData, currentPage]);

  useEffect(() => {
    const clickOutside = (e: MouseEvent) => { if (viewsDropdownRef.current && !viewsDropdownRef.current.contains(e.target as Node)) setShowViewList(false); };
    document.addEventListener('mousedown', clickOutside);
    return () => document.removeEventListener('mousedown', clickOutside);
  }, []);

  const handleSaveView = (e: React.MouseEvent) => {
    e.stopPropagation();
    const viewName = prompt("Save Current View As:");
    if (!viewName || viewName.trim() === "") return;
    const newView = { id: "view_" + Date.now(), name: viewName.trim(), dimensions: [...activeDims], visibleMetrics: metrics.filter(m => m.visible).map(m => m.key as string) };
    setSavedViews(prev => {
      const updated = [...prev, newView];
      localStorage.setItem(STORAGE_KEY, JSON.stringify(updated));
      return updated;
    });
    setShowViewList(false);
  };

  const applyView = (view: SavedView) => {
    setActiveDims([...view.dimensions]);
    setMetrics(prev => {
      const visibleKeys = view.visibleMetrics;
      const sorted = [...prev].sort((a, b) => {
        const idxA = visibleKeys.indexOf(a.key as string);
        const idxB = visibleKeys.indexOf(b.key as string);
        if (idxA === -1 && idxB === -1) return 0;
        if (idxA === -1) return 1;
        if (idxB === -1) return -1;
        return idxA - idxB;
      });
      return sorted.map(m => ({ ...m, visible: visibleKeys.includes(m.key as string) }));
    });
    setShowViewList(false);
    setActiveFilters([]);
    setQuickFilterText('');
  };

  const deleteView = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    if (!window.confirm("Delete this saved view?")) return;
    setSavedViews(prev => {
      const updated = prev.filter(v => v.id !== id);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(updated));
      return updated;
    });
  };

  const toggleDimension = (dim: Dimension) => {
    setActiveDims(prev => prev.includes(dim) ? prev.filter(d => d !== dim) : [...prev, dim]);
    setActiveFilters([]);
  };

  const draggedDimIndex = useRef<number | null>(null);

  const handleDimDragStart = (e: React.DragEvent, idx: number) => {
    draggedDimIndex.current = idx;
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDimDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
  };

  const handleDimDrop = (e: React.DragEvent, dropIdx: number) => {
    e.preventDefault();
    const dragIdx = draggedDimIndex.current;
    if (dragIdx === null || dragIdx === dropIdx) return;

    const newDims = [...activeDims];
    const [removed] = newDims.splice(dragIdx, 1);
    newDims.splice(dropIdx, 0, removed);

    setActiveDims(newDims);
    setActiveFilters([]);
    draggedDimIndex.current = null;
  };

  const handleSort = () => {
    if (dragItem.current !== null && dragOverItem.current !== null) {
      const copyListItems = [...metrics];
      const dragItemContent = copyListItems[dragItem.current];
      copyListItems.splice(dragItem.current, 1);
      copyListItems.splice(dragOverItem.current, 0, dragItemContent);
      dragItem.current = dragOverItem.current;
      dragOverItem.current = null;
      setMetrics(copyListItems);
    }
  };

  const toggleDimExpansion = async (e: React.MouseEvent, row: AdRow) => {
    e.preventDefault(); e.stopPropagation();
    if (!row.hasChild) return;
    const nextExpanded = new Set(expandedDimRows);
    if (nextExpanded.has(row.id)) {
      nextExpanded.delete(row.id);
    } else {
      nextExpanded.add(row.id);
      if (!row.children) {
        const nextLevel = row.level + 1;
        let children: AdRow[];

        // Build filters for this row's hierarchy
        const rowFilters = row.id.split('|').map((v, i) => ({
          dimension: activeDims[i],
          value: v
        }));

        if (useMock) {
          // Use mock data
          children = generateMockReport(
            activeDims[nextLevel],
            nextLevel,
            row.id,
            activeDims.slice(nextLevel + 1),
            selectedRange
          );
        } else {
          // Use real API
          try {
            children = await loadChildData(activeDims, rowFilters, selectedRange, row.id);
          } catch (err) {
            console.error('Error loading child data:', err);
            children = [];
          }
        }

        // Apply permission filtering
        if (currentUser.allowedKeywords && currentUser.allowedKeywords.length > 0) {
          children = children.filter(c => {
            if (c.dimensionType === 'sub_campaign_name') {
              return currentUser.allowedKeywords.some(kw =>
                c.name.toLowerCase().includes(kw.toLowerCase())
              );
            }
            return true;
          });
        }

        setData(prev => {
          const update = (rows: AdRow[]): AdRow[] =>
            rows.map(r =>
              r.id === row.id
                ? { ...r, children }
                : (r.children ? { ...r, children: update(r.children) } : r)
            );
          return update(prev);
        });
      }
    }
    setExpandedDimRows(nextExpanded);
  };

  // Fix: Added toggleDailyBreakdown implementation
  const toggleDailyBreakdown = async (e: React.MouseEvent, rowId: string) => {
    e.preventDefault(); e.stopPropagation();
    const next = new Set(expandedDailyRows);
    if (next.has(rowId)) {
      next.delete(rowId);
    } else {
      next.add(rowId);
      // Load daily data if not already loaded
      // Find the row and load its daily data
      const findRowAndLoadDaily = (rows: AdRow[]): boolean => {
        for (const row of rows) {
          if (row.id === rowId) {
            // Build filters for this row
            const rowFilters = rowId.split('|').map((v, i) => ({
              dimension: activeDims[i],
              value: v
            }));

            // Load daily data
            if (!row.dailyData || row.dailyData.length === 0) {
              apiLoadDailyData(rowFilters, selectedRange, 7).then(dailyData => {
                setData(prev => {
                  const update = (rows: AdRow[]): AdRow[] =>
                    rows.map(r =>
                      r.id === rowId
                        ? { ...r, dailyData }
                        : (r.children ? { ...r, children: update(r.children) } : r)
                    );
                  return update(prev);
                });
              }).catch(err => {
                console.error('Error loading daily data:', err);
              });
            }
            return true;
          }
          if (row.children && findRowAndLoadDaily(row.children)) {
            return true;
          }
        }
        return false;
      };
      findRowAndLoadDaily(data);
    }
    setExpandedDailyRows(next);
  };

  const filteredAndFlattenedData = useMemo(() => {
    const flatten = (rows: AdRow[]): AdRow[] => {
      const results: AdRow[] = [];
      rows.forEach(row => {
        if (quickFilterText && !row.name.toLowerCase().includes(quickFilterText.toLowerCase())) return;
        results.push(row);
        if (expandedDimRows.has(row.id) && row.children) {
          results.push(...flatten(row.children));
        }
      });
      return results;
    };
    return flatten(data);
  }, [data, expandedDimRows, quickFilterText]);

  // User CRUD
  const saveUser = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const keywords = (formData.get('keywords') as string).split(',').map(k => k.trim()).filter(Boolean);
    const userData = {
      name: formData.get('name') as string,
      username: formData.get('username') as string,
      password: formData.get('password') as string,
      email: formData.get('email') as string,
      allowedKeywords: keywords
    };

    try {
      if (useLocalUsers) {
        // Fallback to local storage
        const newUser: UserPermission = {
          id: editingUser?.id || Date.now().toString(),
          ...userData
        };
        let updated;
        if (editingUser) updated = users.map(u => u.id === editingUser.id ? newUser : u);
        else updated = [...users, newUser];
        setUsers(updated);
        localStorage.setItem('ad_tech_users', JSON.stringify(updated));
      } else {
        // Use API
        if (editingUser) {
          const updatedUser = await usersApi.updateUser(editingUser.id, userData);
          setUsers(users.map(u => u.id === editingUser.id ? updatedUser : u));
        } else {
          const newUser = await usersApi.createUser(userData);
          setUsers([...users, newUser]);
        }
      }
      setShowUserModal(false);
      setEditingUser(null);
    } catch (err) {
      console.error('Failed to save user:', err);
      alert(err instanceof Error ? err.message : 'Failed to save user');
    }
  };

  const deleteUser = async (id: string) => {
    if (!confirm('Delete user?')) return;

    try {
      if (useLocalUsers) {
        // Fallback to local storage
        const updated = users.filter(u => u.id !== id);
        setUsers(updated);
        localStorage.setItem('ad_tech_users', JSON.stringify(updated));
      } else {
        // Use API
        await usersApi.deleteUser(id);
        setUsers(users.filter(u => u.id !== id));
      }
    } catch (err) {
      console.error('Failed to delete user:', err);
      alert(err instanceof Error ? err.message : 'Failed to delete user');
    }
  };

  const visibleMetrics = metrics.filter(m => m.visible);

  return (
    <div className="flex h-screen bg-white overflow-hidden text-slate-900 font-sans">
      <aside className={`bg-[#1e293b] text-slate-400 flex flex-col shrink-0 transition-all ${isSidebarOpen ? 'w-64' : 'w-20'}`}>
        <div className="p-6 border-b border-slate-700/50 flex items-center gap-3">
          <div className="w-8 h-8 bg-indigo-500 rounded-lg text-white flex items-center justify-center font-bold"><i className="fas fa-layer-group text-sm"></i></div>
          {isSidebarOpen && <span className="text-white font-black text-sm uppercase italic tracking-tighter">AD DATA AI</span>}
        </div>
        <nav className="flex-1 py-6">
          <button onClick={() => setCurrentPage('performance')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'performance' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
            <i className="fas fa-chart-bar w-5 text-center"></i>
            {isSidebarOpen && <span className="text-sm font-bold">Performance</span>}
          </button>
          <button onClick={() => setCurrentPage('permissions')} className={`w-full flex items-center gap-4 px-6 py-4 transition-colors ${currentPage === 'permissions' ? 'text-white bg-indigo-500/10' : 'hover:bg-slate-800'}`}>
            <i className="fas fa-user-shield w-5 text-center"></i>
            {isSidebarOpen && <span className="text-sm font-bold">Permissions</span>}
          </button>
        </nav>
        <div className="p-4 border-t border-slate-700/50">
          <button onClick={onLogout} className="w-full flex items-center gap-4 px-2 py-3 hover:bg-slate-800 rounded-lg text-rose-400 transition-colors">
            <i className="fas fa-sign-out-alt w-5 text-center"></i>
            {isSidebarOpen && <span className="text-sm font-bold">Sign Out</span>}
          </button>
        </div>
      </aside>

      <main className="flex-1 flex flex-col min-w-0 bg-[#f8fafc]">
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 z-40 shadow-sm shrink-0">
          <div className="flex items-center gap-4">
            <button onClick={() => setIsSidebarOpen(!isSidebarOpen)} className="p-2 hover:bg-slate-50 rounded-lg text-slate-400 transition-colors"><i className="fas fa-bars"></i></button>
            <h2 className="font-extrabold text-slate-800 tracking-tight ml-2 uppercase italic text-sm">{currentPage === 'performance' ? 'Analytics Data' : 'Permissions'}</h2>
            {currentPage === 'performance' && <DatePicker onRangeChange={setSelectedRange} />}
            {/* Data source indicator */}
            {currentPage === 'performance' && (
              <div className={`flex items-center gap-1.5 px-3 py-1 rounded-lg text-[10px] font-bold ${useMock ? 'bg-amber-100 text-amber-700' : 'bg-emerald-100 text-emerald-700'}`}>
                <span className={`w-2 h-2 rounded-full ${useMock ? 'bg-amber-500' : 'bg-emerald-500 animate-pulse'}`}></span>
                {useMock ? 'Mock Data' : 'Live API'}
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            {/* Error notification */}
            {error && (
              <div className="flex items-center gap-2 px-3 py-1.5 bg-rose-100 text-rose-700 rounded-xl text-xs font-bold">
                <i className="fas fa-exclamation-triangle"></i>
                <span>API Error - Using Mock</span>
                <button onClick={() => { setUseMock(false); setError(null); }} className="ml-1 underline hover:text-rose-900">Retry</button>
              </div>
            )}
            {currentPage === 'performance' && (
              <div className="flex items-center gap-2">
                <div className="relative" ref={viewsDropdownRef}>
                  <button onClick={() => setShowViewList(!showViewList)} className={`px-4 py-2 bg-white border ${showViewList ? 'border-indigo-500 ring-2 ring-indigo-500/10' : 'border-slate-200'} text-slate-700 rounded-xl text-xs font-bold shadow-sm flex items-center gap-2 hover:bg-slate-50 transition-all`}>
                    <i className="fas fa-bookmark text-amber-500"></i> Views
                  </button>
                  {showViewList && (
                    <div className="absolute top-full mt-2 right-0 w-80 bg-white rounded-2xl shadow-2xl border border-slate-100 z-[100] p-2 animate-in fade-in zoom-in duration-150">
                      <div className="flex items-center justify-between p-3 border-b border-slate-50 mb-1">
                        <span className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Global Views</span>
                        <button onClick={handleSaveView} className="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-[10px] font-black uppercase hover:bg-indigo-700 transition-all active:scale-95 shadow-md shadow-indigo-100"><i className="fas fa-plus mr-1"></i> Save Current</button>
                      </div>
                      <div className="max-h-72 overflow-y-auto custom-scrollbar p-1">
                        {savedViews.length > 0 ? savedViews.map(v => (
                          <div key={v.id} onClick={() => applyView(v)} className="flex items-center justify-between w-full px-3 py-3 text-[11px] font-bold text-slate-600 hover:bg-indigo-50 hover:text-indigo-700 rounded-xl cursor-pointer group transition-all mb-1 border border-transparent hover:border-indigo-100">
                            <div className="flex items-center gap-2 overflow-hidden"><i className="fas fa-table-list text-slate-300 group-hover:text-indigo-400"></i><span className="truncate">{v.name}</span></div>
                            <button onClick={(e) => deleteView(e, v.id)} className="w-7 h-7 flex items-center justify-center rounded-lg opacity-0 group-hover:opacity-100 text-slate-400 hover:text-rose-500 hover:bg-rose-50 transition-all"><i className="fas fa-trash-can text-[10px]"></i></button>
                          </div>
                        )) : <p className="py-10 text-center text-slate-400 text-[10px]">No saved views</p>}
                      </div>
                    </div>
                  )}
                </div>
                <button onClick={() => setShowColumnEditor(true)} className="px-4 py-2 bg-slate-900 text-white rounded-xl text-xs font-bold shadow-lg shadow-slate-200 flex items-center gap-2 hover:bg-slate-800 transition-colors"><i className="fas fa-columns"></i> Columns</button>
              </div>
            )}
          </div>
        </header>

        {currentPage === 'performance' ? (
          <>
            <div className="px-8 py-5 bg-white border-b border-slate-200 flex flex-col gap-4 z-30 shadow-sm shrink-0">
              <div className="flex items-center gap-4 overflow-x-auto no-scrollbar pb-1">
                <span className="text-[10px] font-black text-slate-400 uppercase tracking-widest whitespace-nowrap">Pivot Layout:</span>
                <div className="flex items-center gap-2">
                  {activeDims.map((dim, idx) => (
                    <div
                      key={dim}
                      draggable
                      onDragStart={(e) => handleDimDragStart(e, idx)}
                      onDragOver={(e) => handleDimDragOver(e)}
                      onDrop={(e) => handleDimDrop(e, idx)}
                      className="flex items-center bg-indigo-50 border border-indigo-100 px-3 py-1.5 rounded-xl gap-2 shadow-sm animate-in fade-in slide-in-from-left-2 cursor-move hover:bg-indigo-100 transition-colors"
                    >
                      <i className="fas fa-grip-vertical text-indigo-300 text-xs"></i>
                      <span className="text-xs font-black text-indigo-700">{ALL_DIMENSIONS.find(d => d.value === dim)?.label}</span>
                      <button onClick={() => toggleDimension(dim)} className="ml-1 text-indigo-200 hover:text-rose-500 transition-colors"><i className="fas fa-times-circle text-xs"></i></button>
                    </div>
                  ))}
                  <div className="h-4 w-px bg-slate-200 mx-2"></div>
                  {ALL_DIMENSIONS.filter(d => !activeDims.includes(d.value)).map(d => (
                    <button key={d.value} onClick={() => toggleDimension(d.value)} className="px-3 py-1.5 border border-dashed border-slate-300 text-slate-400 rounded-xl text-[10px] font-bold hover:border-indigo-400 hover:text-indigo-500 transition-colors">+ {d.label}</button>
                  ))}
                </div>
              </div>
              <div className="flex justify-between items-center">
                <div className="flex gap-2">
                   {activeFilters.length > 0 ? activeFilters.map((f, i) => (
                     <div key={i} className="flex items-center bg-indigo-50 border border-indigo-100 rounded-md px-2 py-1 gap-2">
                        <span className="text-[10px] font-bold text-indigo-700">{f.value}</span>
                        <button onClick={() => setActiveFilters(prev => prev.slice(0, i))} className="text-indigo-300 hover:text-rose-500"><i className="fas fa-times text-[9px]"></i></button>
                     </div>
                   )) : <span className="text-[10px] text-slate-400 italic">Drill down by clicking rows</span>}
                </div>
                <div className="relative w-64">
                   <i className="fas fa-search absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 text-xs"></i>
                   <input type="text" placeholder="Quick Search..." className="w-full pl-9 pr-4 py-2 bg-slate-50 border border-slate-200 rounded-xl text-xs focus:ring-2 focus:ring-indigo-500/20 outline-none" value={quickFilterText} onChange={(e) => setQuickFilterText(e.target.value)} />
                </div>
              </div>
            </div>

            <div className="flex-1 overflow-auto bg-white custom-scrollbar">
              <table className="w-full text-left border-collapse table-fixed min-w-[1400px]">
                <thead>
                  <tr className="bg-slate-50/70 text-[10px] font-black text-slate-400 uppercase tracking-widest border-b border-slate-100 shadow-sm sticky top-0 z-20">
                    <th className="px-8 py-4 sticky left-0 bg-slate-50 z-30 w-[300px] border-r border-slate-100">Grouping Hierarchy</th>
                    {visibleMetrics.map(m => <th key={m.key} className="px-4 py-4 text-right">{m.label}</th>)}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-50">
                  {loading ? <tr><td colSpan={30} className="px-8 py-10"><div className="h-10 bg-slate-50 rounded-2xl w-full animate-pulse"></div></td></tr> : filteredAndFlattenedData.map(row => (
                    <React.Fragment key={row.id}>
                      <tr className="hover:bg-indigo-50/40 transition-all cursor-pointer group" onClick={() => {
                        const nextFilters = row.id.split('|').map((v, i) => ({ dimension: activeDims[i], value: v }));
                        setActiveFilters(nextFilters);
                        setQuickFilterText('');
                      }}>
                        <td className="px-8 py-3 sticky left-0 bg-white z-10 border-r border-slate-50" style={{ paddingLeft: `${row.level * 20 + 32}px` }}>
                          <div className="flex items-center gap-2">
                            <button onClick={(e) => toggleDailyBreakdown(e, row.id)} className={`w-5 h-5 rounded-full flex items-center justify-center transition-colors ${expandedDailyRows.has(row.id) ? 'bg-indigo-600 shadow-sm' : 'bg-slate-100'}`}><div className={`w-1.5 h-1.5 rounded-full ${expandedDailyRows.has(row.id) ? 'bg-white' : 'bg-slate-400'}`}></div></button>
                            {row.hasChild && <button onClick={(e) => toggleDimExpansion(e, row)} className={`w-6 h-6 rounded flex items-center justify-center transition-all bg-slate-50 border border-slate-100 text-slate-400 ${expandedDimRows.has(row.id) ? 'rotate-90' : ''}`}><i className="fas fa-chevron-right text-[10px]"></i></button>}
                            <div className="flex flex-col min-w-0">
                              <span className="text-[13px] font-black text-slate-800 truncate group-hover:text-indigo-600">{row.name}</span>
                              <span className="text-[9px] text-slate-400 font-bold uppercase tracking-wider">{ALL_DIMENSIONS.find(d => d.value === row.dimensionType)?.label}</span>
                            </div>
                          </div>
                        </td>
                        {visibleMetrics.map(m => <td key={m.key} className="px-4 py-3 text-right"><MetricValue value={row[m.key] as number} type={m.type} /></td>)}
                      </tr>
                      {expandedDailyRows.has(row.id) && row.dailyData?.slice(0, 7).map(day => (
                        <tr key={day.date} className="bg-slate-50/50">
                          <td className="px-8 py-2 sticky left-0 bg-slate-50 z-10 border-l-4 border-indigo-600/60 border-r border-slate-50" style={{ paddingLeft: `${row.level * 20 + 72}px` }}><span className="text-[12px] font-bold text-slate-500">{day.date}</span></td>
                          {visibleMetrics.map(m => <td key={m.key} className="px-4 py-2 text-right opacity-80"><MetricValue value={day[m.key as keyof DailyBreakdown] as number || 0} type={m.type} isSub /></td>)}
                        </tr>
                      ))}
                    </React.Fragment>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <div className="flex-1 p-12 overflow-auto bg-slate-50/50">
             <div className="max-w-6xl mx-auto space-y-6">
                <div className="flex justify-between items-center mb-8">
                  <div>
                    <h3 className="text-2xl font-black italic uppercase tracking-tighter">System Permissions</h3>
                    <p className="text-slate-500 text-sm">Manage user access and adset data visibility via keywords.</p>
                  </div>
                  {currentUser.id === 'admin' && (
                    <button onClick={() => { setEditingUser(null); setShowUserModal(true); }} className="bg-indigo-600 text-white px-6 py-3 rounded-2xl font-black text-xs uppercase tracking-widest shadow-lg shadow-indigo-100 hover:bg-indigo-700 transition-all active:scale-95">Add New User</button>
                  )}
                </div>

                <div className="grid grid-cols-1 gap-4">
                  <div className="bg-indigo-600 text-white p-6 rounded-3xl shadow-xl flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className="w-12 h-12 bg-white/20 rounded-xl flex items-center justify-center text-xl font-bold">{currentUser.name.charAt(0)}</div>
                      <div>
                        <div className="font-black text-lg">{currentUser.name} (Current)</div>
                        <div className="text-xs text-indigo-100 font-bold uppercase tracking-widest">{currentUser.email}</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="px-3 py-1 bg-white/20 rounded-lg text-[10px] font-black uppercase inline-block">Role: {currentUser.id === 'admin' ? 'Super Admin' : 'Agent'}</div>
                      <div className="text-[10px] text-indigo-100 mt-2 font-bold uppercase italic tracking-widest">Keywords: {currentUser.allowedKeywords.join(', ') || 'ALL ACCESS'}</div>
                    </div>
                  </div>

                  {currentUser.id === 'admin' && users.map(u => (
                    <div key={u.id} className="bg-white p-6 rounded-3xl border border-slate-100 shadow-sm hover:shadow-md transition-all flex items-center justify-between group">
                      <div className="flex items-center gap-4">
                        <div className="w-12 h-12 bg-slate-100 rounded-xl flex items-center justify-center text-slate-400 font-bold group-hover:bg-indigo-50 group-hover:text-indigo-600 transition-colors">{u.name.charAt(0)}</div>
                        <div>
                          <div className="font-black text-slate-800">{u.name}</div>
                          <div className="text-xs text-slate-400 font-bold uppercase tracking-widest">{u.email}</div>
                        </div>
                      </div>
                      <div className="flex items-center gap-8">
                        <div className="text-right">
                          <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mb-1">Allowed Keywords</div>
                          <div className="flex gap-1 justify-end flex-wrap max-w-xs">
                             {u.allowedKeywords.length > 0 ? u.allowedKeywords.map(kw => (
                               <span key={kw} className="px-2 py-0.5 bg-slate-50 border border-slate-100 text-slate-500 rounded text-[9px] font-bold">{kw}</span>
                             )) : <span className="text-[9px] text-slate-300 italic">Global Access</span>}
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <button onClick={() => { setEditingUser(u); setShowUserModal(true); }} className="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center hover:bg-indigo-100 transition-colors"><i className="fas fa-edit text-xs"></i></button>
                          <button onClick={() => deleteUser(u.id)} className="w-10 h-10 bg-rose-50 text-rose-600 rounded-xl flex items-center justify-center hover:bg-rose-100 transition-colors"><i className="fas fa-trash-alt text-xs"></i></button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
             </div>
          </div>
        )}
      </main>

      {/* User Modal */}
      {showUserModal && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-6">
          <div className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onClick={() => setShowUserModal(false)}></div>
          <form onSubmit={saveUser} className="relative w-full max-w-lg bg-white rounded-3xl shadow-2xl p-8 animate-in zoom-in duration-200">
            <h3 className="text-2xl font-black uppercase italic tracking-tighter mb-6">{editingUser ? 'Edit User' : 'Create New Agent'}</h3>
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Display Name</label>
                  <input required name="name" defaultValue={editingUser?.name} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                </div>
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Email Address</label>
                  <input required type="email" name="email" defaultValue={editingUser?.email} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Username</label>
                  <input required name="username" defaultValue={editingUser?.username} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                </div>
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Password</label>
                  <input required type="password" name="password" defaultValue={editingUser?.password} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                </div>
              </div>
              <div className="space-y-1">
                <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Adset Keywords (Comma separated)</label>
                <input name="keywords" defaultValue={editingUser?.allowedKeywords.join(', ')} placeholder="e.g. ZP, Zp, zp" className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                <p className="text-[9px] text-slate-400 font-bold mt-1 italic">* Agent will only see Adsets (sub-campaigns) containing these strings.</p>
              </div>
            </div>
            <div className="mt-8 flex gap-3">
              <button type="button" onClick={() => setShowUserModal(false)} className="flex-1 py-4 bg-slate-100 text-slate-600 rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-slate-200 transition-all">Cancel</button>
              <button type="submit" className="flex-1 py-4 bg-indigo-600 text-white rounded-2xl font-black text-xs uppercase tracking-widest shadow-xl shadow-indigo-100 hover:bg-indigo-700 transition-all active:scale-[0.98]">Save Permission</button>
            </div>
          </form>
        </div>
      )}

      {showColumnEditor && (
        <div className="fixed inset-0 z-[100] flex justify-end">
          <div className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm" onClick={() => setShowColumnEditor(false)}></div>
          <div className="relative w-[720px] bg-white h-full shadow-2xl flex flex-col p-8 animate-in slide-in-from-right duration-200">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-2xl font-black uppercase italic tracking-tighter">Manage Columns</h3>
              <button onClick={() => setShowColumnEditor(false)} className="w-10 h-10 flex items-center justify-center rounded-full hover:bg-slate-100 text-slate-400 transition-colors"><i className="fas fa-times"></i></button>
            </div>
            <div className="flex-1 flex min-h-0 gap-6">
              <div className="flex-1 flex flex-col min-h-0 bg-slate-50/50 rounded-2xl border border-slate-100 p-5 overflow-y-auto custom-scrollbar">
                {(['Basic', 'Calculated'] as const).map(groupName => (
                  <div key={groupName} className="mb-6">
                    <span className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-3 block">{groupName} Metrics</span>
                    <div className="space-y-2">
                      {metrics.filter(m => m.group === groupName).map(m => (
                        <label key={m.key} className={`flex items-center gap-3 p-3 border rounded-xl cursor-pointer transition-all ${m.visible ? 'border-indigo-500 bg-indigo-50' : 'border-slate-100 bg-white'}`}>
                          <input type="checkbox" checked={m.visible} onChange={() => setMetrics(prev => prev.map(p => p.key === m.key ? { ...p, visible: !p.visible } : p))} className="w-4 h-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-50" />
                          <span className="text-xs font-bold text-slate-700">{m.label}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
              <div className="w-[280px] bg-indigo-50/20 rounded-2xl border border-indigo-100 p-5 flex flex-col overflow-y-auto custom-scrollbar">
                <span className="text-[10px] font-black uppercase tracking-widest text-indigo-600 mb-4 block">Selected Order</span>
                {metrics.filter(m => m.visible).map((m, idx) => (
                   <div key={m.key} draggable onDragStart={() => dragItem.current = idx} onDragEnter={() => dragOverItem.current = idx} onDragEnd={handleSort} onDragOver={e => e.preventDefault()} className="p-3 bg-white border border-indigo-200 rounded-xl flex items-center gap-3 shadow-sm mb-2 cursor-grab">
                      <i className="fas fa-grip-vertical text-indigo-300 text-xs"></i>
                      <span className="text-xs font-bold text-slate-700 truncate flex-1">{m.label}</span>
                   </div>
                ))}
              </div>
            </div>
            <button onClick={() => setShowColumnEditor(false)} className="mt-8 py-5 bg-slate-900 text-white rounded-2xl font-black text-sm uppercase tracking-widest">Apply Layout</button>
          </div>
        </div>
      )}
    </div>
  );
};

const App: React.FC = () => {
  const [user, setUser] = useState<UserPermission | null>(null);
  const handleLogout = () => {
    authApi.logout();  // Clear token and user from localStorage
    setUser(null);
  };
  if (!user) return <LoginPage onLogin={setUser} />;
  return <Dashboard currentUser={user} onLogout={handleLogout} />;
};

export default App;
