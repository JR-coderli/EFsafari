import React, { useState, useEffect } from 'react';
import { UserPermission, UserRole } from '../types';
import { usersApi } from '../src/api/auth';

export interface PermissionsPageProps {
  currentUser: UserPermission;
}

const PermissionsPage: React.FC<PermissionsPageProps> = ({ currentUser }) => {
  const [users, setUsers] = useState<UserPermission[]>([]);
  const [showUserModal, setShowUserModal] = useState(false);
  const [editingUser, setEditingUser] = useState<UserPermission | null>(null);
  const [useLocalUsers, setUseLocalUsers] = useState(false);

  useEffect(() => {
    const loadUsers = async () => {
      if (currentUser.id === 'admin') {
        try {
          const apiUsers = await usersApi.getAllUsers();
          setUsers(apiUsers);
        } catch (err) {
          console.error('Failed to load users from API, using local:', err);
          const localUsers = JSON.parse(localStorage.getItem('ad_tech_users') || '[]');
          setUsers(localUsers);
          setUseLocalUsers(true);
        }
      }
    };
    loadUsers();
  }, [currentUser.id]);

  const saveUser = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const keywords = (formData.get('keywords') as string).split(',').map(k => k.trim()).filter(Boolean);
    const role = formData.get('role') as string;
    const showRevenue = editingUser?.showRevenue !== false;

    const userData = {
      name: formData.get('name') as string,
      username: formData.get('username') as string,
      password: formData.get('password') as string,
      email: formData.get('email') as string,
      role: role,
      keywords: keywords,
      showRevenue: showRevenue
    };

    try {
      if (useLocalUsers) {
        const newUser: UserPermission = {
          id: editingUser?.id || Date.now().toString(),
          ...userData,
          role: (role as UserRole)
        };
        let updated;
        if (editingUser) updated = users.map(u => u.id === editingUser.id ? newUser : u);
        else updated = [...users, newUser];
        setUsers(updated);
        localStorage.setItem('ad_tech_users', JSON.stringify(updated));
      } else {
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
        const updated = users.filter(u => u.id !== id);
        setUsers(updated);
        localStorage.setItem('ad_tech_users', JSON.stringify(updated));
      } else {
        await usersApi.deleteUser(id);
        setUsers(users.filter(u => u.id !== id));
      }
    } catch (err) {
      console.error('Failed to delete user:', err);
      alert(err instanceof Error ? err.message : 'Failed to delete user');
    }
  };

  return (
    <div className="flex-1 p-12 overflow-auto bg-slate-50/50">
      <div className="max-w-6xl mx-auto space-y-6">
        <div className="flex justify-between items-center mb-8">
          <div>
            <h3 className="text-2xl font-black italic uppercase tracking-tighter">System Permissions</h3>
            <p className="text-slate-500 text-sm">Manage user roles and data visibility via keywords. OPS filters by Adset, Business filters by Offer.</p>
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
              <div className="px-3 py-1 bg-white/20 rounded-lg text-[10px] font-black uppercase inline-block">Role: {currentUser.role || 'admin'}</div>
              <div className="text-[10px] text-indigo-100 mt-2 font-bold uppercase italic tracking-widest">Keywords: {currentUser.keywords?.join(', ') || 'ALL ACCESS'}</div>
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
                  <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mb-1">Role</div>
                  <div className="px-2 py-0.5 bg-slate-50 border border-slate-100 text-slate-500 rounded text-[9px] font-bold">{u.role || 'ops'}</div>
                </div>
                <div className="text-right">
                  <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mb-1">Keywords</div>
                  <div className="flex gap-1 justify-end flex-wrap max-w-xs">
                    {u.keywords?.length > 0 ? u.keywords.map(kw => (
                      <span key={kw} className="px-2 py-0.5 bg-slate-50 border border-slate-100 text-slate-500 rounded text-[9px] font-bold">{kw}</span>
                    )) : <span className="text-[9px] text-slate-300 italic">All Access</span>}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <button onClick={() => {
                    setEditingUser(u);
                    setShowUserModal(true);
                  }} className="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center hover:bg-indigo-100 transition-colors"><i className="fas fa-edit text-xs"></i></button>
                  <button onClick={() => deleteUser(u.id)} className="w-10 h-10 bg-rose-50 text-rose-600 rounded-xl flex items-center justify-center hover:bg-rose-100 transition-colors"><i className="fas fa-trash-alt text-xs"></i></button>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

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
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Username <span className="text-slate-300 font-normal">(min 3 chars)</span></label>
                  <input required name="username" minLength={3} defaultValue={editingUser?.username} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" placeholder="username" />
                </div>
                <div className="space-y-1">
                  <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Password <span className="text-slate-300 font-normal">(min 6 chars)</span></label>
                  <input required type="password" name="password" minLength={6} defaultValue={editingUser?.password} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" placeholder="* * * * * *" />
                </div>
              </div>
              <div className="space-y-1">
                <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Role</label>
                <select name="role" defaultValue={editingUser?.role || 'ops'} className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20">
                  <option value="admin">Admin (Full Access)</option>
                  <option value="ops">OPS (Adset Filter)</option>
                  <option value="ops02">OPS02 (Platform Filter)</option>
                  <option value="business">Business (Offer Filter)</option>
                </select>
              </div>
              <div className="space-y-1">
                <label className="text-[10px] font-black uppercase text-slate-400 tracking-widest">Keywords (Comma separated)</label>
                <input name="keywords" defaultValue={editingUser?.keywords?.join(', ') || ''} placeholder="e.g. ZP, Zp, zp" className="w-full px-4 py-3 bg-slate-50 border border-slate-100 rounded-xl text-xs font-bold outline-none focus:ring-2 focus:ring-indigo-500/20" />
                <p className="text-[9px] text-slate-400 font-bold mt-1 italic">* OPS: Adset, OPS02: Platform, Business: Offer. Empty = all access.</p>
              </div>
              <div className="flex items-center gap-3 px-4 py-3 bg-indigo-50 rounded-xl border border-indigo-100">
                <input
                  type="checkbox"
                  name="showRevenue"
                  id="showRevenue"
                  checked={editingUser?.showRevenue !== false}
                  onChange={(e) => {
                    if (editingUser) {
                      setEditingUser({ ...editingUser, showRevenue: e.target.checked });
                    }
                  }}
                  className="w-5 h-5 rounded border-slate-300 text-indigo-600 focus:ring-indigo-50"
                />
                <label htmlFor="showRevenue" className="text-xs font-bold text-slate-700 cursor-pointer select-none">
                  Show Revenue Columns <span className="text-slate-400 font-normal">(Revenue, Profit, EPA, EPC, etc.)</span>
                </label>
              </div>
            </div>
            <div className="mt-8 flex gap-3">
              <button type="button" onClick={() => setShowUserModal(false)} className="flex-1 py-4 bg-slate-100 text-slate-600 rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-slate-200 transition-all">Cancel</button>
              <button type="submit" className="flex-1 py-4 bg-indigo-600 text-white rounded-2xl font-black text-xs uppercase tracking-widest shadow-xl shadow-indigo-100 hover:bg-indigo-700 transition-all active:scale-[0.98]">Save Permission</button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
};

export default PermissionsPage;
