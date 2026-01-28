import React, { useState, useEffect } from 'react';
import { UserPermission } from '../types';
import { tokenManager } from '../src/api/auth';

interface ConfigCard {
  id: string;
  title: string;
  icon: string;
  description: string;
  action: () => void;
  color: string;
}

interface SpecialMediaConfig {
  dates_special_media: string[];
  hourly_special_media: string[];
}

interface SchedulerTask {
  name: string;
  schedule: string;
  icon: string;
  color: string;
  last_run: string | null;
  last_status: 'success' | 'failed' | 'unknown';
  duration: number | null;
  record_count: number | null;
  error_message: string | null;
}

const Config: React.FC<{ currentUser: UserPermission }> = ({ currentUser }) => {
  const [showModal, setShowModal] = useState(false);
  const [modalTitle, setModalTitle] = useState('');
  const [modalContent, setModalContent] = useState<React.ReactNode>(null);
  const [specialMedia, setSpecialMedia] = useState<SpecialMediaConfig>({
    dates_special_media: [],
    hourly_special_media: ['mintegral', 'hastraffic', 'jmmobi', 'brainx']
  });
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [schedulerTasks, setSchedulerTasks] = useState<Record<string, SchedulerTask>>({});
  const [selectedTaskLog, setSelectedTaskLog] = useState<string | null>(null);
  const [selectedTaskName, setSelectedTaskName] = useState<string>('');

  // ETL 运行日志状态
  const [etlLogTitle, setEtlLogTitle] = useState('');
  const [etlLogContent, setEtlLogContent] = useState<string[]>([]);
  const [etlLogTaskId, setEtlLogTaskId] = useState<string | null>(null);
  const [showEtlLog, setShowEtlLog] = useState(false);
  const [isEtlRunning, setIsEtlRunning] = useState(false);

  // 特殊媒体编辑状态
  const [editingMediaType, setEditingMediaType] = useState<'dates' | 'hourly' | null>(null);
  const [tempKeywords, setTempKeywords] = useState('');

  // 轮询 ETL 任务日志
  useEffect(() => {
    let intervalId: NodeJS.Timeout | null = null;

    const pollEtlLog = async () => {
      if (!etlLogTaskId || !isEtlRunning) return;

      try {
        const token = tokenManager.getToken();
        const response = await fetch(`/api/scheduler/log/${etlLogTaskId}?lines=100`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
          const result = await response.json();
          const log = result.log || '暂无日志';

          // 检查任务是否完成
          const isCompleted = log.includes('Task completed') ||
                              log.includes('ETL completed') ||
                              (log.includes('ERROR') && log.includes('Traceback'));

          setEtlLogContent(log.split('\n'));

          if (isCompleted) {
            setIsEtlRunning(false);
            if (intervalId) {
              clearInterval(intervalId);
              intervalId = null;
            }
            setTimeout(() => loadSchedulerStatus(), 2000);
          }
        }
      } catch (error) {
        console.error('Failed to poll ETL log:', error);
      }
    };

    if (etlLogTaskId && isEtlRunning) {
      pollEtlLog();
      intervalId = setInterval(pollEtlLog, 2000);
    }

    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [etlLogTaskId, isEtlRunning]);

  // 加载特殊媒体配置
  useEffect(() => {
    loadSpecialMediaConfig();
    loadSchedulerStatus();
    // 每30秒刷新一次定时任务状态
    const interval = setInterval(loadSchedulerStatus, 30000);
    return () => clearInterval(interval);
  }, []);

  const loadSpecialMediaConfig = async () => {
    try {
      const token = tokenManager.getToken();
      const response = await fetch('/api/config/special-media', {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (response.ok) {
        const data = await response.json();
        setSpecialMedia(data);
      }
    } catch (error) {
      console.error('Failed to load special media config:', error);
    }
  };

  const loadSchedulerStatus = async () => {
    try {
      const token = tokenManager.getToken();
      const response = await fetch('/api/scheduler/status', {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (response.ok) {
        const result = await response.json();
        setSchedulerTasks(result.data);
      }
    } catch (error) {
      console.error('Failed to load scheduler status:', error);
    }
  };

  const loadTaskLog = async (taskId: string, taskName: string) => {
    setLoading(true);
    try {
      const token = tokenManager.getToken();
      const response = await fetch(`/api/scheduler/log/${taskId}?lines=200`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (response.ok) {
        const result = await response.json();
        setSelectedTaskName(taskName);
        setSelectedTaskLog(result.log || '暂无日志');
      } else {
        setSelectedTaskLog('无法读取日志');
      }
    } catch (error) {
      setSelectedTaskLog('读取日志失败');
    } finally {
      setLoading(false);
    }
  };

  const triggerTask = async (taskId: string, taskName?: string) => {
    setLoading(true);
    try {
      const token = tokenManager.getToken();
      const response = await fetch(`/api/scheduler/trigger/${taskId}`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const result = await response.json();
      if (response.ok) {
        // 获取任务名称
        const taskNames: Record<string, string> = {
          'offers': 'Offers ETL',
          'lander': 'Lander URLs Sync',
          'hourly': 'Hourly ETL'
        };
        const name = taskName || taskNames[taskId] || taskId;

        // 设置 ETL 日志状态
        setEtlLogTitle(`${name} 运行日志`);
        setEtlLogContent(['正在启动任务...']);
        setEtlLogTaskId(taskId);
        setIsEtlRunning(true);
        setShowEtlLog(true);
      } else {
        showMessage('error', result.detail || '触发任务失败');
      }
    } catch (error: any) {
      showMessage('error', error.message || '触发任务失败');
    } finally {
      setLoading(false);
    }
  };

  const showMessage = (type: 'success' | 'error', text: string) => {
    setMessage({ type, text });
    setTimeout(() => setMessage(null), 3000);
  };

  // 数据拉取功能
  const triggerDataPull = async (type: 'yesterday' | 'hourly') => {
    setLoading(true);

    try {
      const token = tokenManager.getToken();

      if (type === 'hourly') {
        // Hourly 数据拉取 - 使用 scheduler trigger API
        const response = await fetch('/api/scheduler/trigger/hourly', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${token}` }
        });
        const result = await response.json();

        if (response.ok) {
          // 设置 ETL 日志状态
          setEtlLogTitle('Hourly ETL 运行日志');
          setEtlLogContent(['正在启动任务...']);
          setEtlLogTaskId('hourly');
          setIsEtlRunning(true);
          setShowEtlLog(true);
        } else {
          showMessage('error', result.detail || '触发 Hourly ETL 失败');
        }
      } else {
        // Yesterday 数据拉取 - 使用原有的 config API
        const response = await fetch(`/api/config/pull-data/${type}`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${token}` }
        });
        const result = await response.json();

        if (response.ok) {
          showMessage('success', result.message || 'Yesterday 数据拉取已启动');
          // 刷新定时任务状态以获取最新日志
          setTimeout(() => loadSchedulerStatus(), 3000);
        } else {
          showMessage('error', result.detail || 'Failed to pull data');
        }
      }
    } catch (error: any) {
      showMessage('error', error.message || 'Failed to pull data');
    } finally {
      setLoading(false);
    }
  };

  // 保存特殊媒体配置
  const saveSpecialMedia = async (type: 'dates' | 'hourly', keywords: string[]) => {
    setLoading(true);
    try {
      const token = tokenManager.getToken();
      const response = await fetch('/api/config/special-media', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ type, keywords })
      });
      const result = await response.json();
      if (response.ok) {
        setSpecialMedia(result);
        showMessage('success', 'Special media configuration saved');
        setShowModal(false);
      } else {
        showMessage('error', result.detail || 'Failed to save configuration');
      }
    } catch (error: any) {
      showMessage('error', error.message || 'Failed to save configuration');
    } finally {
      setLoading(false);
    }
  };

  // 打开特殊媒体编辑模态框
  const openSpecialMediaModal = (type: 'dates' | 'hourly') => {
    setEditingMediaType(type);
    const keywords = type === 'dates'
      ? specialMedia.dates_special_media.join(', ')
      : specialMedia.hourly_special_media.join(', ');
    setTempKeywords(keywords);

    const title = type === 'dates'
      ? 'Dates Report 特殊媒体配置'
      : 'Hourly 特殊媒体配置';

    const description = type === 'dates'
      ? '配置 Dates Report 中 spend = revenue 的特殊媒体关键词（逗号分隔）'
      : '配置 Hourly Report 中 spend = revenue 的特殊媒体关键词（逗号分隔）';

    const hint = type === 'hourly' ? (
      <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 text-xs text-amber-800">
        <i className="fas fa-lightbulb mr-2"></i>
        默认包含: mintegral, hastraffic, jmmobi, brainx
      </div>
    ) : null;

    const placeholder = type === 'dates'
      ? '例如: mintegral, hastraffic'
      : '例如: mintegral, hastraffic, jmmobi, brainx';

    setModalTitle(title);
    setModalContent(
      <div className="space-y-4">
        <p className="text-slate-600 text-sm">{description}</p>
        {hint}
        <input
          type="text"
          value={tempKeywords}
          onChange={(e) => setTempKeywords(e.target.value)}
          placeholder={placeholder}
          className="w-full px-4 py-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
        />
        <div className="flex justify-end gap-3">
          <button
            onClick={() => setShowModal(false)}
            className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg"
          >
            取消
          </button>
          <button
            onClick={() => {
              const keywords = tempKeywords.split(',').map(k => k.trim()).filter(k => k);
              saveSpecialMedia(type, keywords);
            }}
            disabled={loading}
            className="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50"
          >
            {loading ? <i className="fas fa-spinner fa-spin mr-2"></i> : null}
            保存配置
          </button>
        </div>
      </div>
    );
    setShowModal(true);
  };

  // 定时任务状态卡片
  const renderSchedulerCard = (taskId: string, task: SchedulerTask) => {
    const statusConfig = {
      success: { bg: 'bg-emerald-100', text: 'text-emerald-700', icon: 'fa-check-circle', label: '成功' },
      failed: { bg: 'bg-rose-100', text: 'text-rose-700', icon: 'fa-exclamation-circle', label: '失败' },
      unknown: { bg: 'bg-slate-100', text: 'text-slate-500', icon: 'fa-question-circle', label: '未知' }
    };

    const status = statusConfig[task.last_status];

    return (
      <div
        key={taskId}
        className="bg-white rounded-2xl p-5 shadow-sm border border-slate-200 hover:shadow-md transition-all"
      >
        <div className="flex items-start justify-between mb-4">
          <div className="flex items-center gap-3">
            <div className={`${task.color} w-11 h-11 rounded-xl flex items-center justify-center text-white`}>
              <i className={`fas ${task.icon}`}></i>
            </div>
            <div>
              <h4 className="font-bold text-slate-800">{task.name}</h4>
              <p className="text-xs text-slate-400 flex items-center gap-1">
                <i className="fas fa-clock"></i>
                {task.schedule}
              </p>
            </div>
          </div>
          <span className={`${status.bg} ${status.text} text-xs px-2.5 py-1 rounded-full flex items-center gap-1.5 font-medium`}>
            <i className={`fas ${status.icon}`}></i>
            {status.label}
          </span>
        </div>

        {/* 详细信息 */}
        <div className="bg-slate-50 rounded-xl p-3 mb-4 space-y-2">
          {task.last_run && (
            <div className="flex justify-between text-sm">
              <span className="text-slate-500">最后运行</span>
              <span className="text-slate-700 font-mono text-xs">{task.last_run}</span>
            </div>
          )}
          {task.duration !== null && (
            <div className="flex justify-between text-sm">
              <span className="text-slate-500">耗时</span>
              <span className="text-slate-700 font-mono">{task.duration.toFixed(2)}s</span>
            </div>
          )}
          {task.record_count !== null && (
            <div className="flex justify-between text-sm">
              <span className="text-slate-500">记录数</span>
              <span className="text-slate-700 font-mono">{task.record_count.toLocaleString()}</span>
            </div>
          )}
          {task.last_status === 'failed' && task.error_message && (
            <div className="mt-2 p-2 bg-rose-50 rounded-lg text-xs text-rose-700">
              {task.error_message}
            </div>
          )}
        </div>

        {/* 操作按钮 */}
        <div className="flex gap-2">
          <button
            onClick={() => loadTaskLog(taskId, task.name)}
            className="flex-1 px-3 py-2 text-sm text-slate-600 bg-slate-100 hover:bg-slate-200 rounded-lg transition-colors flex items-center justify-center gap-2"
          >
            <i className="fas fa-file-alt text-xs"></i>
            查看日志
          </button>
          <button
            onClick={() => triggerTask(taskId)}
            disabled={loading}
            className="flex-1 px-3 py-2 text-sm text-white bg-indigo-600 hover:bg-indigo-700 rounded-lg transition-colors flex items-center justify-center gap-2 disabled:opacity-50"
          >
            <i className={`fas ${loading ? 'fa-spinner fa-spin' : 'fa-play'} text-xs`}></i>
            手动触发
          </button>
        </div>
      </div>
    );
  };

  // 数据拉取卡片
  const dataPullCards: ConfigCard[] = [
    {
      id: 'pull-yesterday',
      title: 'Pull Yesterday Data',
      icon: 'fa-calendar-day',
      description: '拉取昨天的完整数据到数据库',
      action: () => {
        setModalTitle('拉取昨天数据');
        setModalContent(
          <div className="space-y-4">
            <p className="text-slate-600">确定要拉取昨天的数据吗？这将同步 Clickflare API 的数据到本地数据库。</p>
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 text-sm text-amber-800">
              <i className="fas fa-info-circle mr-2"></i>
              此操作可能需要几分钟时间，点击开始后将显示实时日志窗口。
            </div>
            <div className="flex justify-end gap-3">
              <button onClick={() => setShowModal(false)} className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg">取消</button>
              <button
                onClick={() => {
                  setShowModal(false);
                  triggerDataPull('yesterday');
                }}
                disabled={loading}
                className="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2"
              >
                {loading ? <i className="fas fa-spinner fa-spin"></i> : <i className="fas fa-download"></i>}
                开始拉取
              </button>
            </div>
          </div>
        );
        setShowModal(true);
      },
      color: 'bg-blue-500'
    },
    {
      id: 'pull-hourly',
      title: 'Pull Hourly Data',
      icon: 'fa-clock',
      description: '立即拉取今天的 Hourly 数据',
      action: () => {
        setModalTitle('拉取 Hourly 数据');
        setModalContent(
          <div className="space-y-4">
            <p className="text-slate-600">确定要拉取今天的 Hourly 数据吗？</p>
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-sm text-blue-800">
              <i className="fas fa-info-circle mr-2"></i>
              Hourly 数据包含今天 0 点到当前时间的所有小时数据。点击开始后将显示实时日志窗口。
            </div>
            <div className="flex justify-end gap-3">
              <button onClick={() => setShowModal(false)} className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg">取消</button>
              <button
                onClick={() => {
                  setShowModal(false);
                  triggerDataPull('hourly');
                }}
                disabled={loading}
                className="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2"
              >
                {loading ? <i className="fas fa-spinner fa-spin"></i> : <i className="fas fa-download"></i>}
                开始拉取
              </button>
            </div>
          </div>
        );
        setShowModal(true);
      },
      color: 'bg-cyan-500'
    }
  ];

  // 特殊媒体配置卡片
  const specialMediaCards: ConfigCard[] = [
    {
      id: 'dates-special-media',
      title: 'Dates Report Special Media',
      icon: 'fa-calendar-alt',
      description: `当前配置: ${specialMedia.dates_special_media.length > 0 ? specialMedia.dates_special_media.join(', ') : '无'}`,
      action: () => openSpecialMediaModal('dates'),
      color: 'bg-purple-500'
    },
    {
      id: 'hourly-special-media',
      title: 'Hourly Special Media',
      icon: 'fa-clock',
      description: `当前配置: ${specialMedia.hourly_special_media.length > 0 ? specialMedia.hourly_special_media.join(', ') : '无'}`,
      action: () => openSpecialMediaModal('hourly'),
      color: 'bg-orange-500'
    }
  ];

  return (
    <div className="flex-1 p-8 overflow-auto bg-slate-50/50">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h2 className="text-3xl font-black italic uppercase tracking-tighter text-slate-800">Config Tools</h2>
          <p className="text-slate-500 text-sm mt-2">数据拉取、定时任务和系统配置工具</p>
        </div>

        {/* Message */}
        {message && (
          <div className={`mb-6 px-4 py-3 rounded-lg flex items-center gap-3 ${message.type === 'success' ? 'bg-emerald-50 text-emerald-800' : 'bg-rose-50 text-rose-800'}`}>
            <i className={`fas ${message.type === 'success' ? 'fa-check-circle' : 'fa-exclamation-circle'}`}></i>
            <span>{message.text}</span>
          </div>
        )}

        {/* 定时任务状态 Section */}
        <section className="mb-10">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-bold text-slate-700 flex items-center gap-2">
              <i className="fas fa-tasks text-indigo-500"></i>
              定时任务状态
            </h3>
            <button
              onClick={loadSchedulerStatus}
              className="text-xs text-slate-500 hover:text-indigo-600 flex items-center gap-1"
            >
              <i className={`fas fa-sync-alt ${loading ? 'fa-spin' : ''}`}></i>
              刷新
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {Object.entries(schedulerTasks).map(([taskId, task]) => renderSchedulerCard(taskId, task))}
          </div>
        </section>

        {/* Data Pull Section */}
        <section className="mb-10">
          <h3 className="text-lg font-bold text-slate-700 mb-4 flex items-center gap-2">
            <i className="fas fa-database text-indigo-500"></i>
            数据拉取
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {dataPullCards.map(card => (
              <button
                key={card.id}
                onClick={card.action}
                className="bg-white rounded-2xl p-6 shadow-sm border border-slate-200 hover:shadow-md hover:border-indigo-300 transition-all text-left group"
              >
                <div className="flex items-start gap-4">
                  <div className={`${card.color} w-12 h-12 rounded-xl flex items-center justify-center text-white text-lg group-hover:scale-110 transition-transform`}>
                    <i className={`fas ${card.icon}`}></i>
                  </div>
                  <div className="flex-1">
                    <h4 className="font-bold text-slate-800 mb-1">{card.title}</h4>
                    <p className="text-sm text-slate-500">{card.description}</p>
                  </div>
                  <i className="fas fa-chevron-right text-slate-300 group-hover:text-indigo-500 transition-colors"></i>
                </div>
              </button>
            ))}
          </div>
        </section>

        {/* Special Media Section */}
        <section>
          <h3 className="text-lg font-bold text-slate-700 mb-4 flex items-center gap-2">
            <i className="fas fa-cog text-indigo-500"></i>
            特殊媒体配置
          </h3>
          <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
            {specialMediaCards.map((card, index) => (
              <button
                key={card.id}
                onClick={card.action}
                className={`w-full p-6 flex items-start gap-4 hover:bg-slate-50 transition-colors ${index > 0 ? 'border-t border-slate-100' : ''}`}
              >
                <div className={`${card.color} w-12 h-12 rounded-xl flex items-center justify-center text-white text-lg`}>
                  <i className={`fas ${card.icon}`}></i>
                </div>
                <div className="flex-1 text-left">
                  <h4 className="font-bold text-slate-800 mb-1">{card.title}</h4>
                  <p className="text-sm text-slate-500">{card.description}</p>
                </div>
                <i className="fas fa-chevron-right text-slate-300"></i>
              </button>
            ))}
          </div>
        </section>
      </div>

      {/* Modal */}
      {showModal && React.createElement(
        'div',
        {
          className: 'fixed inset-0 bg-black/50 flex items-center justify-center z-50',
          onClick: () => !loading && setShowModal(false)
        },
        React.createElement('div', {
          className: 'bg-white rounded-2xl shadow-xl max-w-md w-full mx-4 overflow-hidden',
          onClick: (e: React.MouseEvent) => e.stopPropagation()
        }, [
          React.createElement('div', {
            key: 'header',
            className: 'px-6 py-4 border-b border-slate-100 flex items-center justify-between'
          }, [
            React.createElement('h3', {
              key: 'title',
              className: 'font-bold text-lg text-slate-800'
            }, modalTitle),
            React.createElement('button', {
              key: 'close',
              onClick: () => !loading && setShowModal(false),
              className: 'text-slate-400 hover:text-slate-600'
            }, React.createElement('i', { className: 'fas fa-times' }))
          ]),
          React.createElement('div', {
            key: 'content',
            className: 'p-6'
          }, modalContent)
        ])
      )}

      {/* Log Modal - 定时任务日志 */}
      {selectedTaskLog && (
        <div
          className="fixed inset-0 bg-black/50 flex items-center justify-center z-50"
          onClick={() => setSelectedTaskLog(null)}
        >
          <div
            className="bg-white rounded-2xl shadow-xl max-w-3xl w-full mx-4 max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e: React.MouseEvent) => e.stopPropagation()}
          >
            <div className="px-6 py-4 border-b border-slate-100 flex items-center justify-between">
              <h3 className="font-bold text-lg text-slate-800">
                {selectedTaskName} - 日志
              </h3>
              <button
                onClick={() => setSelectedTaskLog(null)}
                className="text-slate-400 hover:text-slate-600"
              >
                <i className="fas fa-times"></i>
              </button>
            </div>
            <div className="flex-1 overflow-auto p-4 bg-slate-900">
              <pre
                className="text-xs text-green-400 font-mono whitespace-pre-wrap"
                dangerouslySetInnerHTML={{ __html: selectedTaskLog
                  .replace(/&/g, '&amp;')
                  .replace(/</g, '&lt;')
                  .replace(/>/g, '&gt;') }}
              />
            </div>
          </div>
        </div>
      )}

      {/* ETL Log Modal - 运行中的 ETL 任务日志 */}
      {showEtlLog && (
        <div
          className="fixed inset-0 bg-black/50 flex items-center justify-center z-50"
          onClick={() => !isEtlRunning && setShowEtlLog(false)}
        >
          <div
            className="bg-white rounded-2xl shadow-xl max-w-3xl w-full mx-4 max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e: React.MouseEvent) => e.stopPropagation()}
          >
            <div className="px-6 py-4 border-b border-slate-100 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <h3 className="font-bold text-lg text-slate-800">{etlLogTitle}</h3>
                {isEtlRunning && (
                  <span className="flex items-center gap-1 text-xs text-amber-600">
                    <i className="fas fa-spinner fa-spin"></i>
                    运行中...
                  </span>
                )}
              </div>
              <button
                onClick={() => setShowEtlLog(false)}
                className="text-slate-400 hover:text-slate-600"
              >
                <i className="fas fa-times"></i>
              </button>
            </div>
            <div className="flex-1 overflow-auto p-4 bg-slate-900">
              <pre className="text-xs text-green-400 font-mono whitespace-pre-wrap">
                {etlLogContent.length > 0 ? etlLogContent.join('\n') : '等待日志...'}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Config;
