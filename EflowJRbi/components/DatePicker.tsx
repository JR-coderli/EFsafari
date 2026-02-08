import React, { useState, useRef, useEffect } from 'react';
import { getRangeInfo } from '../utils/dateHelpers';

export interface DatePickerProps {
  onRangeChange: (range: string, start?: Date, end?: Date) => void;
  currentDisplay: string;
  currentRange: string;
}

const DatePicker: React.FC<DatePickerProps> = ({ onRangeChange, currentDisplay, currentRange }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [showCalendar, setShowCalendar] = useState(false);
  const [calendarStart, setCalendarStart] = useState<Date | null>(null);
  const [calendarEnd, setCalendarEnd] = useState<Date | null>(null);
  const [selectingEnd, setSelectingEnd] = useState(false);
  const [currentMonth, setCurrentMonth] = useState(new Date());
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const clickOutside = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setIsOpen(false);
        setShowCalendar(false);
      }
    };
    document.addEventListener('mousedown', clickOutside);
    return () => document.removeEventListener('mousedown', clickOutside);
  }, []);

  const handleQuickSelect = (r: string) => {
    const info = getRangeInfo(r);
    onRangeChange(r, info.start, info.end);
    setIsOpen(false);
  };

  const handleCustomRange = () => {
    setShowCalendar(true);
    setSelectingEnd(false);
    setCalendarStart(null);
    setCalendarEnd(null);
    setCurrentMonth(new Date());
  };

  const handleDateClick = (date: Date) => {
    if (!selectingEnd) {
      setCalendarStart(date);
      setCalendarEnd(date);
      setSelectingEnd(true);
    } else {
      const start = calendarStart || date;
      const end = date;
      setCalendarStart(start);
      setCalendarEnd(end);
      setSelectingEnd(false);
      onRangeChange('Custom', start, end);
      setShowCalendar(false);
      setIsOpen(false);
    }
  };

  const renderCalendar = () => {
    const year = currentMonth.getFullYear();
    const month = currentMonth.getMonth();
    const firstDay = new Date(year, month, 1);
    const lastDay = new Date(year, month + 1, 0);
    const startDate = new Date(firstDay);
    startDate.setDate(startDate.getDate() - firstDay.getDay());
    const days: Date[] = [];
    for (let i = 0; i < 42; i++) {
      const d = new Date(startDate);
      d.setDate(startDate.getDate() + i);
      days.push(d);
    }

    const isStart = (d: Date) => calendarStart && d.toDateString() === calendarStart.toDateString();
    const isEnd = (d: Date) => calendarEnd && d.toDateString() === calendarEnd.toDateString();
    const isInRange = (d: Date) => {
      if (!calendarStart || !calendarEnd) return false;
      const date = d.getTime();
      return date >= calendarStart.getTime() && date <= calendarEnd.getTime();
    };
    const isCurrentMonth = (d: Date) => d.getMonth() === month;

    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const dayNames = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
    const formatStatusDate = (d: Date) => `${d.getMonth() + 1}/${d.getDate()}`;

    return (
      <div className="p-4">
        <div className="flex items-center justify-between mb-4">
          <button type="button" onClick={() => setCurrentMonth(new Date(year, month - 1))} className="p-1 hover:bg-slate-100 rounded"><i className="fas fa-chevron-left text-xs text-slate-500"></i></button>
          <span className="text-xs font-bold text-slate-700">{monthNames[month]} {year}</span>
          <button type="button" onClick={() => setCurrentMonth(new Date(year, month + 1))} className="p-1 hover:bg-slate-100 rounded"><i className="fas fa-chevron-right text-xs text-slate-500"></i></button>
        </div>
        <div className="grid grid-cols-7 gap-1 mb-2">
          {dayNames.map(d => <div key={d} className="text-center text-[10px] font-bold text-slate-400 py-1">{d}</div>)}
        </div>
        <div className="grid grid-cols-7 gap-1">
          {days.map((d, i) => {
            const start = isStart(d);
            const end = isEnd(d);
            const inRange = isInRange(d);
            const current = isCurrentMonth(d);
            let cellClass = "h-7 w-7 flex items-center justify-center text-[10px] rounded cursor-pointer transition-colors ";
            if (start) cellClass += "bg-indigo-600 text-white ";
            else if (end) cellClass += "bg-indigo-600 text-white ";
            else if (inRange) cellClass += "bg-indigo-100 text-indigo-700 ";
            else if (!current) cellClass += "text-slate-300 ";
            else cellClass += "text-slate-600 hover:bg-slate-100 ";
            return <button type="button" key={i} onClick={() => handleDateClick(d)} className={cellClass}>{d.getDate()}</button>;
          })}
        </div>
        <div className="mt-3 pt-3 border-t border-slate-100 flex items-center justify-between">
          <span className="text-[10px] text-slate-500">
            {selectingEnd
              ? (calendarStart ? `From: ${formatStatusDate(calendarStart)} -> To: ?` : 'Select start date')
              : (calendarStart && calendarEnd
                  ? `${formatStatusDate(calendarStart)} - ${formatStatusDate(calendarEnd)}`
                  : 'Select start date')
            }
          </span>
        </div>
      </div>
    );
  };

  return (
    <div className="relative" ref={dropdownRef}>
      <button onClick={() => setIsOpen(!isOpen)} className="flex items-center gap-3 px-4 py-2 bg-white border border-slate-200 rounded-xl text-xs font-bold text-slate-600 hover:bg-slate-50 transition-all shadow-sm active:scale-95">
        <i className="far fa-calendar text-indigo-500"></i>
        <span>{currentDisplay}</span>
        <i className={`fas fa-chevron-down text-[10px] text-slate-400 transition-transform ${isOpen ? 'rotate-180' : ''}`}></i>
      </button>
      {isOpen && (
        <div className="absolute top-full mt-2 left-0 bg-white rounded-2xl shadow-2xl border border-slate-100 z-[9999] overflow-hidden">
          {!showCalendar ? (
            <div className="p-2 w-48">
              <div className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2 px-1">Quick Select</div>
              {['Today', 'Yesterday', 'Last 7 Days', 'Last 14 Days', 'Last 30 Days', 'This Month'].map(r => (
                <button key={r} onClick={() => handleQuickSelect(r)} className={`w-full text-left px-3 py-2 text-[11px] font-bold rounded-xl ${currentRange === r ? 'bg-indigo-100 text-indigo-700' : 'text-slate-600 hover:bg-indigo-50'}`}>
                  {r}
                </button>
              ))}
              <div className="border-t border-slate-100 mt-2 pt-2">
                <button onClick={handleCustomRange} className="w-full text-left px-3 py-2 text-[11px] font-bold text-indigo-600 hover:bg-indigo-50 rounded-xl flex items-center gap-2">
                  <i className="far fa-calendar-alt"></i> Custom Range...
                </button>
              </div>
            </div>
          ) : (
            <div className="w-72">{renderCalendar()}</div>
          )}
        </div>
      )}
    </div>
  );
};

export default DatePicker;
