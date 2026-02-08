import { useState, useEffect } from 'react';
import type { MouseEvent } from 'react';

export const useColumnResize = (initialWidths: Record<string, number>) => {
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>(initialWidths);
  const [resizingColumn, setResizingColumn] = useState<string | null>(null);

  const handleResizeStart = (columnKey: string, e: MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setResizingColumn(columnKey);
  };

  useEffect(() => {
    const handleResizeMove = (e: MouseEvent) => {
      if (resizingColumn && typeof window !== 'undefined') {
        const table = document.querySelector('table') as HTMLTableElement;
        if (table) {
          const rect = table.getBoundingClientRect();
          const newWidth = e.clientX - rect.left;
          if (newWidth >= 80 && newWidth <= 500) {
            setColumnWidths(prev => ({ ...prev, [resizingColumn]: newWidth }));
          }
        }
      }
    };

    const handleResizeEnd = () => {
      setResizingColumn(null);
    };

    if (resizingColumn) {
      document.addEventListener('mousemove', handleResizeMove);
      document.addEventListener('mouseup', handleResizeEnd);
      return () => {
        document.removeEventListener('mousemove', handleResizeMove);
        document.removeEventListener('mouseup', handleResizeEnd);
      };
    }
  }, [resizingColumn]);

  return {
    columnWidths,
    setColumnWidths,
    resizingColumn,
    handleResizeStart
  };
};
