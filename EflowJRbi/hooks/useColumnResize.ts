import { useState, useEffect, useRef } from 'react';
import type { MouseEvent } from 'react';

export const useColumnResize = (initialWidths: Record<string, number>) => {
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>(initialWidths);
  const [resizingColumn, setResizingColumn] = useState<string | null>(null);
  const startXRef = useRef<number>(0);
  const startWidthRef = useRef<number>(0);

  const handleResizeStart = (columnKey: string, e: MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();

    const target = e.currentTarget as HTMLElement;
    const th = target.closest('th, td') as HTMLElement;
    if (th) {
      startXRef.current = e.clientX;
      startWidthRef.current = th.getBoundingClientRect().width;
      setResizingColumn(columnKey);
    }
  };

  useEffect(() => {
    const handleResizeMove = (e: MouseEvent) => {
      if (resizingColumn && typeof window !== 'undefined') {
        const deltaX = e.clientX - startXRef.current;
        const newWidth = startWidthRef.current + deltaX;

        // 更宽的范围：50px - 800px
        if (newWidth >= 50 && newWidth <= 800) {
          setColumnWidths(prev => ({ ...prev, [resizingColumn]: newWidth }));
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
