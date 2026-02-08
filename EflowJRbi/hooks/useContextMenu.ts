import { useState, useEffect, useRef } from 'react';
import type { MouseEvent } from 'react';

export interface ContextMenuState {
  x: number;
  y: number;
  text: string;
}

export const useContextMenu = () => {
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; text: string } | null>(null);
  const contextMenuRef = useRef<HTMLDivElement>(null);

  const handleContextMenu = (e: MouseEvent, text: string) => {
    e.preventDefault();
    setContextMenu({ x: e.clientX, y: e.clientY, text });
  };

  const handleCopyText = () => {
    if (contextMenu) {
      navigator.clipboard.writeText(contextMenu.text);
      setContextMenu(null);
    }
  };

  const closeContextMenu = () => setContextMenu(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (contextMenuRef.current && !contextMenuRef.current.contains(e.target as Node)) {
        closeContextMenu();
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [contextMenu]);

  return {
    contextMenu,
    contextMenuRef,
    handleContextMenu,
    handleCopyText,
    closeContextMenu,
    setContextMenu
  };
};
