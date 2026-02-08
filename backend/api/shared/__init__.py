"""
共享模块包

包含 API 路由器之间共享的工具函数和常量
"""

from .permissions import build_permission_filter

__all__ = ['build_permission_filter']
