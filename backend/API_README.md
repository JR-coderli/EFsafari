# API 开发注意事项

## 数据模型更新规范

**重要**: 当修改后端数据模型时，必须同时更新以下文件：

1. **`backend/api/models/schemas.py`** - Pydantic 模型定义
2. **`backend/api/database.py`** - format_row_for_frontend 等格式化函数
3. **`backend/api/routers/dashboard.py`** - API 路由中的查询逻辑

### 常见错误

#### 错误案例：Lander 维度跳转功能失效

**问题现象**: 前端无法获取 `landerUrl` 字段，导致跳转图标不显示。

**根本原因**:
- `database.py` 中的 `format_row_for_frontend` 函数正确添加了 `landerUrl` 到返回字典
- 但 `schemas.py` 中的 `AdRow` 模型**没有定义** `landerUrl` 字段
- Pydantic 序列化时会过滤掉未定义的字段，导致前端收不到该字段

**错误代码**:
```python
# schemas.py - 缺少 landerUrl 定义
class AdRow(BaseModel):
    id: str
    name: str
    # ... 其他字段
    # ❌ 缺少: landerUrl: Optional[str] = None
```

**正确代码**:
```python
# schemas.py - 包含完整字段定义
class AdRow(BaseModel):
    id: str
    name: str
    # ... 其他字段
    landerUrl: Optional[str] = None  # ✅ 正确：定义了 landerUrl
```

### 检查清单

添加新字段后，务必检查：

- [ ] `schemas.py` 中的模型是否定义了新字段
- [ ] `database.py` 中的格式化函数是否添加了新字段
- [ ] 本地测试 API 响应是否包含新字段
- [ ] 提交代码前确保所有相关文件都已修改
- [ ] 部署后验证服务器上的代码和本地一致

### 调试技巧

如果前端收不到某个字段：

1. 检查 `schemas.py` 模型定义
2. 检查 `database.py` 格式化函数
3. 直接测试 API 响应（使用 curl 或 Python 脚本）
4. 对比本地和服务器的文件差异

---

## 修改记录

### 2026-01-22
- 添加 Lander 维度跳转功能失效问题案例
- 强调数据模型更新的完整性要求
