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

### 2026-02-06
- 添加 Offer URL 复制按钮不显示问题案例

---

## 问题案例：Offer URL 复制按钮不显示

### 问题现象
- Performance 页面 Offer 维度下不显示复制 URL 按钮
- `row.offerId` 始终为 `undefined`
- 后端 hierarchy API 日志显示确实返回了 `offerID`

### 根本原因
前端 `loadRootData` 函数在包含 `lander` 维度时会跳过 hierarchy 缓存，改用 `/data` API：
```typescript
// hooks.ts 中的问题代码
const shouldBypassHierarchyCache = activeDims.includes('lander');
if (cachedHierarchy && !shouldBypassHierarchyCache) {
  // 使用 hierarchy 数据（包含 offerID）
  return getDataFromHierarchy(...);
}
// 否则调用 /data API（此时没有 offerID）
```

**关键问题**：
1. `/data` API 和 `/hierarchy` API 返回的数据结构不同
2. `/data` API 通过 `format_row_for_frontend` 格式化数据，但 `offerID` 字段没有被正确添加
3. 前端根据维度组合动态选择使用哪个 API，导致某些维度组合下 `offerID` 丢失

### 调试难点
1. **双 API 系统**：前端根据场景使用不同 API，难以追踪数据来源
2. **缓存机制**：前端有多层缓存（hierarchy 缓存、data 缓存），旧数据掩盖了问题
3. **HMR 不稳定**：Vite 热模块替换有时不生效，需要完全刷新
4. **后端 reload 不可靠**：`--reload` 模式有时不会正确重新加载代码

### 解决方案
**方案：统一使用 hierarchy API**

修改 `hooks.ts` 中的 `loadRootData` 函数，让所有情况下都优先使用 hierarchy API：
```typescript
// 移除 shouldBypassHierarchyCache 逻辑
const cachedHierarchy = dataCache.get(hierarchyCacheKey);
if (cachedHierarchy) {
  return getDataFromHierarchy(cachedHierarchy.hierarchy, activeDims, activeFilters, 0);
}

// 没有缓存时，直接加载 hierarchy
const hierarchyData = await loadHierarchy(activeDims, selectedRange, customStart, customEnd);
if (hierarchyData) {
  const result = getDataFromHierarchy(hierarchyData.hierarchy, activeDims, activeFilters, 0);
  return result;
}
```

### 调试技巧
1. **确认数据来源**：在后端添加日志，确认哪个 API 被调用
   ```python
   logger.info(f"[DATA API] User: ..., group_by={dimensions}")
   logger.info(f"[HIERARCHY API] dimensions={dim_list}")
   ```

2. **验证 API 返回**：使用 curl 直接测试 API
   ```bash
   curl "http://localhost:8000/api/dashboard/hierarchy?..." | python -m json.tool
   ```

3. **清除所有缓存**：
   - 浏览器硬刷新 (Ctrl+Shift+R)
   - 清除 Vite 缓存：`rm -rf node_modules/.vite`
   - 重启前后端服务

4. **添加前端调试日志**：在关键位置添加 `console.log`，确认数据流向

### 相关文件
- `backend/api/routers/dashboard.py` - `/hierarchy` 和 `/data` 端点
- `backend/api/database.py` - `format_row_for_frontend` 函数
- `EflowJRbi/src/api/hooks.ts` - `loadRootData` 和 `hierarchyNodeToAdRow` 函数

---

## 修改记录

### 2026-01-22
- 添加 Lander 维度跳转功能失效问题案例
- 强调数据模型更新的完整性要求
