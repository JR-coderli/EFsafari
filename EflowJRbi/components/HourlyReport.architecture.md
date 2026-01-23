# HourlyReport 组件架构设计文档

## 概述

HourlyReport 组件是一个支持多时区、多维度下钻的数据报表组件。核心设计目标是：
1. 支持时区切换时正确显示"今天"的数据
2. 支持手动选择任意日期
3. 防止快速操作时的竞态条件
4. 与父组件的日期控件正确同步

---

## 核心状态设计

```typescript
// 内部日期状态：不依赖父组件，由组件自己管理
const [currentDate, setCurrentDate] = useState<string>('2026-01-23');

// 时区状态
const [timezone, setTimezone] = useState<string>('UTC');

// 下钻路径：记录用户点击的筛选层级
const [drillPath, setDrillPath] = useState<DrillPathItem[]>([]);

// 时区切换标志：防止 useEffect 重复触发
const [isTimezoneChanging, setIsTimezoneChanging] = useState(false);

// 时区切换目标日期：防止父组件的日期变化覆盖
const timezoneChangeTargetRef = useRef<string | null>(null);

// 请求序列号：防止旧请求覆盖新请求
const requestIdRef = useRef(0);
```

---

## 流程图

### 1. 时区切换流程

```
┌─────────────────────────────────────────────────────────────────────┐
│                        用户切换时区 (UTC → PST)                        │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  handleTimezoneChange(newTimezone)                                  │
│                                                                     │
│  1. 检查 isTimezoneChanging，防止重复触发                              │
│  2. 计算 PST 时区的"今天"日期 (getDateInTimezone)                      │
│  3. 过滤 drillPath，移除 hour 维度的筛选                               │
│  4. 设置 isTimezoneChanging = true                                   │
│  5. 设置 timezoneChangeTargetRef = 新日期                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  更新组件状态                                                        │
│                                                                     │
│  setTimezone(newTimezone)        ← 更新时区                         │
│  setCurrentDate(todayInNewTimezone) ← 更新日期为 PST 的今天           │
│  setDrillPath(filteredPath)        ← 更新筛选路径                     │
│                                                                     │
│  注意：不调用 onRangeChange，避免触发父组件                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  加载新时区数据                                                        │
│                                                                     │
│  await setTimeout(50ms)          ← 等待状态更新完成                    │
│  fetch('/api/hourly/data')       ← 请求 PST 时区的数据                  │
│  setData(result.data)            ← 更新表格数据                       │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  延迟清除标志 (500ms)                                                   │
│                                                                     │
│  setIsTimezoneChanging(false)                                       │
│  timezoneChangeTargetRef.current = null                             │
└─────────────────────────────────────────────────────────────────────┘
```

### 2. 手动选择日期流程

```
┌─────────────────────────────────────────────────────────────────────┐
│                    用户在日期控件选择 01-22                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  父组件更新 customDateStart                                           │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  useEffect 监听 customDateStart 变化                                  │
│                                                                     │
│  if (isTimezoneChanging &&                                           │
│      timezoneChangeTargetRef.current !== newDate) {                 │
│    return;  // 忽略父组件的日期变化                                   │
│  }                                                                  │
│                                                                     │
│  setCurrentDate(newDate);  // 更新内部日期                           │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  useEffect 监听 currentDate 变化                                       │
│                                                                     │
│  if (isTimezoneChanging) return;  // 时区切换中，跳过                 │
│  loadData();  // 加载新日期的数据                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3. 数据加载与竞态条件防护

```
┌─────────────────────────────────────────────────────────────────────┐
│  loadData() - 带请求序列号的异步加载                                   │
│                                                                     │
│  const currentRequestId = ++requestIdRef.current;  // 生成新 ID      │
│  setLoading(true);                                                  │
│                                                                     │
│  const response = await fetch(...);  // 发起请求                     │
│                                                                     │
│  // ┌─────────────────────────────────────────────────────────────┐ │
│  // │  请求去重检查                                                  │ │
│  // │                                                             │ │
│  // │  if (currentRequestId !== requestIdRef.current) {          │ │
│  // │    return;  // 不是最新请求，忽略响应                         │ │
│  // │  }                                                          │ │
│  // └─────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  setData(result.data);  // 只有最新请求才会设置数据                   │
│  setLoading(false);                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 时序图

### 场景 1: 时区切换 (UTC → PST)

```
用户          组件             useEffect         API           父组件
 │             │                  │              │              │
 │─切换时区───>│                  │              │              │
 │             │                  │              │              │
 │             │─计算 PST 今天───>│              │              │
 │             │  (2026-01-23)    │              │              │
 │             │                  │              │              │
 │             │─setTimezone(PST) │              │              │
 │             │─setCurrentDate() │              │              │
 │             │─setDrillPath()   │              │              │
 │             │                  │              │              │
 │             │─setChanging(true)│              │              │
 │             │                  │              │              │
 │             │─等待 50ms────────│              │              │
 │             │                  │              │              │
 │             │─fetch(PST数据)───>──────────────>│              │
 │             │                  │              │              │
 │             │<────返回数据─────<──────────────│              │
 │             │                  │              │              │
 │             │─setData(8条)     │              │              │
 │             │                  │              │              │
 │             │─500ms 后清除标志─│              │              │
 │             │                  │              │              │
 │             │                  │  (不调用 onRangeChange)       │
 │             │                  │              │              │
 │<──表体更新───│                  │              │              │
```

### 场景 2: 快速操作时的竞态条件防护

```
用户          组件               API              请求ID
 │             │                  │                 │
 │─选择日期───>│                  │                 │
 │             │                  │                 │
 │             │─loadData()       │                 │
 │             │  requestId = 1   │                 │
 │             │                  │                 │
 │             │─fetch(01-22)─────>────────────>     │
 │             │  (请求1进行中)     │                 │
 │             │                  │                 │
 │─再选择日期──>│                  │                 │
 │             │                  │                 │
 │             │─loadData()       │                 │
 │             │  requestId = 2   │                 │
 │             │  (ID变为2)        │                 │
 │             │                  │                 │
 │             │─fetch(01-21)─────>────────────>     │
 │             │  (请求2发出)       │                 │
 │             │                  │                 │
 │             │<────01-22数据────<────────────      │
 │             │  (请求1返回，但ID=1≠2)             │
 │             │  忽略！           │                 │
 │             │                  │                 │
 │             │<────01-21数据────<────────────      │
 │             │  (请求2返回，ID=2===2)             │
 │             │  setData() ✓     │                 │
 │             │                  │                 │
 │<──显示01-21──│                  │                 │
```

### 场景 3: 父组件干扰防护

```
父组件        组件              useEffect
 │             │                  │
 │             │─切换时区(PST)     │
 │             │  setChanging=true │
 │             │  targetRef=01-23  │
 │             │                  │
 │             │─加载数据          │
 │             │  (内部处理)        │
 │             │                  │
 │─onRangeChange│  (不调用，避免干扰)│
 │  (旧逻辑会调用)                  │
 │             │                  │
 │─customDateStart─>               │
 │  = 01-22     │                  │
 │             │                  │
 │             │                  │─检查: changing? │
 │             │                  │  检查: targetRef?│
 │             │                  │  检查: target≠parent?│
 │             │                  │  → 忽略！return   │
 │             │                  │                  │
 │             │                  │  ( currentDate  │
 │             │                  │    保持为 01-23 ) │
 │             │                  │                  │
 │             │─500ms 后         │                  │
 │             │  setChanging=false│                 │
 │             │  targetRef=null   │                  │
```

---

## 关键设计决策

### 1. 为什么不调用 `onRangeChange`？

**问题**：时区切换时调用 `onRangeChange` 会触发父组件重新计算日期，而父组件可能使用不同的时区逻辑，导致日期被错误覆盖。

**解决方案**：时区切换是组件**内部操作**，不需要通知父组件。只有用户**手动选择日期**时才需要同步父组件。

### 2. 为什么使用 `timezoneChangeTargetRef`？

**问题**：`onRangeChange` 可能有延迟，在 `isTimezoneChanging` 清除后才触发父组件的 `customDateStart` 变化。

**解决方案**：使用 `useRef` 存储**目标日期**，在父组件触发时检查是否匹配，不匹配则忽略。

### 3. 为什么使用请求序列号？

**问题**：用户快速操作时，旧请求的响应可能晚于新请求到达，导致数据被错误覆盖。

**解决方案**：每次请求递增序列号，响应时检查是否是最新的请求。

### 4. 为什么延迟 500ms 清除标志？

**问题**：React 状态更新是批量的，`useEffect` 可能在不同时机触发。父组件的响应时间不确定。

**解决方案**：延迟足够长的时间（500ms），确保所有相关的 `useEffect` 都已执行完毕。

---

## 依赖关系

```typescript
// useEffect 依赖链
useEffect(() => {
  if (isTimezoneChanging) return;  // 时区切换时跳过
  loadData();
}, [drillPath, activeDims, timezone, currentDate, isTimezoneChanging]);

// customDateStart 变化时更新 currentDate
useEffect(() => {
  if (customDateStart && !shouldIgnore) {
    setCurrentDate(customDateStart.toISOString().split('T')[0]);
  }
}, [customDateStart, isTimezoneChanging]);

// currentDate 变化触发上面的 useEffect
// 从而触发 loadData()
```

---

## 日期计算逻辑

```typescript
// 获取指定时区的"今天"日期
const getDateInTimezone = (tz: string): string => {
  const tzOffsetMap: Record<string, number> = {
    'UTC': 0,
    'Asia/Shanghai': 8,   // UTC+8
    'EST': -5,            // UTC-5
    'PST': -8,            // UTC-8
  };

  const now = new Date();
  const offsetHours = tzOffsetMap[tz] || 0;

  // 1. 获取 UTC 时间戳（减去本地时区偏移）
  const utcTimestamp = now.getTime() - (now.getTimezoneOffset() * 60000);

  // 2. 加上目标时区偏移
  const tzTimestamp = utcTimestamp + (offsetHours * 3600000);

  // 3. 返回目标时区的日期字符串
  return new Date(tzTimestamp).toISOString().split('T')[0];
};
```

**示例**：
- 当前 UTC 时间：`2026-01-23 15:00:00`
- PST (UTC-8) 的"今天"：`2026-01-23`（15 - 8 = 7，仍在23号）
- 如果 UTC 时间是 `2026-01-23 02:00:00`
- PST (UTC-8) 的"今天"：`2026-01-22`（2 - 8 = -6，跨到22号）

---

## 总结

这个设计的核心思想是：

1. **内部状态自主管理**：`currentDate` 由组件内部管理，不依赖父组件
2. **单向数据流**：时区切换是内部操作，不向上通知；手动选择日期才向上同步
3. **防御性编程**：多层防护（序列号、标志位、Ref）防止各种竞态条件
4. **用户体验优先**：快速操作时只显示最新结果，避免闪烁或错误数据
