# module1-e2e

模块① 数据管理 · 集成与 E2E + Layer API（潜店雷达 WP09，收官包）。

## 五服务串联

```
intake → [门1 空间: geocode-arbiter R018/R019/R031]
       → [门2 身份: identity-service 未决重复即挂起]
       → [门3 规则: service-rules 合同相位/时间窗]
       → assignment-service 建主访
       → dq-workbench 全部拦截落工单+审计链
```

## 验收不变量（E2E 测试锁定）

- **G1 未过门禁的数据不可进入规划**：`Pipeline.planning_eligible()` 只返回
  空间+身份+规则三门全过且已建主访的店；被拦截门店无主访关系、无规划入口
- **G2 每次拦截可追溯**：空间/身份/规则/分配的每次 hold 都在 dq-workbench
  留 finding + 审计事件
- **G3 规划读取是只读快照**：`planning_eligible(states)` 纯函数

## 依赖（全部按 tag 锁定）

coords-core v0.1.0 · geocode-arbiter v0.1.0 · identity-service v0.1.0 ·
assignment-service v0.1.0 · service-rules v0.1.0 · dq-workbench v0.1.0
