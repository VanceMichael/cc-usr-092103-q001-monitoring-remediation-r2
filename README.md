# 监测机构整改取证协同

生态环境部门整改取证协同后台：以哈希链事件存储为底座，把机构资质、
人员授权、仪器校准、阶段期限与处置权限纳入**时间化证据链**。执法人员
沿着一条采样记录即可看到当时有效的资质、授权、仪器状态、原始数据、
整改决定与真实的跨部门移送结果，无需在市场监管、公检法与监测机构
保存的不同副本之间猜测哪份材料曾被采用。

## 领域资料

仓库中的 `contracts/context.schema.json` 描述基础资料格式，
`fixtures/context.json` 给出可公开使用的示例（全部为虚构数据）。
代码库读取并校验这些资料，业务服务沿用相同标识和版本约定。

当前资料反映的事实包括：

- 自查分阶段推进（自查报告 → 整改落实 → 复核，各阶段有法定期限）
- 仪器校准与采样记录需要留痕
- 机构资质、人员授权与仪器校准均按时间窗认定
- 更正保留原事实，补录必须引用旧记录并说明原因
- 错报漏报、敷衍整改、复核不通过、涉嫌犯罪移送按权限矩阵推进
- 涉嫌犯罪线索需要跨部门移送（双审批 + 移送回执 + 受理结果）

## 架构

```
contracts/context.schema.json   领域资料契约（JSON Schema）
fixtures/context.json           虚构示例资料（机构/人员/仪器/期限/权限）
src/context.py                  资料读取与校验
src/evidence/
  canon.py                      规范 JSON + SHA-256（稳定摘要的字节级基础）
  timeutil.py                   UTC 时间处理（occurred_at / recorded_at 分离）
  store.py                      SQLite 追加式哈希链事件存储 + 幂等回执
  reference.py                  时间化参考视图（as-of / knowledge-at 查询）
  domain.py                     领域错误 + 案件状态纯函数折叠
  service.py                    业务命令与查询（全流程）
  routes.py / api.py            标准库 HTTP API
tests/                          46 项端到端测试
scripts/replay_demo.py          校准时间矛盾完整重放演示
```

### 证据链

- 事件**只增不改**；每个事件的哈希覆盖前一事件哈希，任一历史字节
  被改动都会使 `verify-chain` 失败（测试含篡改检测）。
- 每个事件同时记录业务发生时刻 `occurred_at` 与系统记录时刻
  `recorded_at`——迟到补登的校准证书因此无法"改写过去"。
- 每次受理都返回**可核验回执**（`GET /receipts/{id}/verify`）；
  写接口支持 `Idempotency-Key`，**重复上报**只留一条事件、
  返回同一张回执。

### 时间化参考查询

`GET /reference/...?at=...&knowledge_at=...` 回答两类问题：

- `at`：业务时刻——"采样那一刻仪器是否在校准有效期内"；
- `knowledge_at`：系统认知时刻——"以当时系统掌握的资料看是否有效"。

资格依法退出（`POST /institutions/{id}/qualification-exit`）按
`effective_at` 截断时间窗：退出前的历史认定不变，退出后的采样
自动标记，在办案件同步留痕。

### 案件流程与四类处置

自查 → 整改 → 复核，期限来自领域资料；逾期由
`POST /admin/check-overdue` 幂等扫描留痕并给出升级建议。

- **补录**：`occurred_at` 早于阶段开始（或显式声明）时必须说明原因
  并引用至少一条已存在的旧记录，引用不存在则拒绝；
- **更正**：新事件引用原事件，原事实完整保留，重放任一时刻各见其所；
- **复核**：`expected_round` 乐观并发，**多人同时复核**先到者生效，
  后到者收到 409 与当前轮次（有线程级测试覆盖）；
- **错报漏报 / 敷衍整改**：按权限矩阵单事件处置；
- **复核不通过**：复核员退回，案件回到整改阶段、轮次 +1、期限重算；
- **涉嫌犯罪移送**：`propose → approve（法制审核员与部门负责人
  分别审批、审批人互不相同、提案人不得兼任）→ execute（登记移送
  机关、移送文号、接收回执）→ outcome（受理结果，退回则案件恢复
  在办）`——移送结果是链上事实，不是猜测。

### 稳定摘要与完整重放

- `GET /cases/{id}/summary`：同一事件序列必得同一字节与同一
  `digest`（SHA-256），并锚定链头哈希；
- `GET /cases/{id}/replay?at=...`：以任意时刻之前的系统记录重建
  案件状态。监管、司法、机构三方各自重放同一事件序列，得到同一
  状态与同一摘要哈希——一次看似普通的校准时间矛盾由此可被完整
  重放（见 `scripts/replay_demo.py`）。

## 运行

```bash
pip install -r requirements.txt
python -m pytest                              # 46 项测试
python scripts/replay_demo.py                 # 校准时间矛盾重放演示
python -m src.evidence.api --context fixtures/context.json \
    --db evidence.db --port 8080              # 启动后台服务
```

主要接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sampling-records` | 采样登记，即时校验资质/授权/校准并留痕 |
| POST | `/cases` | 依据采样记录立案 |
| POST | `/cases/{id}/self-checks` | 自查/整改材料提交（含补录） |
| POST | `/cases/{id}/corrections` | 更正（保留原事实） |
| POST | `/cases/{id}/reviews` | 复核（乐观并发） |
| POST | `/cases/{id}/dispositions` | 四类处置与移送全流程 |
| POST | `/instruments/{id}/calibrations` | 校准记录登记（触发矛盾检测） |
| POST | `/institutions/{id}/qualification-exit` | 资格依法退出 |
| GET | `/cases/{id}/timeline` `/summary` `/replay` | 时间线 / 稳定摘要 / 重放 |
| GET | `/cases/{id}/verify-chain`、`/receipts/{id}/verify` | 链与回执核验 |
| GET | `/reference/...` | 时间化参考查询 |
| POST | `/admin/check-overdue` | 逾期扫描与升级建议 |

## 本地校验

运行项目自带测试即可确认样例资料可读取、领域标识与版本字段完整、
fixture 通过 JSON Schema 校验。所有示例均为虚构数据，不含真实个人
信息、账号或访问凭据。

## 部署说明

单进程内命令级锁 + SQLite 事务已保证并发一致性；如需多进程部署，
应将 `expected_round` 等乐观并发约束下沉为数据库唯一约束。
