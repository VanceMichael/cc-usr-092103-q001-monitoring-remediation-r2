# 监测机构整改取证协同后台

面向生态环境部门的整改取证协同后台：把机构资质、操作人员授权、仪器校准、
阶段期限与处置权限全部纳入**时间化证据链**，使执法人员沿一条采样记录追查时，
能直接看到**当时有效并被实际采用**的材料，而不必在市场监管、公检法和监测
机构保存的不同副本之间猜测哪份曾被采用。

所有事实只追加、不修改、不删除；任何更正都保留原事实；重复上报、多人同时
复核、整改逾期、资格依法退出等场景都产生**稳定摘要**与**可核验回执**。

## 一分钟体验

```bash
pip install -r requirements.txt

# 端到端剧本：一次看似普通的校准时间矛盾如何被完整重放（固定时钟，全程确定）
python -m src.evidence.cli demo

# 落盘 + 重启后重放 + 全链校验
python -m src.evidence.cli demo --store data/events.jsonl --out replay.json
python -m src.evidence.cli verify --store data/events.jsonl
python -m src.evidence.cli replay CASE-2026-W031 --store data/events.jsonl

# HTTP JSON API
python -m src.evidence.cli serve --port 8080

# 测试（49 项）
python -m pytest tests/ -q
```

全部示例（机构、人员、仪器、案号）均为虚构，不含真实个人信息或凭据。

## 核心设计

### 1. 双轨哈希链（`src/evidence/store.py`）

每条事件携带载荷哈希、前驱哈希、入链时间，SHA-256 串联：

- **全局链**：系统内全部事件（含基准资料）按入链顺序串联，跨案件增删改即可发现；
- **案件链**：同一案件的事件独立串联，执法人员只核验该案件链即可重放全案。

存储为只追加 JSONL；重启时逐条重算哈希自检。规范化编码（`hashing.py`）对
null/布尔/数值/字符串打类型标签，摘要与对象构造顺序无关、跨语言一致。
跨部门移送随案导出 `evidence-case-bundle/1` 材料包，接收方**不依赖本库**
即可独立验链（`EventStore.verify_bundle`）。

### 2. 时间化基准资料（`src/evidence/registry.py`）

机构资质、岗位授权、仪器校准、阶段期限、处置规则均为带
`valid_from/valid_to/recorded_at` 的时间线段，支持任意时点 as-of 投影：

- 登记采样时校验的是**采样时刻**有效的资质、授权与校准；
- 投影带 `known_at` 认知时点：**事后补录、追溯生效的证书不会穿越回历史**，
  系统在 3 月 20 日登记时只能锚定当时已收录的 CAL-2025-1102；
- 采样事件以事件哈希锚定"当时实际采用"的三件套。重放时若当前投影已被后补
  证书改变（CAL-2026-0315X），重放视图直接标出 `contradiction`；
- 自动检测校准矛盾：证书有效期重叠 `overlap`、超 7 日宽限期的追溯补录
  `backdated`、采样时刻无有效校准 `expired_at_sample`；
- 资质撤销（依法退出）、授权吊销、仪器停用都是新时间线段，历史不被改写。

### 3. 处置流转（`src/evidence/service.py`）

| 场景 | 规则 |
|---|---|
| 自查补录 | 只能 `ref_event_hash` 引用既有记录并写明 `reason`，不改被引用记录 |
| 事实更正 | 追加 `fact.corrected`，`before_value`/`after_value` 永久保留，原事件不动 |
| 重复上报 | 拒绝入链；对同一（案件，阶段）永远签发**同一张**稳定拒绝回执 |
| 命令重试 | 所有写命令带 `command_id` 幂等键，重放首次事件组与回执 |
| 错报/漏报 | 依处置权限矩阵认定，规则中的法定动作与权限随事件入链 |
| 敷衍整改 | 每项要求必须有措施且附证据材料；缺项即标记退回，事件照常入链、回照常发 |
| 整改逾期 | 提交时判定 + `sweep_overdue` 巡检幂等登记，记录逾期秒数，不阻断整改 |
| 多人复核 | 提交人回避；复核人携带案件链头做乐观并发（OCC），仅一人成功，后来者得 412 |
| 涉嫌犯罪移送 | 须有错报/更正事实支撑，同一时刻仅允许一单在途；随案移送可独立核验材料包 |
| 回流结果 | 仅公检法回流岗可登记受理/立案/退查与案号，重复登记被拒 |
| 资格退出 | 撤销后的新采样一律拒绝；撤销前的历史采样仍按当时有效资质重放 |

命令在服务层按案件串行化（检查状态与入链之间不被穿插），存储层再以
OCC + 幂等索引兜底。

### 4. 可核验回执（`src/evidence/receipts.py`）

受理与拒绝都签发 HMAC-SHA256 回执：受理回执锚定首事件与链头；拒绝回执
（重复上报等）对同一被拒事实回执号稳定。回执可编码为单行 token，
经 `/api/receipts/verify` 验签。链证明内容未被篡改，回执证明受理时间
与结论，二者各司其职。

### 5. 执法重放视图（`src/evidence/replay.py`）

`GET /api/cases/{id}/replay` 或 CLI `replay` 输出：当时有效的资质/授权/校准
快照（含矛盾标记）、原始事实清单（被更正字段标注更正事件、原值仍在）、
补录引用、整改决定与逾期/复核结论、移送与真实回流结果、全链路哈希，
以及对同一份历史恒定不变的视图摘要 `digest`。

## HTTP API 摘要

请求头 `X-Actor` / `X-Role` 标识操作人与角色；写接口必须带 `command_id`。

- `POST /api/samples` 采样登记（按采样时刻校验资质/授权/校准）
- `POST /api/cases/{id}/self-check`｜`/late-entries`｜`/corrections`
- `POST /api/cases/{id}/faults` 错报漏报认定
- `POST /api/cases/{id}/orders` 下达整改（期限取自基准资料）
- `POST /api/cases/{id}/orders/{oid}/submissions` 整改提交
- `POST /api/cases/{id}/orders/{oid}/verify` 复核（可带 `expected_case_hash`）
- `POST /api/cases/{id}/transfers`、`/transfers/{tid}/result` 移送与回流
- `POST /api/cases/{id}/sweep-overdue`、`/close`
- `POST /api/registry/import`、`/api/registry/withdraw-qualification`
- `GET /api/registry/summary`、`/api/cases/{id}`、`/replay`、`/bundle`
- `POST /api/receipts/verify`

角色：`registry_admin`、`monitor_staff`、`monitor_lead`、`eco_inspector`、
`eco_reviewer`、`transfer_officer`、`judicial_desk`、`auditor`，
权限矩阵见 `src/evidence/access.py`。

## 剧本：校准时间矛盾（`src/evidence/scenario.py`）

固定时钟完整重放：2026-03-20 绿源公司采样（锚定 CAL-2025-1102）→
重复上报拿到稳定回执 → 合规补录（引用+原因）→ 表述更正（原值保留）→
4 月 2 日机构补交声称 3 月 1 日生效的 CAL-2026-0315X（重叠+追溯补录被标记）
→ 认定错报 → 30 日整改决定 → 敷衍整改退回 → 逾期后重新校准并完整整改 →
两名复核人同时提交仅一人生效 → 涉嫌犯罪移送并附可独立验签材料包 →
公安回流"已立案"（虚构案号）→ 案件闭环。任意时间重放，链头与摘要不变。

## 目录

```
contracts/context.schema.json   原领域契约（保留）
fixtures/context.json            原领域样例（保留）
fixtures/registry.json           时间化基准资料（机构/人员/仪器/期限/规则）
src/context.py                   原资料读取器（保留）
src/evidence/
  clock.py        固定/实时时钟（业务时间确定、可重放）
  hashing.py      规范化编码与稳定摘要
  store.py        双轨哈希链、幂等、OCC、JSONL、移送材料包
  receipts.py     HMAC 回执签发/验签/编解码
  access.py       角色与处置权限矩阵
  registry.py     时间化基准资料投影与校准矛盾检测
  service.py      全部业务命令
  replay.py       执法追查重放视图
  api.py          HTTP JSON API（标准库）
  cli.py          demo / serve / replay / verify
  scenario.py     端到端可复现剧本
tests/             49 项测试：哈希链、时间投影、处置流转、并发、持久化、HTTP、剧本
```

生产化时应替换：HMAC 密钥改为 KMS 托管、回执可增加 CRL/时间戳服务、
JSONL 存储换为支持只追加语义的持久层（并保留双轨链校验）、HTTP 层加入
认证网关与审计日志。
