# 基于 tRPC-Agent-Go 的多租户节点化 Agent 平台技术方案

## 1. 需求背景与目标
- 业务背景: 企业希望把 Agent 能力从单点 demo 扩展成平台化服务，同时满足租户隔离、弹性部署、数据一致性、IM 触达、审计合规和后端可替换等要求。  
它的价值在于把框架能力真正映射到企业级 Agent 平台架构，而不是只停留在单个 Agent 进程。
- 目标：
  - 多租户：支持 ≥50 个租户独立部署 Agent，租户间配置、会话、记忆、知识库、工具权限、审计日志完全隔离
  - 节点化部署：Agent Worker 支持水平扩展至 ≥10 节点，无状态化，依赖共享 Session 后端，消息路由到正确租户与 session 的正确率 100%
  - 多后端：支持 ≥3 类存储后端（Redis / SQL / 向量库 / 对象存储），租户级可配、可迁移，迁移期间服务不中断
  - IM 接入：接入企业微信 + 微信客服（或公众号）≥2 类通道，消息端到端（回调到回复落 IM）P95 延迟 ≤15s（含 LLM 生成），消息幂等无重复回复
  - 可观测：单条 trace 串联 IM 回调 → Runner → Tool → Session/Memory 读写 → IM 回复，覆盖率 100%
  - 审计合规：审计日志字段完整（tenant_id / channel / user_id / session_id / agent_name / tool_name / decision / latency / error_type / cost / trace_id），密钥不明文出现在任何日志和 trace 中

## 2. 设计思路  
- 最大化复用 tRPC-Agent-Go 的单体能力：Runner、Session/Memory/Knowledge/Artifact 接口、Plugin/Guardrail、Telemetry Hooks、OpenClaw Channel 模型都已存在，  
  平台层只做"多租户化 + 分布式化 + 治理化"三件事，不重写框架内核。
- 思路推导  
  现状：tRPC-Agent-Go 能跑起一个功能完整的单体 Agent  
  差距：单体进程无法回答四个问题——①多个租户的配置和数据怎么隔离 ②多实例部署时消息路由到哪台机器 ③不同租户选了不同存储后端怎么统一访问 ④出问题时怎么审计和追责  
  路径：针对每个差距引入一个平台层机制——租户模型与路由、无状态 Worker + 共享 Session、Storage Adapter 抽象、治理层（Guardrail + Audit + Telemetry）。每个机制都优先挂在框架已有扩展点上，挂不上的才自建。  
- 关键决策与权衡：
  - 决策一：消息链路采用「同步应答 + 异步消费」，而不是 Gateway 同步直调 Runner
    - 背景：企微/微信回调要求 5s 内应答，而 LLM 生成 P95 远超 5s。
    - 选择：Gateway 验签去重后立即返回 success，消息写入 Redis Stream，由无状态 Worker 消费后通过主动发消息接口回复。
    - 代价与对策：引入了 at-least-once 投递带来的重复消费问题——用 `dedup:{channel}:{msg_id}` 幂等键 + session 分布式锁兜底；
      链路变长，用全链路 trace_id 保证可观测性可穿透异步边界。
  - 决策二：Session 采用「事件追加 + 状态快照」双层存储，而不是只存最新 state
    - 背景：多 Worker 无状态化后，任何节点都可能接管任意会话；崩溃恢复和审计都要求能还原历史。
    - 选择：`session_event` 只追加写（`(session_id, event_seq)` 唯一保证有序不重复），`session.state` 作为快照加速读取；
      崩溃后以 event 为准重放重建 state，对应 tRPC-Agent-Go Session 接口的 AppendEvent/重放语义。
    - 代价与对策：存储量约翻倍、写放大——用 `summary` 表做事件压缩，旧事件可按租户 audit_policy 归档清理。
  - 决策三：密钥不落库，仅存引用（`token_ref` / `aeskey_ref`），运行时从 KMS 解析
    - 背景：审计合规要求密钥不出现在任何日志、trace 和数据库中；多租户密钥需要独立轮换。
    - 选择：数据库只存引用字符串，Channel Adapter 在验签/加解密时通过 Secret Resolver 向 KMS 取值，进程内缓存且禁止打日志。
    - 代价与对策：引入 KMS 可用性依赖——本地开发用文件型 Resolver 兜底，线上用短 TTL 缓存 + 轮换期间双引用并存过渡。

## 3. 总体架构  
- 架构图
```mermaid
flowchart TD
    subgraph IM层
        WX[微信]
        QW[企微]
    end

    subgraph 平台接入层
        GW[Agent Gateway<br/>鉴权/租户路由/限流]
        CA[Channel Adapter<br/>企微适配器 / 微信适配器]
        INBOX[(Inbound Event<br/>消息去重/幂等)]
        OUTBOX[(Outbound Event<br/>异步回复/失败重试)]
    end

    subgraph Agent执行层
        W1[Agent Worker 1<br/>runner.Runner]
        W2[Agent Worker 2<br/>runner.Runner]
        GR[Guardrail 链<br/>白名单/脱敏/预算/审批]
        SA[Storage Adapter<br/>Session/Memory/Knowledge/Artifact]
    end

    subgraph 平台治理层
        ADMIN[Admin API<br/>租户/配置/发布管理]
        TEL[Telemetry Collector<br/>OTel trace + metrics]
        AUDIT[Audit Service<br/>审计日志]
        KMS[KMS/Secret Manager<br/>密钥管理]
    end

    subgraph 数据层
        REDIS[(Session 热数据 / 幂等 / 锁 / 队列)]
        SQL[(结构化数据：租户/绑定/审计/Session 快照)]
        VEC[(Knowledge 向量检索)]
        OSS[(Artifact 文件存储)]
    end

    QW --> GW
    WX --> GW
    GW --> CA
    CA --> INBOX
    INBOX --> W1
    INBOX --> W2
    W1 --> GR
    W2 --> GR
    GR --> SA
    SA --> REDIS
    SA --> SQL
    SA --> VEC
    SA --> OSS
    W1 --> OUTBOX
    W2 --> OUTBOX
    OUTBOX --> CA
    CA --> QW
    CA --> WX
    ADMIN --> SQL
    ADMIN --> KMS
    W1 --> TEL
    W2 --> TEL
    GR --> AUDIT
```

注：数据层画的是平台支持的后端**角色全集**，不是每个租户的全量部署。架构角色固定，产品为当前推荐实现、可替换；
Memory 不单独部署，随后端选择复用 Redis/PG/pgvector。默认所有租户走推荐栈，
租户级差异由 Storage Adapter 按 `tenant.storage_config` 在内部路由，不改变部署拓扑。

| 架构角色 | 推荐实现 | 可替换实现 |
|---|---|---|
| Session 热数据 / 幂等 / 锁 / 队列 | Redis | PG |
| 结构化数据 | PostgreSQL | MySQL |
| Knowledge 向量检索 | pgvector | Qdrant / Milvus |
| Artifact 文件存储 | S3 兼容（MinIO / 云 OSS） | — |

注：Milvus 仅接口预留，不在 5.1.1 的受控菜单内。

- 时序图
```mermaid
sequenceDiagram
    autonumber
    participant U as IM用户
    participant IM as IM平台
    participant GW as Agent Gateway
    participant IB as Inbound<br/>(Redis Stream)
    participant W as Agent Worker<br/>(Runner)
    participant G as Guardrail
    participant S as Storage<br/>(Redis/PG)
    participant L as LLM API
    participant OB as Outbound<br/>(Redis Stream)

    U->>IM: 发送消息
    IM->>GW: webhook 回调(加密XML)
    GW->>GW: 验签 + 解密 + 租户路由
    GW->>IB: SETNX dedup:{msg_id} 去重
    alt 重复消息
        IB-->>GW: 已存在，丢弃
        GW-->>IM: 返回 success
    else 新消息
        IB->>IB: 入队
        GW-->>IM: 立即返回 success(避免5s超时)
    end

    IB->>W: Worker 消费
    W->>G: 输入检查(白名单/脱敏)
    G-->>W: pass / block
    W->>S: 加锁并加载 session + memory
    W->>L: Runner.Run(流式)
    L-->>W: 流式生成 / 工具调用请求
    opt 需要调工具
        W->>W: 执行 Tool/MCP
        W->>L: 携带工具结果继续生成
    end
    W->>G: 输出检查(预算/敏感词)
    W->>S: 追加 event → 更新 state → 释放锁
    W->>OB: 回复消息入队
    OB->>IM: message/send 主动发送
    IM->>U: 收到回复

    Note over W,S: 崩溃恢复：以 event 为准重放 state
    Note over GW,OB: 全链路 trace 串联：Stream 消息体携带 W3C traceparent，Worker 消费时 Extract 续链(OTel)
```
- 业务系统边界：
  - 上游：企业微信、微信客服两类 IM 平台——平台只消费其 webhook 回调（加密 XML），通过其「应用消息/客服消息」主动发送接口回包，不改造 IM 侧任何配置之外的系统。
  - 下游一（LLM）：模型推理服务（OpenAI 兼容接口或自研网关），平台通过 tRPC-Agent-Go 的 Model 层调用，不感知具体模型部署。
  - 下游二（基础设施）：PostgreSQL、Redis、向量库、对象存储、KMS——全部为已有中间件，平台只做接入与读写，不涉及改造。
  - 平级（Admin 前端）：管理控制台通过 Admin API 读写租户/应用/绑定配置，本项目只提供 HTTP API，不含前端实现。
  - 改造点：无对存量业务系统的改造；新增的是 Agent Gateway、Worker、Admin API、Audit Service 四个服务进程，以及 5.1.2 列出的 PG 表与 5.1.4 的 Redis key。

- 模块划分与各自职责：

  | 模块（代码目录） | 职责 | 改动类型 |
  |---|---|---|
  | `trpcservice/channels` | Channel Adapter：验签/加解密、消息编解码、调 IM 主动发送接口；企微（`channels/wecom`）先行，微信客服同接口扩展 | 新建，挂在框架 OpenClaw Channel 扩展点 |
  | `trpcservice/tenant` | 租户模型与路由：租户/应用/绑定的加载与缓存、tenant_id 全链路透传、租户级配置解析 | 新建，平台层核心 |
  | `trpcservice/agent` | Agent 定义与 Runner 装配：按 `agent_app` 配置组装 Agent（prompt/模型/工具），调用 runner.Runner | 基于框架扩展点实现 |
  | `trpcservice/tool` | 平台内置工具注册与租户级工具权限过滤 | 基于框架 Tool 扩展点实现 |
  | `trpcservice/skill` | 租户级 Skill（可复用能力包）的加载与挂载 | 基于框架扩展点实现 |
  | `trpcservice/config` | 服务自身配置（端口、DB、Redis、KMS 地址）与 Secret Resolver 抽象 | 新建 |
  | `trpcservice/web` | Admin API + Gateway HTTP 入口：租户/应用/绑定 CRUD、IM 回调接收 | 新建 |
  | `trpcservice/metrics` | OTel trace/metrics 初始化与通用埋点（复用框架 Telemetry Hooks） | 基于框架扩展点实现 |
  | `trpcservice/log` | 结构化日志，密钥脱敏中间件 | 新建 |
  | `trpcservice/workspace` | 运行时工作目录与临时文件管理（Artifact 落地中转） | 新建 |
  | `cmd/trpc-service` | 进程入口：按启动参数以 gateway / worker / admin / all-in-one 角色启动 | 新建 |
  | `trpcservice/storage` | Storage Adapter：Session/Memory/Knowledge/Artifact 四类框架接口的多后端实现（Redis/PG），含审计日志 PG 写入；租户级可配 | 新建，实现框架已有接口 |
  | Guardrail 链 | 输入白名单/脱敏、输出预算/敏感词，产出审计事件 | 基于框架 Plugin/Guardrail 扩展点实现 |

## 4. 重点技术与选型

### 4.1 关键技术清单

| 领域 | 选型 | 用途 |
|---|---|---|
| Agent 框架 | tRPC-Agent-Go | Agent 编排、Runner 执行、Session/Memory/Knowledge/Artifact 接口、Plugin/Guardrail、Telemetry Hooks、OpenClaw Channel |
| 消息队列 | Redis Stream | Inbound/Outbound 异步消息通道（决策一） |
| 关系存储 | PostgreSQL | 租户/应用/绑定/会话/事件/审计等结构化数据 |
| 缓存与协调 | Redis | Session 热数据、消息幂等、会话分布式锁 |
| 向量库 | pgvector（起步） | Knowledge 向量检索；接口预留 Qdrant/Milvus |
| 对象存储 | S3 兼容（MinIO/云 OSS） | Artifact 文件存储 |
| 密钥管理 | KMS + Secret Resolver | IM token、模型 API key 等密钥的引用与解析（决策三） |
| 可观测 | OpenTelemetry | 全链路 trace + metrics |
| 部署 | Docker Compose（最小）/ Kubernetes（生产） | 见 5.2.5 |

### 4.2 选型依据与备选对比

- **消息队列：Redis Stream（而非 Kafka / RabbitMQ）**
  - 量级估算：50 租户、总入站峰值按 1,000 msg/s 设计（见 5.2.1 假设），单 Redis 实例的 Stream 吞吐（10w+ msg/s）有两个数量级余量。
  - Redis 因 Session 热数据、幂等键、分布式锁本就是必选组件，用 Stream 不再引入新中间件，运维成本和故障面最小。
  - Consumer Group 原生支持 at-least-once 投递与 pending 消息重投，正好配合幂等键实现不丢不重。
  - 代价：持久化弱于 Kafka（依赖 AOF）、堆积容量受内存限制。对策：`XADD MAXLEN` 限制队列长度、pending 积压告警、AOF everysec；量级超出一个数量级后再评估迁 Kafka（Producer/Consumer 封装在 `channels` 与 Worker 内部，替换不影响业务代码）。
- **关系存储：PostgreSQL（而非 MySQL）**
  - 租户/应用的动态配置（`model_config`、`tool_policy` 等）用 JSONB 承载，避免每次加配置项都 DDL。
  - pgvector 扩展可兼任起步期向量库，少维护一个组件；tRPC-Agent-Go 已有 `session/postgres` 实现可直接复用。
  - 代价：团队若只有 MySQL 运维经验需要适应；JSONB 字段不适合做高频条件查询（配置类字段无此需求）。
- **向量库：pgvector 起步，接口层预留替换（而非直接上 Milvus/Qdrant）**
  - 起步期单租户知识库向量量在十万级以内，pgvector（HNSW 索引）延迟与召回均够用，且与业务数据同库，备份、事务、权限管理统一。
  - 专用向量库（Milvus/Qdrant）在百万级以上规模才有明显优势，但要多维护一套集群。
  - 对策：Knowledge 访问走框架 `knowledge` 接口 + 平台路由层，`memory_item.embedding_id` 已按「向量 ID 外部化」设计，迁库时双写过渡、按租户灰度。
- **密钥管理：KMS 引用模式（而非落库加密 / 硬编码配置）**
  - 理由与权衡见第 2 节决策三。补充一点：Secret Resolver 做成接口，本地开发用文件实现、测试用内存实现、生产接云 KMS 或 Vault，三类环境同一套代码路径。
- **可观测：OpenTelemetry（而非自研埋点）**
  - 框架已内置 OTel Hooks，Runner/Tool/Model 调用自动产生 span；平台层只需补 Gateway、队列收发、Guardrail 三处 span 即可实现全链路串联，自研埋点没有收益。
  - 备选 Jaeger/Tempo 仅涉及 Collector 后端选择，与代码无关。

### 4.3 框架能力映射（①原生支持 ②扩展点实现 ③自建）

| 平台需求 | 支持程度 | 复用的框架能力 | 平台层要做的事 | 落点 |
|---|---|---|---|---|
| Agent 编排 | ① | `agent/llmagent`、`agent/graph`、Chain/Parallel/Cycle | 租户级 Agent 注册、发布、路由 | `agent` |
| 执行入口 | ① | `runner.Runner`（流式 Event、context 取消） | 无状态 Worker 调度、队列消费、并发控制 | `cmd` + `agent` |
| Session/Memory/Knowledge/Artifact | ② | `session`/`memory`/`knowledge`/`artifact` 接口及多后端实现 | 租户级后端选择与路由、数据隔离、迁移 | `storage` |
| Tool / MCP / Skill | ② | `tool`、MCP Tool、`skill` | 租户工具白名单、密钥注入、危险工具审批 | `tool`、`skill` |
| 治理 Guardrail | ② | Plugin / Guardrail / Callbacks | 输入白名单与脱敏、输出预算与敏感词、审计事件产出 | `agent`（Guardrail 链） |
| 服务化 | ② | `server/openai`、`server/agui`、`server/a2a` | 统一 Gateway（鉴权/路由/限流）、Admin API | `web` |
| IM 接入 | ②+③ | OpenClaw Channel 模型（仅通道抽象） | 企微/微信客服适配器主体自建（验签/AES 加解密/5s 应答/主动发送），挂在 Channel 扩展点上；租户绑定 | `channels` |
| 可观测 | ① | OpenTelemetry tracing / metrics Hooks | Gateway、队列、Guardrail 三处补 span；租户维度成本统计 | `metrics` |
| 审计合规 | ③ | 无（框架不提供审计存储） | 审计 schema、异步写入、查询 API | `storage`（audit）+ `web` |
| 多租户模型 | ③ | 无（框架无租户概念） | 租户/应用/绑定模型、tenant_id 全链路透传 | `tenant` |

防踩坑提示：

- Runner 的事件通道必须在 context 取消后排空消费，否则 goroutine 泄漏（Worker 长进程下会累积）；故障恢复设计见 5.2。
- OpenClaw Channel 的验签/解密在框架内只做通道抽象，企微的 AES 加解密与「5s 内应答」约束需要 Adapter 自己实现，不要假设框架已覆盖。

## 5. 详细设计
### 5.1 存储设计
#### 5.1.1 存储选型

- 使用 PostgreSQL 存储租户、应用、会话、事件、记忆和审计等结构化数据。
- 动态配置类字段使用 JSONB 存储，便于后续扩展。
- 密钥不直接落库，仅保存密钥引用，例如 `token_ref`、`aeskey_ref`。
- Redis 用于存储热点会话、消息幂等标记和会话分布式锁。
- 事件类数据采用追加写方式，避免更新历史记录。

租户级后端策略（对应 `tenant.storage_config`）：

- 平台只维护一套默认推荐栈（Session=Redis、结构化数据=PG、Knowledge=pgvector、Artifact=S3），绝大多数租户 `storage_config` 为空，直接走默认。
- 可选项收敛为受控菜单：session 二选一（redis / pg）、knowledge 二选一（pgvector / qdrant）、artifact 固定 s3；每种组合都经过测试，不支持任意排列组合。
- 租户级覆盖仅服务例外场景（如合规要求数据不出域的大租户接专有实例），通过 Admin API 由平台运维设置，终端用户不可见；连接串等敏感信息同样只存引用（`dsn_ref`）。
- 更换后端必须走数据迁移流程（双写过渡、按租户灰度），不允许直接改配置生切。

#### 5.1.2 数据表总览

| 表名 | 说明 | 核心约束 |
|---|---|---|
| `tenant` | 租户信息及租户级配置 | `id` 主键 |
| `agent_app` | Agent 应用定义及版本配置 | `(tenant_id, name, version)` 唯一 |
| `channel_binding` | 渠道与 Agent 应用的绑定关系 | `webhook_path` 唯一 |
| `session` | 用户会话及其当前状态 | `(app_id, session_key)` 唯一 |
| `session_event` | 会话事件流水，仅追加写 | `(session_id, event_seq)` 唯一 |
| `memory_item` | 用户长期记忆 | `id` 主键 |
| `summary` | 会话摘要及压缩进度 | `session_id` 主键 |
| `audit_log` | 审计日志 | `id` 主键 |

#### 5.1.3 表结构

**租户表：`tenant`**

存储租户基础信息及租户级策略配置。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `uuid` | 是 | 租户 ID，主键 |
| `name` | `varchar(128)` | 是 | 租户名称 |
| `model_config` | `jsonb` | 否 | 模型配置，包括模型、温度、上下文长度等 |
| `tool_policy` | `jsonb` | 否 | 工具调用策略 |
| `audit_policy` | `jsonb` | 否 | 审计与数据保留策略 |
| `storage_config` | `jsonb` | 否 | 数据后端覆盖配置（受控菜单），如 `{"session":{"type":"redis","dsn_ref":"t42-redis"}}`，空表示走平台推荐栈 |
| `status` | `varchar(32)` | 是 | 状态：`active`、`disabled` 等 |
| `created_at` | `timestamptz` | 是 | 创建时间 |
| `updated_at` | `timestamptz` | 是 | 更新时间 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `tenant_pkey` | 主键 | `id` | 租户唯一标识 |
| `idx_tenant_status` | 普通索引 | `status` | 按租户状态查询 |

---

**Agent 应用表：`agent_app`**

存储一个租户下的 Agent 应用配置。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `uuid` | 是 | 应用 ID，主键 |
| `tenant_id` | `uuid` | 是 | 所属租户 ID |
| `name` | `varchar(128)` | 是 | 应用名称 |
| `agent_type` | `varchar(64)` | 是 | Agent 类型 |
| `config` | `jsonb` | 是 | 应用配置，包括提示词、模型和工具配置 |
| `version` | `int` | 是 | 配置版本 |
| `status` | `varchar(32)` | 是 | 状态：`draft`、`published`、`disabled` |
| `created_at` | `timestamptz` | 是 | 创建时间 |
| `updated_at` | `timestamptz` | 是 | 更新时间 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `agent_app_pkey` | 主键 | `id` | 应用唯一标识 |
| `uk_agent_app_version` | 唯一约束 | `(tenant_id, name, version)` | 同租户下同名称应用版本唯一 |
| `uk_agent_app_published` | 部分唯一索引 | `(tenant_id, name) WHERE status = 'published'` | 同租户同名应用最多一个已发布版本 |
| `idx_agent_app_tenant` | 普通索引 | `tenant_id` | 查询租户下应用列表 |

---

**渠道绑定表：`channel_binding`**

存储外部渠道与 Agent 应用之间的绑定关系。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `uuid` | 是 | 绑定 ID，主键 |
| `tenant_id` | `uuid` | 是 | 租户 ID |
| `channel` | `varchar(64)` | 是 | 渠道类型，如企业微信、飞书、钉钉 |
| `app_id` | `uuid` | 是 | 绑定的 Agent 应用 ID |
| `webhook_path` | `varchar(256)` | 是 | 回调路径 |
| `token_ref` | `varchar(256)` | 否 | Token 密钥引用，不保存明文 |
| `aeskey_ref` | `varchar(256)` | 否 | AES Key 密钥引用，不保存明文 |
| `config` | `jsonb` | 否 | 渠道专有配置，如企微的 `corp_id`/`agent_id`、微信客服的 `kf_account`；敏感字段仍只存引用 |
| `status` | `varchar(32)` | 是 | 绑定状态 |
| `created_at` | `timestamptz` | 是 | 创建时间 |
| `updated_at` | `timestamptz` | 是 | 更新时间 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `channel_binding_pkey` | 主键 | `id` | 绑定唯一标识 |
| `uk_channel_webhook` | 唯一约束 | `webhook_path` | 回调路径全局唯一 |
| `idx_channel_binding_app` | 普通索引 | `app_id` | 查询应用绑定的渠道 |
| `idx_channel_binding_tenant` | 普通索引 | `(tenant_id, channel)` | 查询租户下某类渠道 |

---

**会话表：`session`**

存储用户会话的当前状态，历史明细保存在 `session_event` 中。

`session_key` 生成规则（由 Channel Adapter 在消息归一化时生成）：

- 单聊：`dm:{channel}:{external_userid}`，同一用户在同一应用下跨天对话复用同一会话。
- 群聊：`group:{channel}:{chatid}`，机器人在群里的上下文按群共享（全群一段对话）；需要群内按人隔离的租户可配置为 `group:{channel}:{chatid}:{external_userid}`。
- 隔离边界：会话唯一性由 `(app_id, session_key)` 保证，而 app 隶属于租户，因此跨租户天然隔离——同一微信用户出现在两个租户的渠道里，会落在两个完全不同的 app 命名空间下；跨群隔离由 `chatid` 保证，同用户在不同群是不同的 session。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `uuid` | 是 | 会话 ID，主键 |
| `tenant_id` | `uuid` | 是 | 租户 ID |
| `app_id` | `uuid` | 是 | Agent 应用 ID |
| `session_key` | `varchar(256)` | 是 | 应用内会话唯一键 |
| `user_id` | `varchar(128)` | 是 | 用户 ID |
| `channel` | `varchar(64)` | 是 | 来源渠道 |
| `state` | `jsonb` | 是 | 当前会话状态 |
| `created_at` | `timestamptz` | 是 | 创建时间 |
| `updated_at` | `timestamptz` | 是 | 最近更新时间 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `session_pkey` | 主键 | `id` | 会话唯一标识 |
| `uk_session_app_key` | 唯一约束 | `(app_id, session_key)` | 应用内会话唯一 |
| `idx_session_user` | 普通索引 | `(tenant_id, user_id)` | 查询用户会话 |
| `idx_session_updated_at` | 普通索引 | `updated_at` | 查询最近活跃会话 |

---

**会话事件表：`session_event`**

采用追加写方式保存完整会话事件。旧数据由月度归档任务搬至 `session_event_archive` 同构归档表
（`INSERT ... SELECT` + 限流 `DELETE` 分批搬运，避免长事务锁表）；不做声明式分区——
PG 要求分区键包含在所有唯一约束中，与 `(session_id, event_seq)` 幂等约束冲突，保约束弃分区。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `uuid` | 是 | 事件 ID，主键 |
| `session_id` | `uuid` | 是 | 所属会话 ID |
| `event_seq` | `bigint` | 是 | 会话内事件序号 |
| `event` | `jsonb` | 是 | 事件内容 |
| `created_at` | `timestamptz` | 是 | 事件创建时间 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `session_event_pkey` | 主键 | `id` | 事件唯一标识 |
| `uk_session_event_seq` | 唯一约束 | `(session_id, event_seq)` | 保证事件有序且不重复，兼作按序读取索引 |

---

**用户记忆表：`memory_item`**

记忆分两级：`app_id` 有值为应用私有记忆，`NULL` 为租户级共享记忆；读取时先查当前应用的私有记忆，再合并共享记忆；写入默认记为应用私有。软删除支撑「用户要求遗忘」的合规场景。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `uuid` | 是 | 记忆 ID，主键 |
| `tenant_id` | `uuid` | 是 | 租户 ID |
| `app_id` | `uuid` | 否 | 所属应用 ID；NULL 表示租户级共享记忆 |
| `user_id` | `varchar(128)` | 是 | 用户 ID |
| `content` | `text` | 是 | 记忆内容 |
| `embedding_id` | `varchar(256)` | 否 | 向量库中的向量 ID |
| `created_at` | `timestamptz` | 是 | 创建时间 |
| `updated_at` | `timestamptz` | 是 | 更新时间 |
| `deleted_at` | `timestamptz` | 否 | 软删除时间，NULL 表示有效 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `memory_item_pkey` | 主键 | `id` | 记忆唯一标识 |
| `idx_memory_item_user` | 部分索引 | `(tenant_id, user_id, app_id) WHERE deleted_at IS NULL` | 按用户查询有效记忆（私有 + 共享两级） |

---

**会话摘要表：`summary`**

保存会话摘要以及摘要覆盖到的事件位置。

重放与摘要机制：

- 崩溃恢复时从 `covered_event_id` 之后开始增量重放——之前的事件已被浓缩进 `summary_text`，长会话不做全量重放。
- 摘要由框架 Summarizer 在会话事件数超阈值时异步生成，生成后推进 `covered_event_id` 游标。
- `session.state` 只存框架定义的会话状态（摘要引用、增量游标等元数据），不存全量消息，消息明细永远以 `session_event` 为准。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `session_id` | `uuid` | 是 | 会话 ID，主键 |
| `summary_text` | `text` | 是 | 会话摘要 |
| `covered_event_id` | `uuid` | 是 | 摘要覆盖到的最大事件 ID |
| `updated_at` | `timestamptz` | 是 | 摘要更新时间 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `summary_pkey` | 主键、外键 | `session_id` | 一个会话对应一条摘要 |
| `covered_event_id` | 逻辑关联（不建外键） | — | 指向已压缩的最大事件；流水表会被归档删除，硬外键会阻塞归档 |

---

**审计日志表：`audit_log`**

审计事件按级别分两路写入：常规 `allow` 事件走内存缓冲异步批量写（满 100 条或满 1 秒触发），不在请求关键路径上；`deny` / `review` / 危险工具调用等关键决策事件**同步写入**，宁可增加毫秒级延迟也不接受丢失（合规红线）。实时监控走 metrics 聚合，本表保留行级明细用于对账与审计查询。旧数据由月度归档任务搬至 `audit_log_archive` 同构归档表（同 `session_event`，弃声明式分区以保全唯一约束）。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `uuid` | 是 | 日志 ID，主键 |
| `tenant_id` | `uuid` | 是 | 租户 ID |
| `channel` | `varchar(64)` | 否 | 渠道 |
| `user_id` | `varchar(128)` | 否 | 用户 ID |
| `session_id` | `uuid` | 否 | 会话 ID |
| `agent_name` | `varchar(128)` | 否 | Agent 名称 |
| `tool_name` | `varchar(128)` | 否 | 工具名称 |
| `decision` | `varchar(32)` | 是 | 审计决策，如 `allow`、`deny`、`review` |
| `latency_ms` | `int` | 否 | 调用耗时，单位毫秒 |
| `error_type` | `varchar(64)` | 否 | 错误类型 |
| `cost` | `numeric(12,6)` | 否 | 调用成本 |
| `prompt_tokens` | `int` | 否 | 输入 token 数 |
| `completion_tokens` | `int` | 否 | 输出 token 数 |
| `trace_id` | `varchar(128)` | 否 | 链路追踪 ID |
| `created_at` | `timestamptz` | 是 | 创建时间 |

索引与约束：

| 名称 | 类型 | 字段 | 说明 |
|---|---|---|---|
| `audit_log_pkey` | 主键 | `id` | 日志唯一标识 |
| `idx_audit_tenant_time` | 普通索引 | `(tenant_id, created_at)` | 租户审计查询 |
| `idx_audit_session` | 普通索引 | `session_id` | 会话审计查询 |
| `idx_audit_trace` | 普通索引 | `trace_id` | 链路追踪查询 |

#### 5.1.4 Redis Key 设计

| Key | 类型 | 示例 | Value | TTL | 说明 |
|---|---|---|---|---:|---|
| `stream:inbound` | Stream | `stream:inbound` | 归一化消息 JSON | 队列 MAXLEN 10 万条截断 | Gateway→Worker 入站队列，消费组 `workers` |
| `stream:outbound` | Stream | `stream:outbound` | 回复消息 JSON | 队列 MAXLEN 10 万条截断 | Worker→Channel Adapter 出站队列，消费组 `senders` |
| `dedup:{channel}:{msg_id}` | String | `dedup:wecom:msg123` | `1` 或请求 ID | 24 小时 | 消息幂等 |
| `lock:sess:{session_id}` | String | `lock:sess:8b3c...` | 请求唯一标识 | 5–10 秒 | 防止并发修改同一会话 |
| `sent:{session_id}:{event_seq}` | String | `sent:8b3c...:1024` | IM 返回的消息 ID | 24 小时 | 出站回复幂等，防发送重试造成 IM 侧重复消息 |

Session 存储优先复用框架后端（`session/redis` / `session/postgres`），平台不自建会话缓存层。
框架后端须同时满足三点——`(session_id, event_seq)` 级幂等唯一约束、summary 覆盖游标语义、
`tenant_id` 维度与按月归档的 schema 可控性；不满足的点在 `storage/` 内自实现。无论复用还是
自研，语义统一为「事件追加 + 快照」：**事件序列是 source of truth，state 只是物化快照**；
PG 后端落 5.1.3 的 `session_event` / `session` 表（或框架等价表结构）。

分布式锁规则：

- 加锁：`SET lock:sess:{session_id} {worker_id}:{request_id} NX EX 10`，拿不到锁的请求
  短暂自旋重试，超时进入 Stream 重新排队而非报错。
- 续期：会话执行可能超过锁 TTL（长工具调用），Worker 后台 watchdog 每 TTL/3（约 3 秒）
  续期一次，执行结束停止续期；Worker 崩溃则锁随 TTL 自然过期，不会死锁。
- 释放：用 Lua 脚本「校验 value 是自己的再 DEL」，防止锁已过期、被他人持有时误删。
- 兜底：锁失效导致并发写的极端场景，由 `(session_id, event_seq)` 唯一约束保证事件不重复，
  state 以事件重放为准修复。

幂等写入逻辑：

```text
SET dedup:{channel}:{msg_id} {request_id} NX EX 86400
返回 OK  → 首次到达，继续处理
返回 nil → 重复消息，直接丢弃
```

幂等分两层，职责不同：

- 入口层（`dedup:` key）：只挡 IM 平台的重推。企微/微信的重发通常几秒内到达，但应答失败后的重推间隔可达分钟级（最多 3 次），TTL 取 24h 覆盖全部重试窗口，与出站 `sent:` 对齐。
- 执行层（`(session_id, event_seq)` 唯一约束）：挡 Stream 重投。Worker 崩溃后 pending 消息
  被 XCLAIM 重投时不经过 Gateway，dedup 管不到；重复消费会产生相同 event_seq 的事件，
  被数据库唯一约束拒绝，事件层保证不重复。

出站幂等：出站消费组发送前先查 `sent:{session_id}:{event_seq}`，命中说明已发过，直接 XACK；
未命中则调 IM 发送接口，成功后写入该 key 再 XACK。「发送成功但 ACK 前崩溃」导致的重投
不会让用户收到重复回复。

队列与崩溃恢复：

- 入站：Gateway 验签去重后 `XADD stream:inbound`，Worker 以消费组 `workers` 阻塞读，
  处理完成后 `XACK`；Worker 崩溃时其 pending 消息由存活节点 `XCLAIM` 接管，保证不丢。
- 入队前 Gateway 按租户做令牌桶限流（quota 存 Redis，配置于 `tenant.tool_policy` 同级策略），
  超速直接拒收返回失败、由 IM 重推——防单租户灌量撑满全局队列、截断波及其他租户（noisy neighbor）。
- 出站：Worker 产出回复 `XADD stream:outbound`，发送方消费组 `senders` 调 IM 接口，
  发送成功才 `XACK`，失败保留 pending 重试（超过次数进死信并告警）。
- 两条 Stream 均 `XADD MAXLEN ~ 100000` 限制长度，防消息积压拖垮 Redis 内存；
  积压超过阈值触发告警（见 5.2）。

### 5.2 可靠性设计

#### 5.2.1 容量评估

基本假设：

| 假设项 | 取值 |
|---|---|
| 租户数 | 50 |
| 峰值入站消息 | 1,000 msg/s（均值按峰值 1/10 估算，100 msg/s） |
| 每条消息 LLM 调用 | 平均 2 轮（含 1 次工具调用） |
| 每条消息 token | 输入 ~2,000 + 输出 ~500 |
| 每条消息 Redis 操作 | ~10 次（dedup / 锁 / Stream 收发 / session 读写） |
| 每条消息 PG 写入 | ~5 次（event 追加、state 更新、审计批量折算） |

推算结论：

- **Redis**：峰值 ~10k ops/s，单实例（10w+ ops/s）余量一个数量级，无需集群。
- **PG**：峰值写入 <5k TPS（审计已批量折算），单实例可承担；主从读写分离作为扩容预留，非必需。
- **Worker 并发**：瓶颈是 LLM 生成时长（秒级）而非 CPU。单节点 200 并发 session × 10 节点 = 2,000 并发，
  对应峰值 1,000 msg/s 下平均 2s 生成时长恰好打满，HPA 按积压长度扩容有明确触发信号。
- **存储增长**：每条消息 event 约 5KB，日均 860 万条消息（100 msg/s 均值）约 43GB/天——
  靠 5.1 的月度归档任务控制在线数据量，这是归档任务不可省略的原因。
- **IM 回调峰值**：与入站消息峰值相同（1,000 回调/s），Gateway 无状态水平扩即可。

#### 5.2.2 降级策略

| 故障场景 | 检测 | 动作 | 用户感知 |
|---|---|---|---|
| Worker 节点故障 | 消费组 pending 超时 | 存活节点 XCLAIM 接管消息，state 从 event 重放重建 | 该条消息延迟增加数秒，不丢 |
| IM 平台重推回调 | `dedup:` key 命中 | 直接丢弃并返回 success | 无感知 |
| PG 短暂不可用 | 写失败/连接超时 | 指数退避重试；消息留在 Stream 不 XACK，持续故障则暂停消费并告警，恢复后自动追上 | 回复延迟，不丢消息 |
| Redis 不可用 | 连接失败 | Gateway 无法去重和入队，返回 IM 失败让其稍后重推（利用 IM 自身的重试机制） | 回复延迟 |
| 模型超时 | context deadline（如 60s） | 取消本次生成，重试 1 次；仍失败回复「服务繁忙请稍后再试」并记审计 | 收到降级回复 |
| 工具执行失败 | tool 返回 error / 超时 | 失败结果回灌给 LLM，由模型如实告知用户「该操作未完成」；危险工具不自动重试 | 收到说明性回复 |

Go 并发安全专项（Worker 长进程不泄漏）：

- Runner 调用传入带 deadline 的 `context.Context`，取消信号沿 Runner → Tool → HTTP 调用全程传播。
- Runner 返回的事件通道必须**消费到关闭**：提前取消时也要排空通道，否则框架侧 goroutine 阻塞泄漏。
- 每个会话处理协程的附属协程（锁续期 watchdog、流式转发）随会话结束通过 `errgroup`/`WaitGroup` 统一回收，
  禁止 fire-and-forget 起裸 goroutine。
- 进程退出时先停止从 Stream 拉新消息，排空在途会话后再退出（graceful shutdown）。

#### 5.2.3 灰度发布与配置回滚

- 应用配置灰度：`agent_app` 新版本先以 `draft` 保存 → 仅对灰度租户发布（置 `published`）→
  观察该租户的错误率、延迟、token 成本 → 无异常则全量发布。「同租户同名最多一个 published」
  由 5.1 的部分唯一索引保证，切换是原子操作。
- 配置回滚：将旧版本重新置为 `published` 即完成回滚；Admin API 在发布/回滚时通过 Redis pub/sub
  广播配置失效通知，Worker 收到后立即丢弃本地缓存、下次请求重新加载，秒级生效
  （缓存 TTL 自然过期作为广播丢失时的兜底）；事件层已按旧/新版本各自记录，互不污染。
- 平台自身发布：Gateway 与 Worker 均无状态，滚动更新；Worker 摘流量后依靠 Stream pending
  机制交接在途消息，发版不丢消息。

#### 5.2.4 监控告警

指标清单（OTel，全部带 `tenant_id` 维度）：

| 分类 | 指标 |
|---|---|
| 流量 | IM 回调 QPS、入站/出站消息量 |
| 延迟 | 端到端 P95（回调→回复落 IM）、模型调用耗时、工具调用耗时、Session/Memory 读写耗时 |
| 质量 | IM 投递成功率、错误率（按 error_type 分类）、模型超时率 |
| 成本 | token 消耗、每租户成本（与 audit_log 行级明细对账） |
| 队列 | Stream 长度、pending 数、最老 pending 停留时长、死信数 |
| 资源 | Worker 并发 session 数、goroutine 数（泄漏监控）、Redis/PG 连接池水位 |

告警规则示例：Stream 积压 >5 万条或最老 pending 停留 >5 分钟；IM 投递成功率 <99%；
端到端 P95 >15s（突破第 1 节目标）；错误率 >1%；审计批量写连续失败；goroutine 数持续上涨。

#### 5.2.5 部署方案

- 最小可运行（本地/演示）：Docker Compose 四个服务——`trpc-service`（all-in-one 模式，
  单进程同时扮演 Gateway + Worker + Admin）、PostgreSQL、Redis；Artifact 用 MinIO 容器。
- 生产推荐（Kubernetes）：
  - `gateway` Deployment ≥2 副本，无状态，前置 LB 接 IM 回调；
  - `worker` Deployment，HPA 按 Stream 积压长度 + 并发 session 数扩缩（2–10 副本），CPU 仅作兜底
    （Worker 负载大头是等 LLM 返回的 IO 等待，CPU 低不代表有余量）；
  - `admin` Deployment 1–2 副本，仅内网可达；
  - PG 主从、Redis 哨兵，OTel Collector 边车或 DaemonSet 收 trace/metrics；
  - KMS 用云厂商服务，Secret Resolver 走短 TTL 缓存。

#### 5.2.6 后端数据迁移

租户更换存储后端（如 session 从 Redis 迁 PG、向量库从 pgvector 迁 Qdrant）时服务不中断，统一走四步：

1. **双写开启**：Admin 发起迁移后，Storage Adapter 对该租户同时写新旧两个后端，读仍走旧后端。
2. **存量回填**：后台任务把历史数据从旧后端搬至新后端（session 只搬活跃会话，冷数据按需懒加载；
   向量库全量重建索引），回填进度可观测。
3. **读切换**：回填完成且新旧一致性质检通过后，读切到新后端，双写保留一个观察窗口（如 24h）。
4. **旧后端下线**：观察窗口内无异常，停止双写，`tenant.storage_config` 指向新后端，旧数据按保留策略清理。

任何一步失败都可回退到上一状态（读未切换前回退无代价，切换后回退即把读指回旧后端，
双写保证期间数据不丢）。迁移全程按租户隔离进行，不影响其他租户。

## 6. 预期效果

### 6.1 量化指标

对照第 1 节目标，给出达成口径（手段均已在前文展开）：

| 指标 | 目标 | 达成手段 |
|---|---|---|
| 租户规模与隔离 | ≥50 租户，配置/会话/记忆/知识库/工具/审计完全隔离 | 租户模型 + `tenant_id` 全链路透传（第 3、5.1 节） |
| 节点扩展与路由 | Worker 水平扩至 ≥10 节点，路由正确率 100% | 无状态 Worker + 共享 Session 后端 + `(app_id, session_key)` 路由（第 3、5.1 节） |
| 端到端延迟 | 回调到回复落 IM，P95 ≤15s（含 LLM） | 同步应答 + 异步消费（决策一）；P95 告警阈值 15s（5.2.4） |
| 消息幂等 | 无重复回复 | 入口 dedup + 事件唯一约束 + 出站 sent 三层幂等（5.1.4） |
| 多后端 | ≥3 类后端，租户级可配、可迁移 | 推荐栈 + 受控菜单（5.1.1、第 3 节图例表） |
| 可观测 | 单条 trace 串联全链路，覆盖率 100% | 框架 OTel Hooks + Gateway/队列/Guardrail 三处补 span（4.3） |
| 审计合规 | 审计字段完整，密钥零明文 | `audit_log` schema（5.1.3）+ 密钥引用（决策三） |
| 吞吐容量 | 峰值 1,000 msg/s、2,000 并发 session | 容量推算（5.2.1） |

### 6.2 资源评估

- **最小部署（开发/演示）**：1 台 4C8G 机器跑 Docker Compose（all-in-one 进程 + PG + Redis + MinIO）。
- **生产部署**：Gateway 2×2C4G，Worker 2–10×2C4G（HPA），Admin 1×1C2G；
  PG 2×4C8G 主从，Redis 哨兵 3 节点；规格为按 5.2.1 负载假设的经验配置，上线前以压测校准。
- **存储**：生产环境事件与审计在线保留 1 个月约 1.3TB（按 5.2.1 的 43GB/天估算），
  更早数据由月度归档任务搬至同构归档表（见 5.1.3），在线数据量保持平稳；最小部署为演示流量，数据量可忽略。
- **人力**：1 人，阶段拆分见第 7 节。

### 6.3 验收标准

对照题目验收标准（README）逐条给出本文档落点：

| # | 题目验收标准 | 本文档落点 |
|---|---|---|
| 1 | 覆盖多租户、节点化部署、数据同步、多后端、IM 接入、治理监控、故障恢复 | 第 2–5 章 |
| 2 | 数据模型表达 tenant / agent / channel binding / session / event / memory / summary / audit log 关系 | 5.1.2、5.1.3 |
| 3 | 说明至少两种 IM 通道的接入差异，至少含微信或企微 | 第 3 节（Channel Adapter）、5.1.3 `channel_binding.config` 渠道专有字段 |
| 4 | 说明至少三类后端的存储与同步策略 | 4.2 选型对比、第 3 节图例表、5.1.1 租户级后端策略 |
| 5 | 完整消息链路时序说明，`trace_id` 贯穿 | 第 3 节时序图 |
| 6 | 至少 8 个生产风险及缓解措施 | 第 8 节（9 条 + 回滚方案） |
| 7 | 明确哪些能力复用 tRPC-Agent-Go、哪些需新增平台层模块 | 4.3 框架能力映射表 |

## 7. 时间规划与工作量

总周期：8/26 – 9/9（15 天，其中 11 个工作日），1 人。

里程碑：

| 阶段 | 内容 | 产出物 | 时间 |
|---|---|---|---|
| 阶段 0：环境准备 | 开发环境搭建（compose、PG schema、Redis）；企微测试号/接口权限申请发起 | 可运行的开发环境 | 8/26 – 8/28（3 天） |
| 阶段 1：MVP 最小闭环 | Mock Channel 先行打通「回调→去重入队→Worker/Runner→PG/Redis 存储→出站回复」；企微真实接入并行开发 | 单租户 all-in-one 端到端可演示链路 | 8/31 – 9/3（4 天） |
| 阶段 2：功能完善 | 多租户模型与路由、Storage Adapter 双后端、Guardrail 链、审计异步写、OTel 全链路、Memory/Knowledge 接入 | 多租户完整功能 + 单测覆盖核心链路 | 9/4 – 9/7（2 工作日 + 周末机动） |
| 阶段 3：灰度上线 | Admin API、微信客服通道、监控告警、compose 交付、文档收尾、对照 6.3 验收自查 | 可交付版本 + 验收自查表 | 9/8 – 9/9（2 天） |

模块级工作量评估（人日，含开发 + 自测）：

| 模块 | 工作量 | 备注 |
|---|---:|---|
| 基础设施（compose / schema / config / log） | 1 | |
| `channels`（Mock + 企微） | 2 | 企微加解密若需全自建则 +1 |
| Gateway + Stream 收发 + 幂等 | 1 | |
| Worker + Runner 装配 + 分布式锁 | 1.5 | |
| `storage` Adapter（Redis/PG） | 1 | |
| `tenant` 路由 + Admin API | 1 | |
| Guardrail + 审计 | 0.5 | |
| OTel + metrics | 0.5 | |
| 联调、测试、文档收尾 | 1.5 | |
| **合计** | **10** | 11 个工作日可覆盖，周末机动留给返工 |

依赖与风险：

- 企微测试号/接口权限申请（风险 7）须在第 0 阶段就发起，Mock Channel 保证其阻塞不影响主线。
- 企微加解密若需全自建（+1 天）、框架 session 后端不满足三点要求需自研兜底（+1~2 天）会触发
  工期重估，超出部分从阶段 2 的周末机动中扣。
- 微信客服通道排在阶段 3，若时间不足可降级为「设计覆盖、实现留接口」，优先保证企微通道完整交付。

## 8. 风险评估

| # | 风险 | 类别 | 影响 | 缓解措施 |
|---|---|---|---|---|
| 1 | Redis 单点故障 | 技术（基础设施） | 去重、队列、锁全部失效，服务整体不可用 | 生产部署哨兵；故障期间返回失败利用 IM 重推兜底（见 5.2.2）；恢复后 Stream 无积压损失 |
| 2 | 分布式锁失效（watchdog 崩溃、锁 TTL 过短） | 技术（并发） | 同一会话被两个 Worker 并发写 | 续期 + Lua 安全释放（5.1.4）；事件层唯一约束保证不丢不重，state 重放修复 |
| 3 | Stream 消息积压（Worker 打满或下游故障、单租户灌量） | 技术（容量） | MAXLEN 截断导致丢消息，且截断可能波及其他租户 | Gateway 入口租户级令牌桶限流（5.1.4）；积压长度 + 最老 pending 告警（5.2.4）；HPA 按积压扩容；死信队列人工介入 |
| 4 | `session_event` / `audit_log` 数据膨胀 | 技术（容量） | 查询变慢、存储成本失控 | 月度归档任务搬至归档表（5.1）；按租户 audit_policy 配置保留期 |
| 5 | LLM 超时、限流或成本失控 | 技术（外部依赖） | 端到端 P95 破 15s 目标，租户账单超标 | context deadline + 重试 1 次（5.2.2）；Guardrail 预算限制；每租户 token/成本监控告警 |
| 6 | KMS 不可用 | 技术（外部依赖） | 验签解密失败，渠道收发全停 | Resolver 短 TTL 缓存扛短暂故障；密钥轮换期双引用并存；本地开发用文件 Resolver |
| 7 | 企微/微信侧接入依赖（测试号申请、企业认证、接口权限）耗时超预期 | 进度 | IM 联调阻塞整体进度 | 先用 Mock Channel 跑通「回调→Worker→回复」全链路，IM 真实接入与平台开发并行 |
| 8 | 密钥或敏感信息泄漏进日志/trace | 安全合规 | 触碰第 1 节审计合规红线 | 密钥只存引用（决策三）；日志脱敏中间件；OTel span 属性白名单制；上线前扫描日志样本 |
| 9 | 租户误改 `storage_config` 导致读写路由到错误后端 | 运维 | 会话/记忆读不到，表现为数据丢失 | 变更走迁移流程（双写过渡、按租户灰度），禁止直接改配置生切；变更操作记审计 |

回滚方案：

- 应用配置回滚：旧版本重新置 `published` + pub/sub 广播失效，秒级生效（见 5.2.3）。
- 平台版本回滚：Gateway/Worker 无状态，K8s 滚动回退上一镜像；在途消息由 Stream pending 机制交接，不丢。
- 数据层回滚：schema 变更只做增量（加列、加表、加索引），不做破坏性变更；必须改列时先加新列双写、迁移后删旧列。