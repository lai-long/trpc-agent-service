# 上手教程

跟着本教程走完，你会从「克隆仓库」到「在平台上挂一个自己写的工具、接一个自己写的 IM 通道」。
它采用**渐进式**结构：每一章只解决一件事，最后都有一个可以立刻验证的产物，读不下去随时能停。

---

## 这是什么

一个**多租户的 IM Agent 平台**：企业微信 / 微信客服里的一句 `@机器人`，经过验签、限流、去重入队，
由无状态的 Worker 跑 tRPC-Agent-Go 的 Runner（含工具调用、记忆、知识库），再异步把回复推回 IM。
同一套进程通过 `channel_binding` 行区分租户，租户之间会话、记忆、工具权限、审计互相不可见。

一句话概括消息的一生：**同步应答、异步消费**。IM 回调立刻返回，真正的回复几十秒后由 Worker 算完再下发。

## 最短路径：先跑起来

只要 4 步，1 分钟内能看到第一条消息被接受（**前提**：Docker 和一个 OpenAI 兼容的 API key）。

```bash
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service

# ① 起依赖：pgvector(PG16) / redis(宿主 6380) / minio / jaeger / prometheus
docker compose up -d

# ② 放模型密钥（文件名必须叫这个，见附录 A 的引用机制）
mkdir -p data/secrets
echo -n 'sk-你的模型APIKey' > data/secrets/deepseek-apikey

# ③ 构建 + 启动（all-in-one：单进程兼任 gateway + worker + admin）
./build.sh
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true \
TRPC_METRICS_ADDR=127.0.0.1:8083 TRPC_SESSION_BACKEND=postgres ./start.sh

# ④ 发一条消息
curl -X POST 127.0.0.1:8080/mock/callback -H 'Content-Type: application/json' \
  -d '{"msg_id":"demo-001","user_id":"u-demo","text":"用一句话说明什么是幂等"}'
# → {"reply":"","status":"accepted"}
```

返回 `accepted` 且 `reply` 为空**就是成功**——回复走异步链路。想看到正文，继续第 2 章。

## 学习路线

| 章节 | 你会得到什么 | 依赖 |
|---|---|---|
| [1. 跑通第一条消息](#1-跑通第一条消息) | 完整平台在跑，消息被接受 | 最短路径 |
| [2. 看到 Agent 的回复](#2-看到-agent-的回复) | 用 SQL / 审计 / 日志三种方式确认回复真的产生了 | 1 |
| [3. 这条消息走过了什么](#3-这条消息走过了什么) | 一个能自己排查问题的心智模型 | 1 |
| [4. 建你自己的租户、应用和绑定](#4-建你自己的租户应用和绑定) | 一个属于你的租户，用专属回调路径对话 | 1 |
| [5. 危险工具二次确认](#5-危险工具二次确认) | 完整走一遍带内审批的两轮对话 | 4 |
| [6. 观测：指标、队列、追踪](#6-观测指标队列追踪) | 定位「消息没回复」的第一手手段 | 1 |
| [7. 接真实企业微信](#7-接真实企业微信) | 真实 IM 通道收发 | 4 |
| [8. 写一个自己的工具](#8-写一个自己的工具) | 一个被模型真实调用的业务工具 | 4 |
| [9. 接一个新的 IM 通道](#9-接一个新的-im-通道) | 一个自定义通道出现在绑定列表里 | 4 |
| [10. 像生产那样按角色拆进程](#10-像生产那样按角色拆进程) | 三进程部署，体会 worker 无状态 | 1 |

**附录**是参考手册，用到了再查：[A 配置与密钥](#附录-a-配置与密钥机制) ·
[B 故障排查](#附录-b-故障排查) · [C 重置开发环境](#附录-c-重置开发环境) · [D 日常命令](#附录-d-日常命令)。

## 前置条件

| 需要 | 版本 / 说明 | 检查命令 |
|---|---|---|
| Go | 1.27（见 `go.mod` 的 `go` 指令） | `go version` |
| Docker + Compose 插件 | 起 5 个依赖容器 | `docker compose version` |
| 一个 OpenAI 兼容的模型 API key | 默认对接 DeepSeek | — |
| 空闲端口 | 8080 / 8081 / 8082 / 5432 / 6380 / 9000 / 9001 | `ss -ltnp \| grep -E ':(8080\|8081\|8082\|8083)\b'` |

---

## 1. 跑通第一条消息

**目标**：平台在你本机跑起来，并接受第一条 IM 消息。

最短路径里的 4 步就是全部内容，这里只解释**为什么那四个环境变量是必需的**——它们都是
「安全默认关闭」，不设不是走默认值，而是功能被关掉或进程拒绝启动。

| 变量 | 不设会怎样 |
|---|---|
| `TRPC_ADMIN_TOKEN=dev-insecure` | **进程拒绝启动**（fail-closed）。`dev-insecure` 是哨兵值，只在绑 loopback 时被接受 |
| `TRPC_MOCK_CHANNEL=true` | mock 通道不挂载。它默认关，因为它是**无鉴权的消息注入器**，生产绝不能开 |
| `TRPC_METRICS_ADDR=127.0.0.1:8083` | 默认 8082；被占用时服务不崩，只是 metrics 关闭并打一条 WARN |
| `TRPC_SESSION_BACKEND=postgres` | 默认 `redis`，那样 PG 的 `session`/`session_event` 表是空的，你**没法用 SQL 查对话**（见第 2 章） |

**验证**：`curl` 返回 `{"reply":"","status":"accepted"}`，且 `data/trpc-service.log` 里出现
`gateway listening {"addr":":8080"}`。

> **你可能会撞上**：端口 8082 常被桌面应用监听。被占时只是没指标，换成 8083 即可：
> `ss -ltnp | grep -E ':(8080|8081|8082|8083)\b'` 看看是谁占的。

停止服务：`./stop.sh`。

→ 下一步：[第 2 章](#2-看到-agent-的回复)：把回复的正文挖出来。

---

## 2. 看到 Agent 的回复

**目标**：确认模型真的生成了回复，而不是只被队列吞掉。

这是第一次使用最容易困惑的地方：**mock 通道的 `Send` 只把回复记在内存里，日志只打长度不打正文**
（`channels/mock/mock.go:128`）。所以有三种看法，按推荐度排序。

### ① 查 PostgreSQL（推荐，能看到正文）

前提是启动时带了 `TRPC_SESSION_BACKEND=postgres`。事件的 JSON 是 OpenAI 兼容序列化，正文路径是
`event->'choices'->0->'message'->>'content'`（**不是** `content[0].text`）：

```bash
docker compose exec -T postgres psql -U trpc -d trpc -c "
SELECT e.event_seq,
       e.event->>'author'                                     AS 角色,
       left(e.event->'choices'->0->'message'->>'content', 80) AS 内容,
       e.event->'usage'->>'total_tokens'                      AS tokens
FROM session_event e JOIN session s ON s.id = e.session_id
WHERE s.session_key = 'dm:mock:u-demo'
ORDER BY e.event_seq"
```

实测输出：

```
 event_seq |   角色    |                          内容                           | tokens
-----------+-----------+-------------------------------------------------------+--------
         1 | user      | 用一句话说明什么是幂等                                  |
         2 | assistant | 幂等是指一个操作无论执行一次还是执行多次，产生的结果都相同… | 3016
```

事件 JSON 里还有 `usage.prompt_tokens_details.cached_tokens`、`completion_tokens_details.reasoning_tokens`、
`usage.timing_info.time_to_first_token`、`choices[0].message.reasoning_content`（思维链）、`model`、`author`、`branch`。

**`session_key` 怎么来的**：`channels.SessionKey()` 定义为单聊 `dm:{channel}:{user_id}`、群聊
`group:{channel}:{chat_id}`。所以 `user_id=u-demo` 走 mock 单聊就是 `dm:mock:u-demo`。

### ② 查审计（看得到延迟/token/成本/trace，看不到正文）

```bash
curl -s -H "Authorization: Bearer dev-insecure" \
  "127.0.0.1:8081/admin/audit?limit=5" | python3 -m json.tool
```

实测（节选）：

```json
{"channel":"mock","decision":"allow","latency_ms":1227,
 "tenant_id":"00000000-0000-0000-0000-000000000001",
 "trace_id":"04fa8fa09efaedb47114ba13a733072e","user_id":"u-demo"}
```

### ③ 查日志（只确认链路走通）

```bash
grep 'reply sent' data/trpc-service.log | tail
# reply sent {"channel":"mock","session_key":"dm:mock:u-demo",
#             "trace_id":"04fa8fa0…","text_len":101}
```

> **你可能会撞上**：PG 里查不到会话。原因是 `TRPC_SESSION_BACKEND` 默认 `redis`，会话存在 Redis 里，
> 键形如 `hashidx:evtdata:{app_id}:{user_id}:{session_key}`，PG 的会话表保持为空。
> **这不是 bug，是后端选择**——想在 PG 里查就切 `postgres`。

**验证**：上面 ①②③ 任意一种能看到内容或审计行。

→ 下一步：[第 3 章](#3-这条消息走过了什么)：理解为什么回复是异步的。

---

## 3. 这条消息走过了什么

**目标**：建立一个能自己排查问题的心智模型。

```
curl POST /mock/callback
   ↓  mock 适配器归一化（channel/msg_id/session_key/user_id/text）
Gateway :8080
   ↓  按 webhook_path 查 channel_binding → 得到 tenant_id / app_id
   ↓  租户令牌桶限流 → SET dedup:... NX EX 86400 去重 → XLEN 背压检查
   ↓  XADD stream:inbound（消息体带 W3C traceparent）
   ↓  立即返回 {"status":"accepted"}
Redis Stream（消费组 workers）
   ↓
Worker
   ↓  查 done: 幂等标记 → SET lock:sess:{app}:{session} NX EX 10（watchdog 续期）
   ↓  加载 memory / summary
   ↓  Guarded 治理链前置：白名单 → 审批应答 → 敏感词 → token 预算
   ↓  Assembler 按 app 取/建 Runner → runner.Runner.Run（llmagent 调 LLM）
   ↓  治理链后置：输出脱敏、拒绝词
   ↓  追加 session_event → 更新 session.state → 写 audit_log
   ↓  写 done: 标记 → XADD stream:outbound → XACK
Sender（消费组 senders）
   ↓  查 sent: 幂等 → 令牌桶限速 → 超 2048B 分段
   ↓  调 channel.Send
```

**为什么必须异步**：LLM 生成 P95 远超企微 5 秒的应答时限；而且被动回复一次回调只能回一条，
覆盖不了分段和审批这类多轮场景。

关键角色对应的进程/端口：

| 角色 | 端口 | 职责 |
|---|---|---|
| gateway | `:8080` | 接 IM 回调，验签/去重/限流/入队。**公网可达的唯一入口** |
| worker | — | 消费队列，跑 Runner，写存储 |
| admin | `127.0.0.1:8081` | `/admin/*` 管理 API，Bearer token 鉴权 |
| metrics | `127.0.0.1:8082`（教程用 8083） | `/metrics`，Prometheus 与探针抓取 |

三个口彼此分离本身是安全设计：Admin 和 metrics 只绑内网，回调口靠验签而非 token。

`all-in-one`（`serve` 或 `serve all`）把四者放进一个进程，方便本地调试；生产按角色拆开，见第 10 章。

→ 下一步：[第 4 章](#4-建你自己的租户应用和绑定)：建一套属于你的配置。
（想先知道消息卡住时怎么查，可以跳到第 6 章。）

---

## 4. 建你自己的租户、应用和绑定

**目标**：从零建一个租户，用**它自己的回调路径**发出一条消息，不需要重启服务。

`seed.sql` 已经灌了一个演示租户（`demo-tenant`）+ 一个已发布应用（`assistant`）+ 几条绑定，
所以第 1 章能直接发消息。下面是从零建一套的完整流程，**全部实测通过**。

```bash
H='Authorization: Bearer dev-insecure'
A=127.0.0.1:8081

# ① 建租户（策略字段都可省略，省略即走平台默认）
T=$(curl -s -X POST $A/admin/tenants -H "$H" -H 'Content-Type: application/json' \
     -d '{"name":"acme-demo"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "tenant_id = $T"

# ② 建应用（新建即 draft，未发布不接客）
APP=$(curl -s -X POST $A/admin/tenants/$T/apps -H "$H" -H 'Content-Type: application/json' \
     -d '{"name":"support","agent_type":"llm","config":{
            "prompt":"你是 ACME 的客服助手，回答简洁。",
            "tools":{"allow":["get_weather","delete_user_data"]}}}' \
     | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "app_id = $APP"

# ③ 发布（原子切换：同租户同名应用最多一个 published）
curl -s -X POST $A/admin/apps/$APP/publish -H "$H" -H 'Content-Type: application/json' \
     -d '{"version":1}'
# → {"published":"f365b692-cc9d-4ed5-a52d-692fc7b4026d"}

# ④ 建渠道绑定（webhook_path 留空 → 自动填充为 /callback/{channel}/{binding_id}）
curl -s -X POST $A/admin/apps/$APP/bindings -H "$H" -H 'Content-Type: application/json' \
     -d '{"channel":"mock"}' | python3 -m json.tool
# → {"id":"cb2fa915-…","webhook_path":"/callback/mock/cb2fa915-…"}

# ⑤ 立刻用新路径发消息——不需要重启服务
B=cb2fa915-ecab-4c39-be6e-7584367ca161
curl -s -X POST 127.0.0.1:8080/callback/mock/$B -H 'Content-Type: application/json' \
     -d '{"msg_id":"tut-001","user_id":"u-acme","text":"你们支持哪些渠道？一句话"}'
# → {"reply":"","status":"accepted"}
```

**验证**：新绑定**立即可达**（配置快照 TTL 30s + Redis pub/sub 失效广播，Admin 写操作会主动广播）；
会话落在新租户的 app 命名空间下；审计记录的 `tenant_id` 正是新建的那个。

### 请求体字段速查

| 接口 | 必填 | 可选 |
|---|---|---|
| `POST /admin/tenants` | `name` | `model_config` `tool_policy` `audit_policy` `guardrail_policy` `rate_policy` `storage_config` |
| `POST /admin/tenants/{id}/apps` | `name` `agent_type` `config` | — |
| `POST /admin/apps/{id}/publish` | `version` | — |
| `POST /admin/apps/{id}/bindings` | `channel` | `webhook_path` `token_ref` `aeskey_ref` `config` |
| `POST /admin/tenants/{id}/storage-migrations` | `resource` `to_backend` | — |
| `POST /admin/apps/{id}/knowledge/documents` | `name` `content` | — |

### 会被 400 拒掉的写法（都是有意的，不是 bug）

- `webhook_path` 填一个平台没挂载的路径 → 400。否则绑定建成功、列表里也正常，但 IM 每次回调都在
  mux 上 404，**是个静默黑洞**。留空让系统自动填充最安全。
- `channel=mock` 却填 `webhook_path=/mock/callback` → 400。legacy 路径是启动时用 env 全局凭据挂载的，
  绑定行自带凭据却挂在那条路径上会「看起来权威、实际验签从不读它」。
- `config` 里出现未知字段 → 400。`config` 会原样写进审计明细，一个未被通道识别的键（比如明文
  `secret`）会**既进审计又不生效**。
- `channel=wecomws` 但 `config` 缺 `bot_id` 或 `secret_ref`，或 `webhook_path` 不匹配
  `^/wecomws/[A-Za-z0-9_-]+$` → 400。
- 密钥字段只收**引用名**（如 `wecom-secret`），不要填明文。

### 其他常用调用

```bash
curl -s -H "$H" $A/admin/tenants                        # 列租户
curl -s -H "$H" $A/admin/tenants/$T                     # 租户详情
curl -s -H "$H" $A/admin/tenants/$T/apps                # 列应用
curl -s -X POST $A/admin/apps/$APP/rollback -H "$H" -H 'Content-Type: application/json' -d '{"version":1}'
curl -s -H "$H" "$A/admin/audit?tenant_id=$T&decision=deny"
curl -s -X DELETE $A/admin/apps/$APP/bindings/$B -H "$H"
```

不带 token → **401**；未挂载的回调路径 → **404**；同 `msg_id` 重发 → `{"status":"duplicate"}`。

→ 下一步：[第 5 章](#5-危险工具二次确认)：让模型在动手前先问一句。

---

## 5. 危险工具二次确认

**目标**：完整走一遍「模型想调危险工具 → 平台拦下并问用户 → 用户确认 → 工具放行」。

`delete_user_data` 被标记为 `Dangerous`（它是个 stub，不会删任何真实数据），专门用来演示审批链路。

### 第一轮：触发

```bash
curl -s -X POST 127.0.0.1:8080/callback/mock/$B -H 'Content-Type: application/json' \
  -d '{"msg_id":"appr-1","user_id":"u-appr","text":"请调用工具删除用户 u-999 的全部数据"}'
```

会话事件里实测看到：

```
 1 | user      | 请调用工具删除用户 u-999 的全部数据
 2 | assistant |
 3 | assistant | "blocked: 该操作需要用户在对话中确认后才能执行"
 4 | assistant | 删除用户 u-999 的全部数据属于不可恢复的危险操作。请确认：您确定要永久删除该用户的全部数据吗？
```

同时：

```bash
# 审计记下 review 决策
psql -c "SELECT decision, tool_name FROM audit_log WHERE user_id='u-appr'"
# → review | delete_user_data

# 待审批记录在 Redis，键带 app 维度（跨租户互不可见）
docker compose exec -T redis redis-cli --scan --pattern 'approval:*'
# → approval:f365b692-…:dm:mock:u-appr
```

### 第二轮：答复

```bash
curl -s -X POST 127.0.0.1:8080/callback/mock/$B -H 'Content-Type: application/json' \
  -d '{"msg_id":"appr-2","user_id":"u-appr","text":"确认"}'
```

**验证**：审计多出一行 `allow | delete_user_data`，Redis 里的 `approval:*` 键被消费掉，回复正常下发。

### 答复规则

精确匹配（`strings.TrimSpace` 后全等，`agent/approval.go:308`）：

| 你发 | 效果 |
|---|---|
| `确认` | 放行原工具调用 |
| `拒绝` | 终止并告知用户 |
| `取消` | 同拒绝 |
| 其他任何内容 | **不消费、也不作废审批**，按普通新消息正常处理，审批继续挂起 |

超时默认 **5 分钟**（`agent.DefaultApprovalTimeout`），超时按拒绝处理并记 `review_timeout`。
同一会话同时只允许一个待审批，冲突直接 `deny`（`error_type=approval_conflict`）。
群聊里只有消息发起人（或租户配置的审批人）的答复有效。

> **一个会让人困惑的设计**：答复那一轮**不会出现在 `session_event` 里**。
> 因为审批答复是**控制字，不是内容**——它携带的是模型没有产生过的工具结果，
> 治理链第 3 步直接返回回复、不进 Runner（`agent/guardrail.go:126` 的注释就是这个意思）。
> 副作用是模型下一轮也看不到「确认」这句话。这一轮只在 `audit_log`（`decision=allow`）和日志里可见。

→ 下一步：[第 8 章](#8-写一个自己的工具)：把你自己的危险工具接进来。

---

## 6. 观测：指标、队列、追踪

**目标**：掌握「消息没回复」时第一手该看什么。

```bash
# 指标（注意用你实际设的 metrics 端口）
curl -s 127.0.0.1:8083/metrics | grep -E 'im_inbound_total|llm_tokens_total|stream_length'

# 队列积压——排查「消息没回复」的第一站
docker compose exec -T redis redis-cli XLEN stream:inbound
docker compose exec -T redis redis-cli XLEN stream:outbound
docker compose exec -T redis redis-cli XLEN stream:deadletter

# 服务日志
tail -f data/trpc-service.log
```

指标都带 `channel` / `tenant_id` 维度。常用的几个：`im_inbound_total`、`im_outbound_total`、
`im_dedup_dropped_total`、`im_end_to_end_duration`、`worker_process_duration`、
`worker_process_error_total`、`llm_tokens_total`、`gateway_rejected_total`、
`send_rate_limited_total`、`audit_dropped_total`、`stream_length`、`stream_pending`、
`stream_oldest_pending_seconds`。

**链路追踪**：compose 里的 Jaeger 已就绪，启动时加上

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317   # gRPC exporter 要 host:port，不带 scheme
```

UI 在 <http://localhost:16686>。这个变量**不是必需的**——不设也能正常启动，只是不导出 trace。

Prometheus 在 <http://localhost:9090>，抓取配置是 `deploy/prometheus/prometheus.yml`
（默认抓 `host.docker.internal:8082`；你若改了 metrics 端口，这里也要跟着改）。

→ 下一步：[第 7 章](#7-接真实企业微信) 或 [第 10 章](#10-像生产那样按角色拆进程)。

---

## 7. 接真实企业微信

**目标**：把消息来源从 mock 换成真实 IM。

需要三样东西：**密钥文件**、**env 开关**、**公网 HTTPS 回调地址**。

```bash
# ① 密钥文件（文件名 = 下面的 *_REF 默认值，改了就同步改 env）
echo -n '你的Token'            > data/secrets/wecom-token
echo -n '你的EncodingAESKey'   > data/secrets/wecom-aeskey
echo -n '你的corpsecret'       > data/secrets/wecom-secret
chmod 600 data/secrets/wecom-*

# ② 启动时加通道开关（不设 = 该通道不挂载）
TRPC_ADMIN_TOKEN=dev-insecure \
TRPC_METRICS_ADDR=127.0.0.1:8083 \
TRPC_SESSION_BACKEND=postgres \
TRPC_WECOM_CORP_ID=ww你的corpid \
TRPC_WECOM_AGENT_ID=1000002 \
./start.sh

# ③ 企微管理后台「接收消息」里填的回调 URL
#    https://你的域名/wecom/callback               ← env 配置的单绑定默认路径
#    https://你的域名/callback/wecom/{binding_id}  ← 多租户路径，每个绑定用自己的密钥验签
```

**验证**：日志里出现 `gateway listening {"addr":":8080"}` 且没有 `wecom channel disabled` 之类的 WARN。

### 微信客服（wxkf）

`TRPC_WXKF_CORP_ID` + `TRPC_WXKF_KF_ACCOUNT`，密钥文件默认名 `wxkf-token` / `wxkf-aeskey` / `wxkf-secret`。
注意它**只处理 text 消息**（媒体是后续工作），且主动发送受 48 小时窗口限制。

### 企微智能机器人（wecomws，免公网回调）

适合内网/无域名场景，由平台**主动**连企微 WS 网关：

```bash
TRPC_WECOMWS_ADDR=wss://openws.work.weixin.qq.com ./start.sh
```

BotID / Secret 不放 env，放 `channel_binding.config`：

```bash
curl -s -X POST $A/admin/apps/$APP/bindings -H "$H" -H 'Content-Type: application/json' \
  -d '{"channel":"wecomws",
       "webhook_path":"/wecomws/你的botid",
       "config":{"bot_id":"你的botid","secret_ref":"wecomws-bot-secret"}}'
echo -n '你的BotSecret' > data/secrets/wecomws-bot-secret
```

> **你可能会撞上**：企微每 bot 同时只允许一条连接、新连接踢旧连接。所以多副本部署时由
> `lock:leader:wecomws` 选出全局单 leader 持有全部连接，其余副本待命——你看到某些副本
> 「没动静」是正常的。

四类通道的差异（连接方向、应答时限、媒体能力等）见 [`docs/README.md` §6](./README.md)。

→ 下一步：[第 9 章](#9-接一个新的-im-通道)：照着现有适配器写一个自己的通道。

---

## 8. 写一个自己的工具

**目标**：新增一个业务工具，让模型在对话中真实调用它，并受租户白名单约束。

平台工具就是框架的 `tool.Tool` 加一个平台元数据 `Dangerous`。`trpcservice/tool/tool.go` 已经把
模式写好了（`DemoTools` 就是两个 stub），照抄即可：

```go
// trpcservice/tool/biztools.go
package tool

import (
	"context"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

type orderArgs struct {
	OrderID string `json:"order_id" jsonschema:"description=订单号"`
}

type orderResult struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func queryOrder(_ context.Context, in orderArgs) (orderResult, error) {
	return orderResult{OrderID: in.OrderID, Status: "已发货"}, nil // TODO: 换成真实查询
}

// BizTools 是平台对外暴露的工具全集。新增工具在这里登记一次即可。
func BizTools() *Registry {
	return NewRegistry(
		Tool{Tool: function.NewFunctionTool(queryOrder,
			function.WithName("query_order"),
			function.WithDescription("按订单号查询物流状态"))},
		// Dangerous：命中后由治理链拦下，必须用户在对话中确认才真正执行。
		Tool{Tool: function.NewFunctionTool(refundOrder,
			function.WithName("refund_order"),
			function.WithDescription("对指定订单发起退款，不可撤销")), Dangerous: true},
	)
}
```

然后把它接到装配链上——`cmd/trpc-service/main.go:818` 那一行换成你的注册表：

```go
registry := tool.BizTools()   // 原来是 tool.DemoTools()
```

之后是**租户级收窄**，不需要改代码，改配置即可（`agent/assemble.go:360` 两级过滤：
`tenant.tool_policy` 先收窄，`agent_app.config.tools` 再收窄，非空 `allow` 即白名单）：

```bash
curl -s -X POST $A/admin/tenants/$T/apps -H "$H" -H 'Content-Type: application/json' \
  -d '{"name":"support","agent_type":"llm","config":{
         "prompt":"你是 ACME 的客服助手。",
         "tools":{"allow":["query_order","refund_order"]}}}'
```

**验证**：重新构建启动 → 发布应用 → 发一句「查一下订单 A-1001 到哪了」→ 用第 2 章的 SQL 看
`session_event`，会出现工具调用与结果事件；再说「给它退款」会先被拦下走第 5 章的审批流程。

> **注意**：工具名一旦被写进租户白名单就成了配置的一部分。改名或删除工具时，老租户的
> `allow` 里会留下一个不存在的名字——`Registry.Allowed` 对未知名字是**忽略**而不是报错，
> 所以表现为「这个工具突然消失了」，而不是报错。

→ 下一步：[第 9 章](#9-接一个新的-im-通道)。

---

## 9. 接一个新的 IM 通道

**目标**：写一个自定义通道，让它出现在绑定列表里并能收发消息。

一个通道只需要做三件事（`trpcservice/channels/channels.go:210`）：

```go
type Channel interface {
	Name() string                                          // 通道标识，对应 channel_binding.channel
	RegisterRoutes(mux *http.ServeMux, h Handler)          // 挂载 IM 回调（入站）
	Send(ctx context.Context, msg OutboundMessage) error   // 调 IM 主动发送接口（出站）
}
```

最小骨架（`channels/mock/mock.go` 就是这个形状的 100 行版本，可以直接抄）：

```go
// trpcservice/channels/feishu/feishu.go
package feishu

const ChannelName = "feishu"
const CallbackPath = "/feishu/callback"

type Channel struct{ client *http.Client }

func (c *Channel) Name() string { return ChannelName }

func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	mux.HandleFunc(http.MethodPost+" "+CallbackPath, c.callback(h))
}

func (c *Channel) callback(h channels.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req callbackRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		msg := channels.InboundMessage{
			Channel:     c.Name(),
			MsgID:       req.MsgID,     // 平台按它做入口去重
			SessionKey:  channels.SessionKey(c.Name(), req.UserID, req.ChatID),
			UserID:      req.UserID,
			ChatID:      req.ChatID,
			Text:        req.Text,
			WebhookPath: r.URL.Path,    // Gateway 靠它路由到 tenant/app
			ReceivedAt:  time.Now(),
		}
		out, err := h.Handle(r.Context(), msg)
		if errors.Is(err, channels.ErrDuplicate) {
			// 重复投递是成功结果：必须回 200，否则 IM 会一直重推
			return
		}
		_ = err
		_ = out // out.Text 为空 = 回复走异步链路，由 Sender 调 Send 下发
	}
}

func (c *Channel) Send(ctx context.Context, msg channels.OutboundMessage) error {
	for _, seg := range channels.SplitText(msg.Text, 2048) { // 按字节切，不切坏 UTF-8
		// 调飞书 open api 发送 seg
	}
	return nil
}
```

挂到进程里（`cmd/trpc-service/main.go:220` 的 `channelSet`）：

```go
if cfg.FeishuEnabled {                    // 你自己的 env 开关
	ch := feishu.New()
	ch.RegisterRoutes(mux, enqueue)       // 入站
	channelSet[ch.Name()] = ch            // 出站：Sender 按 msg.Channel 找到它
}
```

最后建绑定（`channel` 必须等于 `Name()`）：

```bash
curl -s -X POST $A/admin/apps/$APP/bindings -H "$H" -H 'Content-Type: application/json' \
  -d '{"channel":"feishu"}'
# → webhook_path 自动填充为 /callback/feishu/{binding_id}
```

> **两个一定会踩的坑**：
>
> 1. **没实现 `BindingAware` 就只能用 legacy 路径**。`web/binding.go:50` 对不支持按绑定验签的通道
>    直接回 404（`channel does not support bindings`），`/callback/feishu/{id}` 永远不通。
>    要么实现 `CallbackHandler(h, creds)` 走自动填充路径，要么把 `CallbackPath` 登记进
>    `web/admin.go` 的 `legacyCallbackPaths`（那里只接受这两种形状，其他 `webhook_path` 一律 400）。
> 2. **出站要用 `SplitText` + 串行发送**。平台统一按 2048 字节切分，分段共享同一 trace；
>    出站错误记得过一遍 `channels.ScrubError`，它会把 URL 里的 `access_token` 打码——
>    字段级日志脱敏看不见字符串内部的东西。

→ 下一步：[第 10 章](#10-像生产那样按角色拆进程)。

---

## 10. 像生产那样按角色拆进程

**目标**：三进程部署，并亲身体会「worker 无状态」这件事。

```bash
# 三个终端，各自一个角色（共用同一套 PG/Redis）
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./bin/trpc-service serve gateway
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./bin/trpc-service serve worker
TRPC_ADMIN_TOKEN=dev-insecure                        ./bin/trpc-service serve admin
```

用法：`trpc-service serve [all|gateway|worker|admin]`，不带参数等价于 `all`。

**验证**：起两个 worker 进程，消息会被消费组自动分摊；`kill` 掉其中一个，另一个通过
`XAUTOCLAIM` 接管 pending 消息，不丢。gateway 只做验签/去重/入队（全是 Redis 操作），可以随便加副本。

生产部署（Deployment / HPA / Ingress / db-init Job / Secret 挂载）见
[`deploy/k8s/README.md`](../deploy/k8s/README.md)。

---

## 附录 A：配置与密钥机制

**配置只来自环境变量**（约 65 个 `TRPC_*`），仓库里没有配置文件。
全部由 `trpcservice/config/config.go` 的 `Load()` 集中读取，未设则取默认值。

**密钥永远不出现在配置和数据库里**，只存**引用名**：

```
channel_binding.token_ref = "wecom-token"      ← 这是引用，不是密钥
                                  ↓  运行时
                    SecretResolver 解析（TRPC_SECRET_RESOLVER）
                                  ↓
       file（默认）：读 $TRPC_SECRETS_DIR/wecom-token，即 data/secrets/wecom-token
       kms（生产）：向 TRPC_KMS_ENDPOINT 取值，用 TRPC_SECRETS_DIR 下的 bootstrap token 鉴权
                                  ↓
                    CachedResolver 缓存 1 分钟（TRPC_SECRET_CACHE_TTL）
```

所以本地开发只要把密钥写成 `data/secrets/<引用名>` 即可，权限建议 600。
`data/` 已被 `.gitignore` 忽略（只保留 `data/README.md`），**不要把它提交上去**。

### 常用变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `TRPC_HTTP_ADDR` | `:8080` | 回调口 |
| `TRPC_ADMIN_ADDR` | `127.0.0.1:8081` | Admin 口 |
| `TRPC_METRICS_ADDR` | `127.0.0.1:8082` | 指标口 |
| `TRPC_ADMIN_TOKEN` | `""`（**拒绝启动**） | Admin Bearer token；本地哨兵值 `dev-insecure` |
| `TRPC_PG_DSN` | `postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable` | 与 compose 一致 |
| `TRPC_REDIS_ADDR` | `localhost:6380` | compose 把容器 6379 映射到宿主 **6380** |
| `TRPC_SESSION_BACKEND` | `redis` | `redis` 或 `postgres` |
| `TRPC_SECRET_RESOLVER` | `file` | 生产设 `kms` |
| `TRPC_SECRETS_DIR` | `data/secrets` | file resolver 的目录 |
| `TRPC_MODEL_BASE_URL` | `https://api.deepseek.com` | 受 `TRPC_MODEL_BASE_URL_ALLOW` 白名单约束，必须 https |
| `TRPC_MODEL_NAME` | `deepseek-v4-flash` | |
| `TRPC_MODEL_APIKEY_REF` | `deepseek-apikey` | 引用名 |
| `TRPC_MODEL_TIMEOUT` | `60s` | 超时后取消并重试 1 次 |
| `TRPC_MODEL_PRICES` | `""` | 配了才会算 `audit_log.cost` |
| `TRPC_S3_ENDPOINT` / `TRPC_S3_BUCKET` | `localhost:9000` / `artifacts` | Artifact 存储 |
| `TRPC_MOCK_CHANNEL` | `false` | 本地演示才开 |
| `TRPC_LOG_LEVEL` / `TRPC_LOG_FORMAT` | `info` / `console` | 生产建议 `json` |
| `TRPC_GATEWAY_RATE_QPS` / `_BURST` | `50` / `100` | 平台默认入口限流，租户 `rate_policy` 可覆盖 |
| `TRPC_SEND_RATE_QPS` / `_BURST` | `20` / `40` | 发送侧限流 |
| `TRPC_EMBEDDER_MODEL` | `""`（禁用） | 设了才启用向量检索；默认聊天端点没有 embeddings API |
| `TRPC_WECOM_CORP_ID` / `_AGENT_ID` | `""` | 设了才挂载企微通道 |
| `TRPC_WXKF_CORP_ID` / `_KF_ACCOUNT` | `""` | 设了才挂载微信客服通道 |
| `TRPC_WECOMWS_ADDR` | `""` | 设了才启用 WS 通道 |

---

## 附录 B：故障排查

按「你看到的症状」查。前四条是实测遇到过的。

| 症状 | 原因 | 解法 |
|---|---|---|
| 启动日志 `metrics listener failed … address already in use` | 8082 被别的程序占用 | 加 `TRPC_METRICS_ADDR=127.0.0.1:8083`。服务不会崩，只是没指标 |
| 一启动就刷 `worker … process … failed: unknown agent app: a1` / `tenant route inactive: tenant t1` | **Redis 里有集成测试残留消息**。Worker 串行消费，你的消息排在它们后面 | 见附录 C 重置，或 `XTRIM stream:inbound MAXLEN 0` |
| `/callback/mock/{binding_id}` 返回 `unknown binding` | 你的库是用**旧版 `seed.sql`** 灌的（initdb.d 只在空卷首次启动时跑），后来新增的绑定行从没进过库 | 见附录 C 重置，或手工 INSERT 那条 binding |
| `make test` 之后开发库多出一堆 `pgstore-…` 之类的租户、Redis 里多出队列消息 | `testenv.go` 的默认值就指向开发依赖：`TRPC_TEST_PG_DSN` 默认 = `TRPC_PG_DSN`（同一个 `trpc` 库），`TRPC_TEST_REDIS_ADDR` 默认 = `localhost:6380`（同一个 Redis）。CI 用全新 service container，所以只有本地会这样 | 给测试单独建库再跑：<br>`docker compose exec -T postgres psql -U trpc -d postgres -c 'CREATE DATABASE trpc_test'`<br>`docker compose exec -T postgres psql -U trpc -d trpc_test -v ON_ERROR_STOP=1 < deploy/db/init.sql`<br>`TRPC_TEST_PG_DSN='postgres://trpc:trpc-dev-only@localhost:5432/trpc_test?sslmode=disable' make test`<br>Redis 侧**没有等价开关**（配置只有 host:port，不支持 db index），队列残留只能按附录 C 清理或另起一个 Redis 实例 |
| 进程启动即退出，日志说 admin token 相关 | `TRPC_ADMIN_TOKEN` 未设（fail-closed） | 本地用 `dev-insecure`，生产用真 token |
| `resolve … no such file` 类错误 | `data/secrets/` 下缺对应引用名的文件 | 按附录 A 补齐，文件名必须与 `*_REF` 一致 |
| 发消息返回 `accepted` 但一直没有回复 | ① worker 没起（all-in-one 模式下看日志有无 `worker` 相关行）② 队列积压 ③ 模型调用失败 | 依次查 `XLEN stream:inbound`、`grep -a 'process .* failed' data/trpc-service.log`、`XLEN stream:deadletter` |
| 回复变成「服务繁忙请稍后再试」 | 模型超时（默认 60s）或报错，重试 1 次后降级 | 查日志里的 `ModelError`；确认 `data/secrets/deepseek-apikey` 有效、`TRPC_MODEL_NAME` 正确 |
| 消息进了 `stream:deadletter` | 出站发送连续失败超过 5 次 | 查日志定位（多为 IM 凭据或限流），修好后需人工重放 |
| PG 里查不到会话 | `TRPC_SESSION_BACKEND=redis`（默认） | 切 `postgres`，或按第 2 章末尾去 Redis 查 |
| 企微回调一直 404 | 通道没挂载（`TRPC_WECOM_CORP_ID` 未设）或 `webhook_path` 与后台填的 URL 不一致 | 查启动日志的通道挂载行；核对 `channel_binding.webhook_path` |

看日志的几个常用姿势：

```bash
grep -a 'reply sent'          data/trpc-service.log | tail    # 回复下发成功
grep -a 'process .* failed'   data/trpc-service.log | tail    # worker 处理失败
grep -a 'duplicate message'   data/trpc-service.log | tail    # 去重命中
grep -ac ERROR                data/trpc-service.log           # 错误总数
```

---

## 附录 C：重置开发环境

数据乱了（测试残留、旧 seed、想重来）最干净的办法是**连卷一起删**：

```bash
./stop.sh
docker compose down -v      # 删除 pgdata / redisdata / miniodata 三个卷
docker compose up -d        # 空卷启动 → 自动重跑 init.sql + seed.sql
```

`init.sql` 会建 13 张表（12 业务表 + `schema_migrations`）并把自己标为迁移版本 1；
`seed.sql` 会灌 demo 租户、已发布应用和 4 条绑定（含多租户演示用的 `/callback/mock/{binding_id}`）。

`data/secrets/` 在宿主机上，**不受影响**，密钥不用重配。

只想清 Redis 队列、保留 PG 数据：

```bash
for s in stream:inbound stream:outbound stream:deadletter; do
  docker compose exec -T redis redis-cli XTRIM $s MAXLEN 0
done
docker compose exec -T redis redis-cli --scan --pattern 'dedup:*'  | xargs -r -n50 docker compose exec -T redis redis-cli DEL
docker compose exec -T redis redis-cli --scan --pattern 'done:*'   | xargs -r -n50 docker compose exec -T redis redis-cli DEL
docker compose exec -T redis redis-cli --scan --pattern 'sent:*'   | xargs -r -n50 docker compose exec -T redis redis-cli DEL
```

想保留现有 PG 数据、只补 seed 里缺的绑定：直接 `INSERT INTO channel_binding …`，
参考 `deploy/db/seed.sql` 的第 4 条。

---

## 附录 D：日常命令

```bash
make deps      # docker compose up -d
make build     # ./build.sh  → bin/trpc-service
make start     # ./start.sh（环境变量要在命令前缀里给）
make stop      # ./stop.sh
make test      # go test ./...（不带 -race）
make cover     # ./coverage.sh → 带 -race 和覆盖率
make fmt lint  # gofmt / go vet + golangci-lint
make migrate   # ./deploy/db/migrate.sh up（增量 schema 迁移）
./clean.sh     # 清 bin/、根目录游离二进制、coverage 产物
```

跑测试需要 PG / Redis / MinIO 在线（`make deps`）。**依赖不在线时集成测试会 skip，
覆盖率会从 87.5% 掉到约 55%**——那不是有效测量，CI 的 zero-skip 门禁也会因此失败。
建议给测试指定独立库，避免污染开发数据（见附录 B）。

---

## 下一步读什么

| 想了解 | 去哪 |
|---|---|
| 架构、数据模型、多后端一致性、幂等与迁移、风险清单 | [`docs/README.md`](./README.md) |
| 完整技术方案：选型对比、容量推算、协议细节、取舍论证 | [`docs/design.md`](./design.md) |
| 数据库 schema 与演示数据 | `deploy/db/init.sql`、`deploy/db/seed.sql` |
| 增量 schema 迁移的约定 | `deploy/db/migrations/README.md` |
| 生产部署（Ingress / HPA / db-init Job / Secret 挂载） | `deploy/k8s/README.md` |
| 告警规则 | `deploy/prometheus/alerts.yml` |
