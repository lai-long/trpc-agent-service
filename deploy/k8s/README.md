# Kubernetes 生产部署（design 5.2.5）

最小拓扑：gateway / worker / admin 三个 Deployment 各自独立扩缩，共享同一套
PG / Redis / S3 / KMS 中间件（本目录不部署中间件本身，用已有的或托管服务）。

```bash
# 构建镜像
docker build -t trpc-agent-service:latest .

# 平台配置 + 基础设施引用（先按环境改 config.yaml 里的占位符）
kubectl apply -f deploy/k8s/config.yaml
kubectl create secret generic trpc-admin-token --from-literal=token="$(openssl rand -hex 24)"

kubectl apply -f deploy/k8s/gateway.yaml
kubectl apply -f deploy/k8s/worker.yaml
kubectl apply -f deploy/k8s/admin.yaml
```

要点：

- **角色与端口**：gateway 对外（IM webhook 经 LB/Ingress 进来）；worker 只暴露
  metrics（TRPC_WORKER_ADDR）；admin 仅 ClusterIP（内网），需要更强管控时上
  TRPC_ADMIN_TLS_CERT/KEY/CLIENT_CA 三件套启用 mTLS。
- **HPA**：worker 默认按 CPU 兜底；真正的扩容信号是 `stream_length` 积压长度，
  需要 prometheus-adapter 把该指标接进 HPA（worker.yaml 里有注释示例）。
- **优雅停机**：worker 的 `terminationGracePeriodSeconds` 必须大于排空上限
  （排空 = 模型超时 60s × 重试 + 余量，默认给 130s）；滚动更新时在途会话由
  Stream pending + XCLAIM 接管，不丢消息。
- **密钥**：所有密钥以引用（`*_REF`）存在于配置中，运行时经 KMS Resolver 取值；
  不要把明文写进 ConfigMap/Secret。
- **有状态依赖**：PG 主从、Redis 哨兵、MinIO/云 OSS、OTel Collector、KMS sidecar
  均为外部中间件，按你的环境接入。
