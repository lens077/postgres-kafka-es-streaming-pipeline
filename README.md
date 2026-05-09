# CDC 数据同步链路说明文档
本仓库实现了一套基于 Change Data Capture (CDC) 模式的实时数据同步系统，旨在将业务数据库(Postgres)的变更毫秒级同步至搜索与分析引擎(Elasticsearch)。

架构: [业务服务] ——> (Postgres 写库) ——> [CDC/Debezium] ——> Strimzi-Kafka ——> [消费者应用] ——> (ES 读库)
   1. Service: 业务微服务，负责处理交易逻辑并将数据持久化至 Postgres。
   2. Postgres (Source): 开启逻辑复制(Logical Replication)，作为数据源头。
   3. Debezium (Connect): 监听 Postgres WAL 日志，将变更转换为 JSON 消息发送至 Kafka。
   4. Kafka (Message Queue): 消息解耦与削峰填谷，存储原始变更记录。
   5. App Consumer: 自研高性能消费者，负责数据清洗、类型转换及批量写入。
   6. Elasticsearch (Sink): 提供全文检索与复杂聚合能力。
   7. Kibana (UI): 数据可视化验证与系统监控。

# 优点

- 完全物理分离

- 断点续传(Offset 管理):使用 CommitInterval: 0。 在 processMessage 成功将数据放入 BulkIndexer 队列后立即调用 CommitMessages。虽然 Bulk 是异步的，但在 CDC 最终一致性场景下，配合索引操作的幂等性(始终以 DB 主键作为 ES ID)，这能平衡性能与安全性。如果程序在 Bulk 刷新前崩溃，重启后 Kafka 会重发该位移后的消息，ES 侧执行相同的 Upsert，结果保持一致。

- 最终一致性(按序与幂等):代码逻辑直接从 Debezium 的 payload.after 提取数据。

- 幂等性: indexer.go 中强制指定 Action: "index" 且指定 DocumentID。这意味着无论重试多少次，同一 ID 的文档只会有最新的一份。

- 动态索引(健壮性):引入 sync.Map 缓存索引存在状态。不再需要预先在 main.go 里手动写死索引列表，一旦 Kafka 有新 Topic 且名称匹配前缀，系统会自动在 ES 创建索引并同步。

- 死信处理(错误隔离):对于 Unmarshal 失败的记录，日志记录后直接返回 nil 并提交位移。这防止了单条坏数据导致整个同步链路“卡死”的情况。

# 场景
用户下订单后，立即查看订单详情

架构:
```
[订单服务] ——> (Postgres 写库) ——> [CDC/Debezium] ——> Kafka ——> [报表服务/搜索服务] ——> (ES 读库)
```

流程：

1. 写：客户端发送 PlaceOrder 命令 → 应用服务 → 订单聚合执行校验、创建订单 → 保存到 MySQL（写库）→ 发布 OrderPlaced 事件。
2. 同步：事件通过 CDC 进入 Kafka，查询端消费事件，更新 ES 中的订单文档。
3. 读：客户端立即调用 GetOrder 查询接口 → 直接从 ES 读取订单详情。

# 技术栈
## Postgres 端
- WAL Level: 必须设置为 `logical`。
```postgresql.conf
wal_level = logical
```

- Publication: 建议手动创建或由 Debezium 自动创建以覆盖指定表。
```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: postgres-source-connector
  namespace: kafka
  labels:
    strimzi.io/cluster: my-connect-cluster
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  config:
    # 在该数据库内捕获所有变更
    schema.include.list: .*
    table.include.list: .*\..*
```

## Debezium / Kafka Connect
- Decimal Handling: 设置为 double 或 string(当前采用 double 以适配 ES 的 float 映射)。
- Unwrap SMT: 使用 io.debezium.transforms.ExtractNewRecordState 展平消息结构，简化下游解析逻辑。
- Topic Naming: 自动生成格式为 server.schema.table。
- Topic Naming:自动生成格式为 server.schema.table。

KafkaConnector配置文件:
```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: postgres-source-connector
  namespace: kafka
  labels:
    strimzi.io/cluster: my-connect-cluster
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  config:
    database.hostname: postgres-postgresql.postgres.svc
    database.port: 5432
    database.user: debezium_user
    database.password: password
    database.dbname: dbname
    topic.prefix: dbname_cdc
    plugin.name: pgoutput
    # 想捕获的表匹配的列表
    # 显式声明表和schema, 例如:
    # table.include.list: orders.order_main,orders.order_item
    # schema.include.list: public
    # 使用通配符, 例如: 按 schema 捕获: orders\..*,products\..*
    # schema.include.list: orders\..*,products\..*

    # 在该数据库内捕获所有变更
    schema.include.list: .*
    table.include.list: .*\..*

    # 数值处理 (解决 Base64 问题)
    # 将 Decimal 转换为 float64，这样 应用 读到就是数字，ES 也能直接接受
    decimal.handling.mode: "double"

    # 消息展平 SMT (解决结构解析问题)
    # 启用 Debezium 的 ExtractNewRecordState 转换器
    transforms: unwrap
    transforms.unwrap.type: io.debezium.transforms.ExtractNewRecordState
    # 删除记录时，在消息中添加 __deleted: true 标志
    transforms.unwrap.delete.handling.mode: rewrite
    # 发生更新时，如果数据没变是否丢弃 (默认 false)
    transforms.unwrap.drop.tombstones: true

    # 时间戳处理 (可选)
    # 将时间戳转为符合 ISO8601 的字符串，方便 ES 自动识别日期, 解决时间戳精度问题 (转换为 Connect 标准格式)
    time.precision.mode: connect

    # 确保序列化器不带冗余 Schema
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: false

```

KafkaConnect 配置文件:
```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnect
metadata:
  name: my-connect-cluster
  namespace: kafka
  annotations:
    # 关键注解: 启用 KafkaConnector CRD 来管理连接器
    strimzi.io/use-connector-resources: "true"
spec:
  version: 4.2.0
  replicas: 1
  bootstrapServers: my-cluster-kafka-bootstrap:9093
  # ... (TLS、存储Topic等配置)
  tls:
    trustedCertificates:
      - secretName: my-cluster-cluster-ca-cert
        pattern: "*.crt"
  groupId: my-connect-cluster
  configStorageTopic: my-connect-cluster-configs
  statusStorageTopic: my-connect-cluster-status
  offsetStorageTopic: my-connect-cluster-offsets
  config:
    # 将内部主题的复制因子设为与 kafka broker 数量一致
    config.storage.replication.factor: 1
    offset.storage.replication.factor: 1
    status.storage.replication.factor: 1
    # 其他可能需要的配置(例如，如果集群启用了 TLS，下面这些可以不用显式写)
    # security.protocol: SSL
    # ssl.truststore.type: PEM
    # ssl.truststore.location: ...
  build:
    output:
      type: docker
      # 替换为你自己的私有镜像仓库地址，用于推送构建好的镜像
      image: ccr.ccs.tencentyun.com/sumery/strimzi-connect-debezium:latest
      pushSecret: tcr-registry-secret
    plugins:
      - name: debezium-postgres-connector
        artifacts:
          - type: tgz
            url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.5.0.Final/debezium-connector-postgres-3.5.0.Final-plugin.tar.gz
            #sha512sum: 962a12151bdf9a5a30627eebac739955a4fd95a08d373b86bdcea2b4d0c27dd6e1edd5cb548045e115e33a9e69b1b2a352bee24df035a0447cb820077af00c03
```
当使用需要账号密码登录的容器镜像仓库服务(Container Registry)时, 使用:
```shell
kubectl create secret docker-registry tcr-registry-secret \
  --docker-server= \
  --docker-username= \
  --docker-password= \
  -n kafka

```

## Go Consumer (Sync Service)
- Kafka Lib: [github.com/segmentio/kafka-go](https://github.com/segmentio/kafka-go)。
- ES Lib: [github.com/elastic/go-elasticsearch/v9](https://github.com/elastic/go-elasticsearch/v9)。

Bulk Indexing: 采用 esutil.BulkIndexer 提高写入吞吐，配置 10s 或 5MB 的刷新阈值。

Index Auto-Creation: 内部逻辑根据 Topic 映射自动创建 ecommerce_ 前缀索引。

运行
```shell
# Kafka 地址
export KAFKA_BROKERS=192.168.3.120:9092
# 组ID
export KAFKA_GROUP_ID=es-sync-group
# 消息前缀
export KAFKA_TOPIC_PREFIX=ecommerce_cdc.

# Elasticsearch 配置
export ES_ADDRESSES=http://localhost:9200
export ES_USERNAME=elastic
export ES_PASSWORD=XwjLbwoaCLvuJ7PwaAWtBkNO
export ES_INDEX_PREFIX=ecommerce_

# 启动程序
go run cmd/main.go
```

## 全量重新索引
当需要从数据库全量同步数据到 ES 时，可启用全量重新索引模式。这在以下场景特别有用：
- 首次部署或初始化 ES 数据
- ES 数据丢失需要从数据库重建
- 数据一致性问题需要强制全量同步

```shell
# 全量同步模式（从头写入 ES）
export REINDEX_MODE=true
export REINDEX_TOPICS=orders.order_main,orders.order_item,products.skus

# 数据库连接配置
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=your_password
export DB_NAME=ecommerce

# 启动程序（全量同步完成后自动退出）
go run cmd/main.go
```

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| REINDEX_MODE | 是否启用全量同步模式，设为 `true` 启用 | `false` |
| REINDEX_TOPICS | 需要同步的表，格式：`schema.table`，多个用逗号分隔 | 空 |
| DB_HOST | 数据库主机地址 | `localhost` |
| DB_PORT | 数据库端口 | `5432` |
| DB_USER | 数据库用户名 | `postgres` |
| DB_PASSWORD | 数据库密码 | 空 |
| DB_NAME | 数据库名称 | `postgres` |

**注意**：
- 全量同步完成后程序会自动退出，不会启动实时同步
- 使用幂等写入，会覆盖 ES 中已存在的同名 ID 文档
- 建议在低峰期执行全量同步，避免对生产数据库造成压力

## 数据流转对照表

| 环节            | 数据形态示例                             | 说明                 |
|---------------|------------------------------------|--------------------|
| Postgres      | total_amount: 650.50 (numeric)     | 原始高精度数值            |
| Kafka         | "{""total_amount"": 650.5...}"     | 已解开 after 包装的 JSON |
| Go Client     | map[string]interface{}             | 经过逻辑解析与校验          |
| Elasticsearch | total_amount: 650.5 (float/double) | 最终可检索数值            |

# 验证与调试
## 检查 Kafka 消息
使用 Strimzi 临时容器检查 Topic 中是否存在数据: 

```Bash
kubectl -n kafka run kafka-inspector -ti --image=quay.io/strimzi/kafka:latest-kafka-3.7.0 --rm=true --restart=Never -- \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic ecommerce_cdc.orders.order_main --from-beginning
```

## Kibana Dev Tools 验证
检查索引 Mapping 与文档计数: 

```HTTP
# 1. 检查索引列表
GET _cat/indices/ecommerce_orders_*?v

# 2. 检查字段类型(确保不是 Base64 字符串)
GET ecommerce_orders_order_item/_mapping

# 3. 搜索最新同步的数据
GET ecommerce_orders_order_item/_search
{
"query": { "match_all": {} },
"sort": [{ "created_at": "desc" }]
}
```
## 常见问题排查 (Troubleshooting)
- docs.count 为 0:
  - 检查 Go 消费者的 GROUP_ID 是否正确(尝试更换 ID 触发重读)。

  - 检查 BulkIndexer 是否执行了 Close() 以冲刷缓冲区。

- Parsing Error (Base64 Issue):

    - [若看到 total_amount 为 BPWI 等字符串，请检查 Debezium 配置是否包含 decimal.handling.mode: double。
    
    - 清理旧索引: DELETE ecommerce_orders_*。]()

- Yellow 状态:

- 单节点开发环境下副本分片无法分配，属正常现象。可通过设置 number_of_replicas: 0 修复。

# 最后说明 
本文档由工程实践总结得出。在生产环境中，建议开启 Kafka 的 TLS 认证及 Elasticsearch 的 RBAC 权限控制。

文档分散在各处:
1. strimzi kafka 安装和配置: https://github.com/lens077/cloud-native-deploy/kafka/strimzi-kafka
2. strimzi kafka单机版示例配置文件: https://github.com/lens077/cloud-native-deploy/kafka/strimzi-kafka/examples/kafka-single-node.yml
3. Kafka Connect和Kafka Connector示例配置文件: https://github.com/lens077/cloud-native-deploy/kafka/strimzi-kafka/connect/examples
4. postgres 安装和配置: https://github.com/lens077/cloud-native-deploy//postgres/default/openebs
5. elastic-stack 安装和配置: https://github.com/lens077/cloud-native-deploy/elastic-stack
