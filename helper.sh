#!/usr/bin/env bash
# 启用 POSIX 模式并设置严格的错误处理机制
set -o posix errexit -o pipefail

# 启动一个临时消费者，看看 ecommerce_cdc.orders.order_item 里到底存了什么
kubectl -n kafka run kafka-inspector -ti --image=quay.io/strimzi/kafka:latest-kafka-3.7.0 --rm=true --restart=Never -- \
  bin/kafka-console-consumer.sh --bootstrap-server 192.168.3.120:9092 \
  --topic ecommerce_cdc.orders.order_item --from-beginning --max-messages 200
