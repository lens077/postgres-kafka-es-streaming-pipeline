package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"postgres-kafka-es-streaming-pipeline/internal/conf"
	"postgres-kafka-es-streaming-pipeline/internal/es"
	"postgres-kafka-es-streaming-pipeline/pkg"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader  *kafka.Reader
	indexer *es.Indexer
	config  *conf.Config
}

func NewConsumer(cfg *conf.Config, topics []string, indexer *es.Indexer) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		GroupID:        cfg.KafkaGroupID,
		GroupTopics:    topics,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // 严格手动提交，保证至少消费一次
	})
	return &Consumer{reader: reader, indexer: indexer, config: cfg}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if err == io.EOF || err == context.Canceled {
				return nil
			}
			return err
		}

		// 处理消息并根据结果决定是否提交 Offset
		if err := c.processMessage(ctx, msg); err != nil {
			log.Printf("Critical process error (Offset %d): %v", msg.Offset, err)
			// 策略：如果不是数据格式问题，可以考虑暂不提交，程序重启后重试
			continue
		}

		// 成功存入 BulkIndexer 队列后提交 Offset
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Offset commit failed: %v", err)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	if len(msg.Value) == 0 {
		return nil
	}

	indexName := pkg.TopicToIndex(msg.Topic, c.config.KafkaTopicPrefix, c.config.ESIndexPrefix)

	// 1. 将整条消息解析为 map
	var record map[string]interface{}
	if err := json.Unmarshal(msg.Value, &record); err != nil {
		return nil
	}
	// 因为启用了 unwrap，record 就是数据本身
	// 如果是删除，unwrap 会加上 __deleted 字段
	if deleted, ok := record["__deleted"].(string); ok && deleted == "true" {
		return c.indexer.DeleteDocument(ctx, indexName, fmt.Sprint(record["id"]))
	}

	// var record map[string]interface{}
	// if err := json.Unmarshal(msg.Value, &record); err != nil {
	// 	log.Printf("Unmarshal failed at Offset %d: %v", msg.Offset, err)
	// 	return nil // 坏数据跳过，不阻塞 Offset
	// }

	// 2. 鲁棒性检查：判断是“展平格式”还是“标准 Debezium 格式”
	// 检查是否存在 payload 字段，如果存在则提取 after
	// if payload, ok := record["payload"].(map[string]interface{}); ok {
	// 	if after, ok := payload["after"].(map[string]interface{}); ok {
	// 		record = after
	// 	} else if op, ok := payload["op"].(string); ok && op == "d" {
	// 		// 处理删除逻辑
	// 		if before, ok := payload["before"].(map[string]interface{}); ok {
	// 			id := before["id"]
	// 			return c.indexer.DeleteDocument(ctx, indexName, fmt.Sprint(id))
	// 		}
	// 	}
	// }

	// 3. 检查是否有 ID 字段（此时 record 已经是展平后的数据了）
	if _, ok := record["id"]; !ok {
		log.Printf("Warning: No ID found in message at Offset %d, raw: %s", msg.Offset, string(msg.Value))
		return nil
	}

	// 4. 写入 ES
	return c.indexer.IndexDocument(ctx, indexName, record)
}
