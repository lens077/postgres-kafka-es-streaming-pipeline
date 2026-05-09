package conf

import (
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers []string
	KafkaGroupID string
	// KafkaTopicPrefix 用于过滤和处理 Topic 名，例如 "ecommerce_cdc."
	KafkaTopicPrefix string

	ESAddresses []string
	ESUsername  string
	ESPassword  string
	// ESIndexPrefix 转换后的 ES 索引前缀，例如 "ecommerce_"
	ESIndexPrefix string

	// 全量重新索引配置
	ReindexMode bool
	// ReindexTopics 指定需要全量同步的表，格式: schema.table,schema.table2
	ReindexTopics string

	// 数据库连接配置
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
}

func Load() (*Config, error) {
	return &Config{
		KafkaBrokers:     getEnvSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "es-sync-consumer"),
		KafkaTopicPrefix: getEnv("KAFKA_TOPIC_PREFIX", "ecommerce_cdc."),

		ESAddresses:   getEnvSlice("ES_ADDRESSES", []string{"http://localhost:9200"}),
		ESUsername:    getEnv("ES_USERNAME", "elastic"),
		ESPassword:    getEnv("ES_PASSWORD", ""),
		ESIndexPrefix: getEnv("ES_INDEX_PREFIX", "ecommerce_"),

		ReindexMode:   getEnvBool("REINDEX_MODE", false),
		ReindexTopics: getEnv("REINDEX_TOPICS", "ecommerce_cdc.orders.order_main,ecommerce_cdc.orders.order_item,ecommerce_cdc.orders.order_log,ecommerce_cdc.products.skus,ecommerce_cdc.products.spus,ecommerce_cdc.products.sale_detail"),

		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "5432"),
		DBUser:     getEnv("DB_USER", "postgres"),
		DBPassword: getEnv("DB_PASSWORD", ""),
		DBName:     getEnv("DB_NAME", "postgres"),
	}, nil
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "true" || v == "1"
	}
	return fallback
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvSlice(key string, fallback []string) []string {
	if v := os.Getenv(key); v != "" {
		return strings.Split(v, ",")
	}
	return fallback
}
