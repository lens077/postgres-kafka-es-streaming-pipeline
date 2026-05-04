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
	}, nil
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
