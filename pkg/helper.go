package pkg

import "strings"

// TopicToIndex 将 Kafka Topic 转换为 ES 索引名
func TopicToIndex(topic, prefix, targetPrefix string) string {
	s := topic
	if prefix != "" {
		s = strings.TrimPrefix(topic, prefix)
	}
	// 替换点号为下划线，符合 ES 命名规范
	return targetPrefix + strings.ReplaceAll(s, ".", "_")
}
