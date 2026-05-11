package reindex

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"

	"postgres-kafka-es-streaming-pipeline/internal/conf"
	"postgres-kafka-es-streaming-pipeline/internal/es"
	"postgres-kafka-es-streaming-pipeline/pkg"

	_ "github.com/lib/pq"
)

type Reindexer struct {
	db      *sql.DB
	indexer *es.Indexer
	config  *conf.Config
}

func NewReindexer(cfg *conf.Config, esClient *elasticsearch.TypedClient) (*Reindexer, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	indexer, err := es.NewIndexer(esClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexer: %w", err)
	}

	return &Reindexer{db: db, indexer: indexer, config: cfg}, nil
}

func (r *Reindexer) Close() {
	if r.db != nil {
		r.db.Close()
	}
	if r.indexer != nil {
		r.indexer.Close()
	}
}

func (r *Reindexer) FullReindex(ctx context.Context) error {
	topicsStr := r.config.ReindexTopics
	if len(topicsStr) == 0 {
		log.Println("No topics specified for reindexing, skipping")
		return nil
	}
	topics := strings.Split(topicsStr, ",")
	log.Printf("Starting full reindex for topics: %v", topics)

	for _, topic := range topics {
		if err := r.reindexTable(ctx, topic); err != nil {
			log.Printf("Failed to reindex topic %s: %v", topic, err)
			return err
		}
	}

	log.Println("Full reindex completed successfully")
	return nil
}

func (r *Reindexer) reindexTable(ctx context.Context, topic string) error {
	parts := strings.Split(topic, ".")
	if len(parts) != 2 {
		return fmt.Errorf("invalid topic format: %s, expected schema.table", topic)
	}
	schema := parts[0]
	table := parts[1]

	indexName := pkg.TopicToIndex(
		r.config.KafkaTopicPrefix+topic,
		r.config.KafkaTopicPrefix,
		r.config.ESIndexPrefix,
	)

	log.Printf("Reindexing table %s.%s to index %s", schema, table, indexName)

	query := fmt.Sprintf("SELECT * FROM %s.%s", schema, table)
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query table %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	count := 0
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		doc := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			switch v := val.(type) {
			case []byte:
				// 如果是数字或JSON字符串，转为 string 避免 Base64
				// 建议增加逻辑判断，或者直接 string(v)
				doc[col] = string(v)
			default:
				doc[col] = v
			}
		}

		if err := r.indexer.IndexDocument(ctx, indexName, doc); err != nil {
			log.Printf("Error indexing document: %v", err)
			continue
		}

		count++
		if count%1000 == 0 {
			log.Printf("Indexed %d documents for %s.%s", count, schema, table)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	log.Printf("Finished reindexing %d documents for %s.%s", count, schema, table)
	return nil
}
