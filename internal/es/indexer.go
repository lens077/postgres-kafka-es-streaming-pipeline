package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esutil"
)

type Indexer struct {
	es           *elasticsearch.TypedClient
	bi           esutil.BulkIndexer
	checkedIndex sync.Map // 缓存已确认存在的索引名
}

func NewIndexer(es *elasticsearch.TypedClient) (*Indexer, error) {
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        &es.BaseClient,
		NumWorkers:    4,
		FlushBytes:    5e+6,
		FlushInterval: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create bulk indexer: %w", err)
	}

	return &Indexer{es: es, bi: bi}, nil
}

// EnsureIndex 动态检查并创建索引（含基础 Mapping）
func (i *Indexer) EnsureIndex(ctx context.Context, indexName string) {
	if _, ok := i.checkedIndex.Load(indexName); ok {
		return
	}

	exists, _ := i.es.Indices.Exists(indexName).Do(ctx)
	if !exists {
		log.Printf("Dynamic creation: index %s not found, creating...", indexName)
		// 注意：实际生产中建议配合 ES Index Template 使用，此处仅做兜底创建
		_, err := i.es.Indices.Create(indexName).Do(ctx)
		if err != nil {
			log.Printf("Error creating index %s: %v", indexName, err)
		}
	}
	i.checkedIndex.Store(indexName, true)
}

func (i *Indexer) IndexDocument(ctx context.Context, index string, doc map[string]interface{}) error {
	i.EnsureIndex(ctx, index)

	id, ok := doc["id"]
	if !ok {
		return fmt.Errorf("document missing primary key 'id'")
	}

	body, _ := json.Marshal(doc)
	return i.bi.Add(ctx, esutil.BulkIndexerItem{
		Action:     "index", // 幂等操作：存在则覆盖，不存在则创建
		Index:      index,
		DocumentID: fmt.Sprint(id),
		Body:       bytes.NewReader(body),
		OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
			log.Printf("Bulk Error [%s]: %s", res.Error.Type, res.Error.Reason)
		},
	})
}

func (i *Indexer) DeleteDocument(ctx context.Context, index string, docID string) error {
	i.EnsureIndex(ctx, index)
	_, err := i.es.Delete(index, docID).Do(ctx)
	return err
}

func (i *Indexer) Close() error {
	return i.bi.Close(context.Background())
}
