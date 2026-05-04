package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"

	// 注意：请将此处路径替换为你项目中 es 包的真实导入路径
	"postgres-kafka-es-streaming-pipeline/internal/es"
)

func TestElasticsearchV9Full(t *testing.T) {
	ctx := context.Background()
	indexName := "test_sale_details_v9"

	// 1. 初始化客户端
	// 使用你提供的密码和配置
	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{"http://127.0.0.1:9200"}, // 确保地址正确
		Username:  "elastic",
		Password:  "XwjLbwoaCLvuJ7PwaAWtBkNO",
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %s", err)
	}

	// 2. 预先检查并创建索引 (防止 404)
	fmt.Printf("检查并创建索引: %s\n", indexName)
	exists, err := client.Indices.Exists(indexName).Do(ctx)
	if err != nil {
		t.Fatalf("检查索引是否存在失败: %v", err)
	}

	if !exists {
		// 模拟 Postgres 表结构映射到 ES
		_, err := client.Indices.Create(indexName).
			Mappings(&types.TypeMapping{
				Properties: map[string]types.Property{
					"id":           types.NewIntegerNumberProperty(),
					"order_no":     types.NewKeywordProperty(),
					"total_amount": types.NewFloatNumberProperty(),
					"type":         types.NewKeywordProperty(),
					"created_at":   types.NewDateProperty(),
				},
			}).Do(ctx)
		if err != nil {
			t.Fatalf("创建索引失败: %v", err)
		}
		fmt.Println("索引创建成功")
	}

	// 3. 初始化 Indexer (调用 internal/es 中的 NewIndexer)
	idx, err := es.NewIndexer(client)
	if err != nil {
		t.Fatalf("初始化 Indexer 失败: %v", err)
	}

	// 4. 准备模拟 Kafka 摄入的数据
	mockDocs := []map[string]interface{}{
		{
			"id":           101,
			"order_no":     "ORDER20260504001",
			"total_amount": 8999.00,
			"type":         "paid",
			"created_at":   time.Now().Format(time.RFC3339),
		},
		{
			"id":           102,
			"order_no":     "ORDER20260504002",
			"total_amount": 888.00,
			"type":         "paid",
			"created_at":   time.Now().Format(time.RFC3339),
		},
	}

	// 5. 执行写入
	fmt.Println("正在通过 BulkIndexer 写入数据...")
	for _, d := range mockDocs {
		if err := idx.IndexDocument(ctx, indexName, d); err != nil {
			t.Errorf("写入失败: %v", err)
		}
	}

	// 7. 关键步骤：显式触发 ES 刷新
	// 强制让刚才写入的数据变为可搜索状态
	_, err = client.Indices.Refresh().Index(indexName).Do(ctx)
	if err != nil {
		t.Fatalf("Refresh 失败: %v", err)
	}

	// 8. 验证搜索结果
	fmt.Println("正在验证写入结果...")
	_, err = client.Search().
		Index(indexName).
		Query(&types.Query{
			MatchAll: &types.MatchAllQuery{},
		}).
		Do(ctx)

	if err != nil {
		t.Fatalf("搜索失败: %v", err)
	}

	var total int64
	for i := 0; i < 3; i++ { // 最多重试 3 次
		res, err := client.Search().Index(indexName).Do(ctx)
		if err != nil {
			t.Fatalf("搜索失败: %v", err)
		}
		total = res.Hits.Total.Value
		if total == 2 {
			break
		}
		fmt.Printf("未找到全部文档 (当前: %d)，等待后重试...\n", total)
		time.Sleep(500 * time.Millisecond)
		client.Indices.Refresh().Index(indexName).Do(ctx)
	}
	fmt.Printf("验证成功：找到 %d 条文档\n", total)
	if total != 2 {
		t.Errorf("预期找到 2 条文档，实际获得 %d 条", total)
	}

	// 9. 验证删除功能
	fmt.Println("正在测试删除 ID: 101...")
	if err := idx.DeleteDocument(ctx, indexName, "101"); err != nil {
		t.Fatalf("调用删除接口失败: %v", err)
	}

	// 使用 Get API 实时验证 (Get 是实时的，不需要 Refresh)
	getRes, err := client.Get(indexName, "101").Do(ctx)
	// 在 v9 TypedClient 中，如果找不到文档，err 可能为 nil 但 getRes.Found 为 false
	if err == nil && getRes.Found {
		t.Error("文档 101 应该已被删除，但依然能查到")
	} else {
		fmt.Println("删除验证成功：文档已不存在")
	}
}
