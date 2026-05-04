package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/elastic/go-elasticsearch/v9"

	"postgres-kafka-es-streaming-pipeline/internal/conf"
	"postgres-kafka-es-streaming-pipeline/internal/es"
	"postgres-kafka-es-streaming-pipeline/internal/kafka"
)

func main() {
	cfg, _ := conf.Load()

	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: cfg.ESAddresses,
		Username:  cfg.ESUsername,
		Password:  cfg.ESPassword,
	})
	if err != nil {
		log.Fatalf("ES Init Error: %v", err)
	}

	indexer, _ := es.NewIndexer(esClient)

	// 业务关注的 Topic
	topics := []string{
		"ecommerce_cdc.orders.order_main",
		"ecommerce_cdc.orders.order_item",
		"ecommerce_cdc.products.skus",
		"ecommerce_cdc.products.spus",
		"ecommerce_cdc.products.sale_detail",
	}

	consumer := kafka.NewConsumer(cfg, topics, indexer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 捕获退出信号
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		<-stop
		cancel()
	}()

	log.Println("Sync Service Running...")
	if err := consumer.Run(ctx); err != nil {
		log.Printf("Execution stopped: %v", err)
	}

	// 优雅停机：确保 Bulk 缓冲区清空
	log.Println("Shutting down and flushing buffer...")
	indexer.Close()
}
