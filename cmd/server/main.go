package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/elastic/go-elasticsearch/v9"

	"postgres-kafka-es-streaming-pipeline/internal/conf"
	"postgres-kafka-es-streaming-pipeline/internal/es"
	"postgres-kafka-es-streaming-pipeline/internal/kafka"
	"postgres-kafka-es-streaming-pipeline/internal/reindex"
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

	if cfg.ReindexMode {
		log.Println("Reindex mode enabled, starting full reindex...")
		reindexer, err := reindex.NewReindexer(cfg, esClient)
		if err != nil {
			log.Fatalf("Reindexer Init Error: %v", err)
		}
		defer reindexer.Close()

		if err := reindexer.FullReindex(context.Background()); err != nil {
			log.Fatalf("Full reindex failed: %v", err)
		}
		log.Println("Full reindex completed, exiting...")
		return
	}

	indexer, _ := es.NewIndexer(esClient)

	// topics := []string{
	// 	"ecommerce_cdc.orders.order_main",
	// 	"ecommerce_cdc.orders.order_item",
	// 	"ecommerce_cdc.orders.order_log",
	// 	"ecommerce_cdc.products.skus",
	// 	"ecommerce_cdc.products.spus",
	// 	"ecommerce_cdc.products.sale_detail",
	// }
	topicsStr := cfg.ReindexTopics
	if len(topicsStr) == 0 {
		log.Println("No topics specified for reindexing, skipping")
	}
	topics := strings.Split(topicsStr, ",")
	log.Printf("Starting full reindex for topics: %v", topics)
	consumer := kafka.NewConsumer(cfg, topics, indexer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	log.Println("Shutting down and flushing buffer...")
	indexer.Close()
}
