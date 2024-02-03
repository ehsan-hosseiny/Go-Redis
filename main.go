package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

//go:embed foods
var foodTags string

func main() {
	rdb := redis.NewClient(&redis.Options{})
	rdb.FlushAll(context.Background())
	importTags(rdb)
	listByTag(rdb, "healthy", "vegetable")
}

func importTags(rdb *redis.Client) {
	tags := strings.Split(strings.TrimSpace(foodTags), "\n")
	for _, row := range tags {
		items := strings.Split(row, " ")
		if len(items) != 3 {
			log.Fatalf("bad row: %s\n", row)
		}
		score, _ := strconv.Atoi(strings.TrimSpace(items[2]))
		if err := rdb.ZAdd(context.Background(), fmt.Sprintf("tag:%s", items[0]), redis.Z{
			Score:  float64(score),
			Member: items[1],
		}).Err(); err != nil {
			panic(err)
		}
	}
	log.Println("added all rows")
}

func listByTag(rdb *redis.Client, tags ...string) {
	sort.Strings(tags)
	key := "tag:" + strings.Join(tags, ":")
	var keys []string
	for _, tag := range tags {
		keys = append(keys, "tag:"+tag)
	}
	// this tag does not exist
	if rdb.Exists(context.Background(), key).Val() == 0 {
		log.Printf("%s does not exist, calling ZINTERSTORE\n", key)
		if err := rdb.ZInterStore(context.Background(), key, &redis.ZStore{
			Keys:      keys,
			Aggregate: "SUM",
		}).Err(); err != nil {
			log.Printf("error while creating %s: %v\n", keys, err)
			return
		}
		log.Printf("%s was created\n", key)
	}
	result, _ := rdb.ZRevRangeWithScores(context.Background(), key, 0, 10).Result()
	for _, z := range result {
		log.Printf("[%2.0f]  %s\n", z.Score, z.Member)
	}
}
