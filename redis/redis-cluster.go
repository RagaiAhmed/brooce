//go:build cluster
// +build cluster

package redis

import (
	"log"
	"strings"
	"sync"
	"time"

	"brooce/config"
	"context"
	"github.com/redis/go-redis/v9"
)

var Ctx = context.Background()

var redisClient *redis.ClusterClient
var once sync.Once

func Get() *redis.ClusterClient {
	once.Do(func() {
		threads := len(config.Threads) + 10

		redisClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        strings.Split(config.Config.Redis.Host, ","),
			Password:     config.Config.Redis.Password,
			MaxRetries:   10,
			PoolSize:     threads,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 5 * time.Second,
			PoolTimeout:  1 * time.Second,
		})

		for {
			err := redisClient.Ping(Ctx).Err()
			if err == nil {
				break
			}
			log.Println("Can't reach redis at", config.Config.Redis.Host, "-- are your redis addr and password right?", err)
			time.Sleep(5 * time.Second)
		}
	})

	return redisClient
}

func FlushList(src, dst string) (err error) {
	redisClient := Get()
	for err == nil {
		_, err = redisClient.RPopLPush(Ctx, src, dst).Result()
	}

	if err == redis.Nil {
		err = nil
	}

	return
}

func ScanKeys(match string) (keys []string, err error) {
	redisClient := Get()

	err = redisClient.ForEachMaster(Ctx, func(ctx context.Context, client *redis.Client) error {
		iter := client.Scan(ctx, 0, match, 10000).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		return iter.Err()
	})

	return
}
