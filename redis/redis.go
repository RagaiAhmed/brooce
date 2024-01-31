//go:build !cluster
// +build !cluster

package redis

import (
	"log"
	"sync"
	"time"

	"brooce/config"
	"context"
	"github.com/redis/go-redis/v9"
)

var Ctx = context.Background()

var redisClient *redis.Client
var once sync.Once

func Get() *redis.Client {
	once.Do(func() {
		threads := len(config.Threads) + 10

		network := "tcp"
		addr := config.Config.Redis.Host
		if config.Config.Redis.Socket != "" {
			network = "unix"
			addr = config.Config.Redis.Socket
		}

		redisClient = redis.NewClient(&redis.Options{
			Network:      network,
			Addr:         addr,
			Password:     config.Config.Redis.Password,
			MaxRetries:   10,
			PoolSize:     threads,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 5 * time.Second,
			PoolTimeout:  1 * time.Second,
			DB:           config.Config.Redis.DB,
		})

		err := redisClient.Ping(Ctx).Err()
		if err != nil {
			log.Fatalln("Can't reach redis at", addr, "-- are your redis addr and password right?", err)
		}

		log.Println("Connected to redis at", addr)
	})

	return redisClient
}

// in the past, this function would just keep running RPOPLPUSH until it got an error back
// this works until the list gets long: then you can get into a race where the delayed list
// is being both flushed and repopulated (by a worker thread) forever
func FlushList(src, dst string) error {
	redisClient := Get()
	length, err := redisClient.LLen(Ctx, src).Result()
	if err != nil {
		return err
	}

	for i := int64(0); i < length; i++ {
		_, err = redisClient.RPopLPush(Ctx, src, dst).Result()
		if err != nil {
			break
		}
	}

	if err == redis.Nil {
		err = nil
	}

	return err
}

func ScanKeys(match string) (keys []string, err error) {
	redisClient := Get()

	iter := redisClient.Scan(Ctx, 0, match, 10000).Iterator()
	for iter.Next(Ctx) {
		keys = append(keys, iter.Val())
	}
	err = iter.Err()

	return
}
