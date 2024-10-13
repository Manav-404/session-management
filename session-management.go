package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var threshold int = 2
var ttl int64 = 86400000000000

var rdb *redis.Client
var ctx = context.Background()

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	fmt.Println("Successfully connected to redis...")
}

func NewSession(id string, metadata string) error {
	fmt.Println(fmt.Sprintf("Adding user for %s", id))

	currentTime := time.Now().Unix()

	setLength, err := rdb.ZCard(ctx, id).Result()
	if err != nil {
		return err
	}

	if setLength >= int64(threshold) {
		err = RemoveOldestSession(id)
		if err != nil {
			return err
		}
	}

	_, err = rdb.ZAdd(ctx, id, &redis.Z{
		Score:  float64(currentTime),
		Member: metadata,
	}).Result()

	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("Added new user session for %s", id))

	err = rdb.Expire(ctx, id, time.Duration(ttl)).Err()
	if err != nil {
		return err
	}

	return nil

}

func RemoveOldestSession(id string) error {
	fmt.Println(fmt.Sprintf("Removing old user session for %s", id))

	_, err := rdb.ZRemRangeByRank(ctx, id, 0, 0).Result()
	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("Removed old user session for %s", id))

	return nil
}

func main() {
	initRedis()

	for i := 0; i < 3; i++ {
		err := NewSession("Manav", fmt.Sprintf("deviceId-%s", i))
		if err != nil {
			fmt.Errorf("Error in adding a new user session", err)
		}
	}
}
