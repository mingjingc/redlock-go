package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/mingjingc/redlock-go"
)

func main() {
	redisClients := []*redis.Client{}
	c := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	redisClients = append(redisClients, c)
	c = redis.NewClient(&redis.Options{
		Addr: ":6380",
	})
	redisClients = append(redisClients, c)
	c.Close()

	c = redis.NewClient(&redis.Options{
		Addr: ":6381",
	})
	redisClients = append(redisClients, c)

	dml := redlock.New(redisClients)
	dml.Lock("lock", 1000*20)
	time.Sleep(time.Second * 20)
	res := dml.Unlock("lock")
	if !res {
		fmt.Println("解锁失败")
	}
}
