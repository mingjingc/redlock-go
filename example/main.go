package main

import (
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/mingjingc/redlock-go"
)

func main() {
	dml := redlock.New(redis.NewClient(&redis.Options{
		Addr: ":6379",
	}), redis.NewClient(&redis.Options{
		Addr: ":6380",
	}), redis.NewClient(&redis.Options{
		Addr: ":6381",
	}))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			if mylock, ok := dml.Lock("foo", 50); ok {
				time.Sleep(time.Millisecond * 40)
				dml.Unlock(mylock)
			}
			defer wg.Done()
		}()
	}
	wg.Wait()
}
