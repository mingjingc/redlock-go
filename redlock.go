package redlock

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

type RedLock struct {
	uniqueValue      interface{}
	quorum           int
	clockDriftFactor float64 //误差因子,如：精确到0.01 ms
	tryCount         int

	redisClients []*redis.Client
}

func New(redisClients []*redis.Client) *RedLock {
	return &RedLock{
		redisClients:     redisClients,
		quorum:           2,
		tryCount:         1,
		clockDriftFactor: 0.01,
	}
}

func (r *RedLock) Lock(lockName string, ttl int64) bool {
	drift := int64(float64(ttl)*r.clockDriftFactor) + 2
	for i := 0; i < r.tryCount; i++ {
		t1 := time.Now().Unix() * 1000
		r.uniqueValue = r.generateUniqueValue()
		n := r.getLock(lockName, r.uniqueValue, ttl)
		t2 := time.Now().Unix() * 1000
		minValidity := ttl - (t2 - t1) - drift
		if minValidity > 0 && n > r.quorum {
			return true
		} else {
			r.Unlock(lockName)
		}
		time.Sleep(delay())
	}
	return false
}

var deleteLockScript = `
	if redis.call("get",KEYS[1]) == ARGV[1] then
		return redis.call("del",KEYS[1])
	else
		return 0
	end
`

func (r *RedLock) Unlock(lockName string) bool {
	success := true
	for _, redisClient := range r.redisClients {
		res, err := redisClient.Eval(deleteLockScript, []string{lockName}, r.uniqueValue).Result()
		if err != nil {
			fmt.Println(err)
		}
		if res == 0 {
			success = false
		}
	}
	return success
}

func (r *RedLock) getLock(lockName string, uniqueValue interface{}, ttl int64) int {
	n := 0
	resultCh := make(chan bool, len(r.redisClients))
	for _, redisClient := range r.redisClients {
		go func(redisClient *redis.Client) {
			fmt.Println(redisClient.Options().Addr)
			res, err := redisClient.SetNX(lockName, uniqueValue,
				time.Duration(ttl*int64(time.Millisecond))).Result()
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(res)
			resultCh <- res
		}(redisClient)
	}
	for i := 0; i < len(r.redisClients); i++ {
		if <-resultCh {
			n++
		}
	}
	return n
}

func (r *RedLock) generateUniqueValue() uint32 {
	return uuid.New().ID()
}

func delay() time.Duration {
	return time.Millisecond*time.Duration(rand.Int63n(100)) + 1
}
