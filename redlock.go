package redlock

import (
	"log"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

type LockIfnfo struct {
	validity int64
	resource string
	val      interface{}
}

func (l LockIfnfo) GetResource() string {
	return l.resource
}
func (l LockIfnfo) GetValidity() int64 {
	return l.validity
}
func (l LockIfnfo) GetVal() interface{} {
	return l.val
}

type RedLock struct {
	quorum int
	// redis master nodes' time accuracy, for example: 0.01 ms
	clockDriftFactor float64
	tryCount         int
	tryDelay         int64 // millisecond

	redisClients []*redis.Client
}

func New(redisClients ...*redis.Client) *RedLock {
	return &RedLock{
		redisClients:     redisClients,
		quorum:           len(redisClients)>>1 + 1,
		tryCount:         200,
		clockDriftFactor: 0.01,
		tryDelay:         200,
	}
}

func (r *RedLock) SetTryCount(tryCount int) {
	r.tryCount = tryCount
}
func (r *RedLock) SetClockDriftFactor(clockDriftFactor float64) {
	r.clockDriftFactor = clockDriftFactor
}
func (r *RedLock) SetQuorum(quorum int) {
	r.quorum = quorum
}
func (r *RedLock) SetTryDelay(tryDelay int64) {
	r.tryDelay = tryDelay
}

func (r *RedLock) Lock(resource string, ttl int64) (LockIfnfo, bool) {
	drift := int64(float64(ttl)*r.clockDriftFactor) + 2
	for i := 0; i < r.tryCount; i++ {
		t1 := time.Now().Unix() * 1000
		uniqueValue := r.generateUniqueValue()
		n := r.lockInstance(resource, uniqueValue, ttl)
		t2 := time.Now().Unix() * 1000
		validity := ttl - (t2 - t1) - drift
		if validity > 0 && n > r.quorum {
			return LockIfnfo{
				validity: validity,
				resource: resource,
				val:      uniqueValue,
			}, true
		} else {
			r.unlockInstance(resource, uniqueValue)
		}
		time.Sleep(r.delay())
	}
	return LockIfnfo{}, false
}

var deleteLockScript = `
	if redis.call("get",KEYS[1]) == ARGV[1] then
		return redis.call("del",KEYS[1])
	else
		return 0
	end
`
var resetLockTTLScript = `
	if redis.call("get",KEYS[1]) == ARGV[1] then
		return redis.call("pexpire",KEYS[1],ARGV[2])
	else
		return 0
	end
`

func (r *RedLock) unlockInstance(resource string, val interface{}) bool {
	success := true
	for _, redisClient := range r.redisClients {
		_, err := redisClient.Eval(deleteLockScript, []string{resource}, val).Result()
		if err != nil {
			log.Printf("unlock %s instance failed in %s", resource, redisClient.Options().Addr)
			success = false
		}
	}
	return success
}

func (r *RedLock) Unlock(lock LockIfnfo) bool {
	return r.unlockInstance(lock.resource, lock.val)
}

func (r *RedLock) ResetTTL(lock LockIfnfo, ttl int64) bool {
	resultCh := make(chan bool)
	n := 0
	for _, redisClient := range r.redisClients {
		go func(redisClient *redis.Client) {
			res, err := redisClient.Eval(resetLockTTLScript, []string{lock.resource}, lock.val, ttl).Result()
			if err != nil {
				res = 0
			}
			resultCh <- (res != 0)
		}(redisClient)
	}
	for i := 0; i < len(r.redisClients); i++ {
		if <-resultCh {
			n++
		}
	}
	return n >= r.quorum
}

func (r *RedLock) lockInstance(lockName string, uniqueValue interface{}, ttl int64) int {
	n := 0
	resultCh := make(chan bool)
	for _, redisClient := range r.redisClients {
		go func(redisClient *redis.Client) {
			res, err := redisClient.SetNX(lockName, uniqueValue,
				time.Duration(ttl*int64(time.Millisecond))).Result()
			if err != nil {
				res = false
			}
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

func (r *RedLock) delay() time.Duration {
	return time.Millisecond*time.Duration(rand.Int63n(r.tryDelay)) + 1
}
