package utils

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

var rc redis.Conn

func connect() {
	if rc != nil {
		return
	}

	fmt.Printf("ns=utils-lock fn=connect at=start\n")

	var err error
	host, password, err := ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	rc, err = redis.Dial("tcp", host)
	if err != nil {
		log.Fatalf("Locking service is unable to connect to redis. err: %s",
			err)
	}
	rc.Do("AUTH", password)

	fmt.Printf("ns=utils-lock fn=connect at=finish\n")
}

func disconnect() {
	fmt.Printf("ns=utils-lock fn=disconnect at=start\n")
	rc.Close()
	rc = nil
	fmt.Printf("ns=utils-lock fn=disconnect at=finish\n")
}

func init() {
	connect()
}

func UnlockPartition(key string) {
	rc.Do("DEL", key)
}

func LockPartition(ns string, max, ttl uint64) (uint64, error) {
	for {
		for p := uint64(0); p < max; p++ {
			name := fmt.Sprintf("lock.%s.%d", ns, p)
			locked, err := writeLock(name, ttl)
			if err != nil {
				return 0, err
			}
			if locked {
				return p, nil
			}
		}
		time.Sleep(time.Second * 5)
	}
	return 0, errors.New("LockPartition impossible broke the loop.")
}

func writeLock(name string, ttl uint64) (bool, error) {
	connect()
	new := time.Now().Unix() + int64(ttl) + 1
	old, err := redis.Int(rc.Do("GETSET", name, new))
	// If the ErrNil is present, the old value is set to 0.
	if err != nil && err != redis.ErrNil && old == 0 {
		disconnect()
		return false, err
	}
	// If the new value is greater than the old
	// value, then the old lock is expired.
	return new > int64(old), nil
}
