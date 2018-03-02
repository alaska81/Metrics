// functions
package redis

import (
	"fmt"

	"github.com/go-redis/redis"
)

var rclient *redis.Client

func init() {
	connect()

}

func connect() error {
	rclient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := rclient.Ping().Result()
	fmt.Println(pong, err)

	return err
}

func pushListValue(key string, value string) {
	err := rclient.RPush(key, value).Err()
	if err != nil {
		panic(err)
	}
}

func pushHashValue(key string, field string, value interface{}) {
	err := rclient.HSet(key, field, value).Err()
	if err != nil {
		panic(err)
	}
}

func getHashValue(key string, field string) {
	val := rclient.HExists(key, field)

	return val
}
