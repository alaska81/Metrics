// functions
package redis

import (
	"fmt"

	"github.com/go-redis/redis"
)

var rclient *redis.Client

func init() {
	if err := connect(); err != nil {
		fmt.Println(err)
	}

}

func connect() error {
	rclient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := rclient.Ping().Result()
	if err != nil {
		return fmt.Errorf("connect: %v", err)
	}

	return nil
}

func AddValue(key string, value string) error {
	err := rclient.SAdd(key, value).Err()
	if err != nil {
		return fmt.Errorf("AddValue: %v", err)
	}

	return nil
}

func ExistValue(key string, value string) bool {
	val := rclient.SIsMember(key, value).Val()

	return val
}

func DelKey(key string) error {
	err := rclient.Del(key).Err()
	if err != nil {
		return fmt.Errorf("DelKey: %v", err)
	}

	return nil
}

func RenameKey(key string, newkey string) error {
	err := rclient.Rename(key, newkey).Err()
	if err != nil {
		return fmt.Errorf("RenameKey: %v", err)
	}

	return nil
}
