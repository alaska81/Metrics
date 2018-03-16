// functions
package redis

import (
	"fmt"
	"log"

	"MetricsNew/config"

	"github.com/go-redis/redis"
)

var rclient *redis.Client

func init() {
	if err := connect(); err != nil {
		fmt.Println(err)
		log.Println(err)
	}

}

func connect() error {
	rclient = redis.NewClient(&redis.Options{
		Addr:     config.Config.Redis_Addr,
		Password: config.Config.Redis_Password, // no password set
		DB:       config.Config.Redis_DB,       // use default DB
	})

	_, err := rclient.Ping().Result()
	if err != nil {
		return fmt.Errorf("Redis connect: %v", err)
	}

	fmt.Printf("Redis init (%s)...\n", config.Config.Redis_Addr)

	return nil
}

func AddValueInTmp(key interface{}, value interface{}) error {
	err := rclient.SAdd(fmt.Sprint(key)+"_tmp", fmt.Sprint(value)).Err()
	if err != nil {
		return fmt.Errorf("AddValue: %v", err)
	}

	return nil
}

func ExistValue(key interface{}, value interface{}) bool {
	val := rclient.SIsMember(fmt.Sprint(key), fmt.Sprint(value)).Val()

	return val
}

//func DelKey(key string) error {
//	err := rclient.Del(key).Err()
//	if err != nil {
//		return fmt.Errorf("DelKey: %v", err)
//	}

//	return nil
//}

func SwitchKeys(key interface{}) error {
	if rclient.Exists(fmt.Sprint(key)+"_tmp").Val() == 1 {
		err := rclient.Rename(fmt.Sprint(key)+"_tmp", fmt.Sprint(key)).Err()
		if err != nil {
			return fmt.Errorf("RenameKey: %v", err)
		}
	}

	return nil
}
