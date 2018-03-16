package main

import (
	r "MetricsNew/redis"
	"fmt"
)

func main() {

	if r.ExistValue(123, []interface{}{23123, 121212}) {
		fmt.Println("123123", " - old")
	} else {
		fmt.Println("123123", " - new")
	}
	if err := r.AddValue(123, []interface{}{23123, 121212}); err != nil {
		fmt.Println(err)
	}

	if err := r.RenameKey(123); err != nil {
		fmt.Println(err)
	}

}
