package main

import (
	r "MetricsNew/redis"
	"fmt"
)

func main() {

	if r.ExistValue("123", "123123") {
		fmt.Println("123123", " - old")
	} else {
		fmt.Println("123123", " - new")
	}
	if err := r.AddValue("123_tmp", "123123"); err != nil {
		fmt.Println(err)
	}

	if r.ExistValue("123", "222222") {
		fmt.Println("222222", " - old")
	} else {
		fmt.Println("222222", " - new")
	}
	if err := r.AddValue("123_tmp", "222222"); err != nil {
		fmt.Println(err)
	}

	if r.ExistValue("123", "321321") {
		fmt.Println("321321", " - old")
	} else {
		fmt.Println("321321", " - new")
	}
	if err := r.AddValue("123_tmp", "321321"); err != nil {
		fmt.Println(err)
	}

	if err := r.RenameKey("123_tmp", "123"); err != nil {
		fmt.Println(err)
	}

}
