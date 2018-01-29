package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var Config Configurations
var allPath string

type Configurations struct {
	Tls_connect string
	Count_page  int64
	Word        string
}

func getPathFile() { //узнаем путь до данного файла
	filepath.Walk("../config", func(path string, info os.FileInfo, err error) error {
		bol := strings.HasSuffix(path, "UserConfig.conf")
		if bol {
			allPath = path
			fmt.Println("TRUE:", path)
			return nil
		} else {
			//fmt.Println("NO:", path)
		}
		return nil
	})
}

func init() {
	getPathFile()
	fmt.Println(allPath)
	confFile, err := os.Open(allPath)
	if err != nil {
		panic(err)
	}
	defer confFile.Close()
	stat, err := confFile.Stat()
	if err != nil {
		panic(err)
	}
	bs := make([]byte, stat.Size())
	confFile.Read(bs)
	if err := json.Unmarshal(bs, &Config); err != nil {
		panic(err)
	}
}
