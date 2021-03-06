package config

// Получение настроек из файла конфигураций

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

//var RequestResponse struct

//Объявление структуры конфигураций.
type Configurations struct {
	Enable_service_log bool
	Enable_record_log  bool

	Postgre_user     string
	Postgre_password string
	Postgre_host     string
	Postgre_database string
	Postgre_ssl      string

	Order_service        string
	Organization_service string
	Role_service         string

	TLS_server string
	TLS_port   string
	TLS_pem    string
	TLS_key    string

	Expired_count    int
	Connect_count    int
	Detailed_logging bool

	Start_time string

	Port_Gin     string
	Word         string
	Using_Domain string

	Redis_Addr     string
	Redis_Password string
	Redis_DB       int
}

// type Log interface {
// 	toChannel(values ...interface{})
// }

type Channel struct {
	c chan []interface{}
}

type ChanMessaging struct {
	c chan []interface{}
}

type ChanErrors struct {
	c chan []interface{}
}

var (
	logMessaging *log.Logger
	logErrors    *log.Logger
)

// var (
// 	ChanMessaging = make(chan []interface{})
// 	ChanErrors    = make(chan []interface{})
// )

func (c *Configurations) getConfigurations() error {

	confFile, err := os.Open("../config/config.conf")

	if err != nil {
		return err
	}
	defer confFile.Close()

	stat, err := confFile.Stat()

	if err != nil {
		return err
	}

	bs := make([]byte, stat.Size())
	_, err = confFile.Read(bs)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bs, &c)

	if err != nil {
		return err
	}

	return nil
}

var Config Configurations
var LogFile *os.File

var ConfigTls tls.Config

func Sertification() {
	var err error
	cert2_b, err := ioutil.ReadFile(Config.TLS_pem)
	if err != nil {
		panic(err)
	}
	priv2_b, err := ioutil.ReadFile(Config.TLS_key)
	if err != nil {
		panic(err)
	}
	priv2, err := x509.ParsePKCS1PrivateKey(priv2_b)
	if err != nil {
		panic(err)
	}

	cert := tls.Certificate{
		Certificate: [][]byte{cert2_b},
		PrivateKey:  priv2,
	}
	ConfigTls = tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
}

func init() {
	// Чтение файла конфигураций
	err := Config.getConfigurations()
	if err != nil {
		log.Panic("Config file not found!:", err)
	}
	log.Println("Config established!")
	if Config.Enable_service_log {
		StartLog()
		go RecLog()
	}
	Sertification()

	go formChannel()

	//var log Log
	ChanMessaging.c = make(chan []interface{})
	ChanErrors.c = make(chan []interface{})

	// log := &Channel{ChanErrors}
	// log.toChannel("aa")
	// log.toChannel("bbb")
	// log = &Channel{ChanMessaging}
	// log.toChannel("ccc")
}

func RecLog() {
	t1 := time.Now()
	t2, err := time.Parse("2006-01-02T15:04:05.000000-07:00", t1.String()[0:10]+"T23:59:59.999999+05:00")
	if err != nil {
		return
	}
	time.Sleep(time.Minute)
	time.Sleep(t2.Sub(t1.Add(time.Minute * 3)))
	StartLog()
	RecLog()
}

func StartLog() {
	LogFile.Close()

	_, err := os.Stat("./log/")
	if os.IsNotExist(err) {
		os.MkdirAll("./log/", 0777)
	}

	LogFile, err = os.OpenFile("./log/"+time.Now().String()[:10]+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Panic("Logfile not found!:", err)
	}
	log.SetOutput(LogFile)

	LogFile, err = os.OpenFile("./log/messaging "+time.Now().String()[:10]+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logMessaging = log.New(LogFile, "", log.Ldate|log.Ltime|log.Lshortfile)

	LogFile, err = os.OpenFile("./log/errors "+time.Now().String()[:10]+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logErrors = log.New(LogFile, "", log.Ldate|log.Ltime|log.Lshortfile)

	log.Println("_______NEW_START_OF_SERVER_______")
	logMessaging.Println("_______NEW_START_OF_SERVER_______")
	logErrors.Println("_______NEW_START_OF_SERVER_______")
}

func formChannel() {
	for {
		select {
		case x, ok := <-ChanMessaging:
			if !ok {
				ChanMessaging = nil
				continue
			}
			logMessaging.Println(x)

		case x, ok := <-ChanErrors:
			if !ok {
				ChanErrors = nil
				continue
			}
			logErrors.Println(x)
		}
	}
}

func (channel *Channel) toChannel(values ...interface{}) {
	fmt.Println(values)
	if channel.c != nil {
		channel.c <- values
	}
}

func (channel *ChanMessaging) toChannel(values ...interface{}) {
	fmt.Println(values)
	if channel.c != nil {
		channel.c <- values
	}
}
