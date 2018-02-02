package tls

import (
	ConfServis "MetricsTest/config"
	"MetricsTest/connect"
	"MetricsTest/templates/models"
	Conf "MetricsTest/templates/user/config"
	"crypto/tls"

	"MetricsTest/structures"
	//	"MetricsTest/action"

	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"io"
	"sync"
)

var ConnectTls *tls.Conn
var config tls.Config
var MetkaReboot bool //Если программа подключилась true
var Word string
var Start chan bool
var Activ chan int

var RWMutex *sync.RWMutex

func TimeReconnect() {
	time.Sleep(time.Minute * 1)
	if !MetkaReboot { //Если сейчас нет переподключения то пингуем, в случае ошибки он там начнет переподключение
		Ur.Ping()
	}
	TimeReconnect()
}

func Send(SM []byte, conn net.Conn) error {
	_, err := conn.Write(append([]byte(strconv.Itoa(len(SM))+":"), SM...))
	if err != nil {
		if !MetkaReboot {
			Start <- true
		}
	}
	return err
}

func Read(conn net.Conn) (string, error, bool) {
	Leng := make([]byte, 1)
	if _, err := io.ReadFull(conn, Leng); err != nil {
		if !MetkaReboot {
			Start <- true
		}
		return string(Leng), err, true
	}
	CountLen := string(Leng)
	for strings.Index(CountLen, ":") == -1 {
		if _, err := io.ReadFull(conn, Leng); err != nil {
			if !MetkaReboot {
				Start <- true
			}
			return string(Leng), err, true
		}
		CountLen += string(Leng)

	}
	if len(CountLen) == 0 {
		return string(Leng), errors.New("Не корректная длинна сообщения"), false
	}
	lenReply, err := strconv.Atoi(string(CountLen[:len(CountLen)-1]))
	if err != nil {
		return string(Leng), err, false
	}
	mes := make([]byte, lenReply)
	if _, err := io.ReadFull(conn, mes); err != nil {
		if !MetkaReboot {
			Start <- true
		}
		return string(Leng), err, true
	}
	return string(mes), nil, false
}

func init() {
	RWMutex = &sync.RWMutex{}
	MetkaReboot = false
	go TimeReconnect()
	Start, Activ = make(chan bool), make(chan int, 1)
	go AllConnect()
	Start <- true
}

func AllConnect() {
	for {
		select {
		case <-Start:
			MetkaReboot = true
			err := errors.New("ss")
			for err != nil {
				if err = Reconnect(); err != nil {
					time.Sleep(time.Second * 2)
					continue
				}
			}
			MetkaReboot = false
		}
	}
}

func Reconnect() error {
	var err error
	ConnectTls, err = tls.Dial("tcp", Conf.Config.Tls_connect, &ConfServis.ConfigTls)
	if err != nil {
		log.Println("client: dial:", err)
		fmt.Println("client: dial:", err)
		return err
	}
	if err = Ur.Ping(); err != nil {
		fmt.Println("client: ping:", err)
		return err
	}

	return nil

}

/************************ PING  ********************************/
func (u *Usr) Ping() error {
	RWMutex.Lock()
	defer RWMutex.Unlock()
	if err := Send([]byte("PING"), ConnectTls); err != nil {
		fmt.Println(err)
		return err
	}
	reply, err, _ := Read(ConnectTls)
	if err != nil {
		fmt.Println(err)
		return err
	}
	log.Println("User:", reply)
	return nil
}

func (u *Usr) SelectAll(Type, Query string, inLimit, inStart int64) error {
	RWMutex.Lock()
	defer RWMutex.Unlock()
	fmt.Println("Зашли во все выборки")

	Q := models.QueryMessage{Table: Type, Query: Query, Limit: inLimit, Offset: inStart}
	Bytes1, err := json.Marshal(Q)
	if err != nil {
		return err
	}
	if err := Send([]byte(string(Bytes1)), ConnectTls); err != nil {
		return err
	}
	for {
		reply, err, _ := Read(ConnectTls)
		if err != nil {
			return err
		}
		id, mes := reply[0:2], reply[3:]
		if id == "00" {
			return errors.New("Error:" + mes)
		} else {
			if mes == "EOF" {
				break
			}
			if err = json.Unmarshal([]byte(mes), &u.Solo); err != nil {
				return err
			}
			u.Array = append(u.Array, u.Solo)
		}
	}
	return nil
}

/*Велосипед для отказов*/
type CancellationOrder struct {
	Order_id          int64
	Order_time        time.Time
	Cancellation_time time.Time
	User_hash         string
	User_name         string
	Cancellation_note string
	Comp              []CompCancellationOrder
}

type CompCancellationOrder struct {
	Price_id   int64
	Price_name string
	Units      string
	Type_id    int64
	Type_name  string
}

func (c *CancellationOrder) Select() {

}

type Common struct {
	Array []interface{}
}

func (com *Common) Select(IP string, query, table, type_parameter string, values ...interface{}) error {

	Q := structures.Message{Query: query}
	conn, err := connect.CreateConnect(&IP)
	if err != nil {
		return err
	}
	defer conn.Close()

	Table := structures.Table{Name: table, TypeParameter: type_parameter}
	Table.Values = append(Table.Values, values...)
	Q.Tables = append(Q.Tables, Table)
	Answer_Message, err := connect.SelectMessage(&conn, Q) //Спросили по промежутку или чего то там
	if err != nil {
		fmt.Println(3333)
		return err
	}
	if len(Answer_Message.Tables) > 0 {
		com.Array = Answer_Message.Tables[0].Values
		fmt.Println(4444)
		return nil
	}
	return nil
}
