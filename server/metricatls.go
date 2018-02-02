package main

import (
	"crypto/tls"
	"crypto/x509"
	//	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	//	"io"
	"io/ioutil"
	"log"
	"net"
	//	"strconv"
	//	"strings"
	"time"
	//	"sync"

	"MetricsTest/action"
	"MetricsTest/config"
	fn "MetricsTest/function"
	db "MetricsTest/postgresql"
	"MetricsTest/routing"
	"MetricsTest/structures"
)

var comments bool

func init() {
	comments = config.Config.Detailed_logging
}

func main() {
	ca_b, err := ioutil.ReadFile(config.Config.TLS_pem)
	if err != nil {
		log.Println(time.Now, "SERVER_ERROR_READ_PEM_FILE", err)
		panic(err)
		return
	}

	priv_b, err := ioutil.ReadFile(config.Config.TLS_key)
	if err != nil {
		log.Println(time.Now, "SERVER_ERROR_READ_KEY_FILE", err)
		panic(err)
		return
	}

	ca, err := x509.ParseCertificate(ca_b)
	if err != nil {
		log.Println(time.Now, "SERVER_ERROR_PARSE_CERT", err)
		panic(err)
		return
	}

	priv, err := x509.ParsePKCS1PrivateKey(priv_b)
	if err != nil {
		log.Println(time.Now, "SERVER_ERROR_PARSE_PRIVATE_KEY", err)
		panic(err)
		return
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca)

	cert := tls.Certificate{
		Certificate: [][]byte{ca_b},
		PrivateKey:  priv,
	}

	cfg := tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
	}

	//Запуск сервера gin
	go routing.Gin_Start()

	fmt.Println(config.Config.TLS_server + ":" + config.Config.TLS_port)

	listener, err := tls.Listen("tcp", config.Config.TLS_server+":"+config.Config.TLS_port, &cfg)
	if err != nil {
		panic(err)
		return
	}
	log.Print("server: listening")
	defer listener.Close()

	/*Запуск левых, потом сюда встанет GIN, а TLS уйдет отдельно*/
	go action.InitMetrics()

	for {
		conn, err := listener.Accept()

		if err != nil {
			log.Printf("server: accept: %s", err)
			continue
		}
		log.Printf("server: accepted from %s", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	defer action.Recover()

	for {
		mes, err := fn.Read(&conn, false)
		if err != nil {
			break
		}
		if string(mes) == "PING" {
			fn.Send([]byte("PONG"), conn)
			continue
		}
		fmt.Println("\nmes: ", string(mes))

		var Q structures.Message

		if err = json.Unmarshal([]byte(mes), &Q); err != nil {
			fmt.Println("error in Unmarshal", err.Error())
			Q.Error.Code, Q.Error.Type, Q.Error.Description = 1, "json", err.Error()
			b, _ := json.Marshal(Q)
			fn.Send([]byte(b), conn)
			continue
		}

		fmt.Println("Q: ", Q)
		log.Println("Q: ", Q)

		for key, _ := range Q.Tables {
			if err := Select(&Q, key); err != nil {
				fmt.Println("err:", err)
				var q structures.Message
				q.Error.Code, q.Error.Type, q.Error.Description = 1, "json", err.Error()
				b, _ := json.Marshal(q)
				fn.Send([]byte(b), conn)
			}
		}
		b, _ := json.Marshal(Q)

		log.Println("\nA: ", string(b))

		fn.Send([]byte(b), conn)
	}
}

func Select(m *structures.Message, index int) error {
	//	if m.Tables[index].Name == "Funct" {
	//		maps, err := Buns(m.Tables[index].TypeParameter, m.Tables[index].Values...)
	//		if err != nil {
	//			return err
	//		}
	//		m.Tables[index].Values = nil
	//		m.Tables[index].Values = append(m.Tables[index].Values, maps)
	//	} else {
	//log.Println(m.Query+"."+m.Tables[index].Name+"."+m.Tables[index].TypeParameter, m.Tables[index].Values)
	log.Println("Quest m:", m)

	Rows, err := db.Requests.Query(m.Query+"."+m.Tables[index].Name+"."+m.Tables[index].TypeParameter, m.Tables[index].Values...)
	if err != nil {
		log.Println("Select db.Requests.Query err", err)
		return err
	}
	defer Rows.Close()

	m.Tables[index].Values = nil //очищаем параметры, чтобы потом добавить в ответ новые

	for Rows.Next() {
		var answer structures.BD_READ

		switch m.Tables[index].TypeParameter {
		case "ReportSaleByInterval":
			answer = &structures.Metrics_add_info{}
		case "ReportSaleNewByInterval":
			answer = &structures.ReportSale{}
		case "ReportSummaOnTypePaymentsFromCashBox":
			answer = &structures.Result_summ{}
		default:
			return errors.New("Неизвестная таблица")
		}

		if err := answer.Record(Rows); err != nil {
			return err
		}

		m.Tables[index].Values = append(m.Tables[index].Values, answer)
	}
	//	}

	log.Println("Answer m:", m)
	return nil
}

//func ParseFunction(st *structures.Message) error {
//	var Transaction postgresql.Transaction
//	if err := Transaction.Begin(); err != nil {
//		return err
//	}
//	defer Transaction.RollBack()    //val.Name  - это действие // дата, склад, хеш, коунт, total_price (тоже разницу надо)
//	for _, val := range st.Tables { //Пока тут только склады   //  0 	 1		2	  3			4
//		fmt.Println("\nst:", st)
//		if len(val.Values) != 5 {
//			return errors.New("Не корректное число переданных параметров (надо 5), пришло:" + fmt.Sprint(len(val.Values)))
//		}
//		var Values []interface{}
//		var DateS, Sklad string = val.Values[0].(string), val.Values[1].(string)
//		var Parameter_ID int64
//		MAI := postgresql.Metrics_add_info{
//			Hash:      val.Values[2].(string),
//			Name:      action.Default,
//			Units:     action.Default,
//			Count:     val.Values[3].(float64),
//			Price:     val.Values[4].(float64),
//			Price_id:  action.DefaultFloat64,
//			Status_id: -1,
//		}
//		switch val.Name {
//		case "Rashod":
//			Parameter_ID = 9
//		case "Prixod":
//			Parameter_ID = 10
//		case "Spisanie":
//			Parameter_ID = 11
//		default:
//			return errors.New("Не известное действие: " + val.Name)
//		}
//		Values = append(Values, DateS, Parameter_ID, Sklad)
//		//date($1) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=3"
//		if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "DateStep_idParameter_idMS(3)", Values...); err != nil && err.Error() != postgresql.SQL_NO_ROWS {
//			return err
//		}
//		SELECT_ID := Transaction.HashData
//		log.Println("SELECT_ID:", SELECT_ID)
//		if SELECT_ID == nil {
//			var MDD postgresql.Metrics_dop_data
//			if err := MDD.Select("Select", "metrics_dop_data", "Franchise_hierarchy", Sklad, DateS[:10]); err != nil {
//				if err.Error() != postgresql.SQL_NO_ROWS {
//					return err
//				}
//				MDD.MFH.Name, MDD.MFH.Parent_hash = "НЕ ИДЕНТИФИЦИРОВАН", "НЕ ИДЕНТИФИЦИРОВАН"
//			}
//			if err := Transaction.Transaction_QTTV_One(true, "Insert", "metrics", "", MDD.MFH.Hash, MDD.MFH.Name, DateS, -1, 3, Parameter_ID); err != nil {
//				return err
//			}
//			if err := Transaction.Transaction_QTTV_One(false, "Insert", "metrics_add_info", "Point", Transaction.HashData, MAI.Hash, MAI.Name, MAI.Count, MAI.Units, MAI.Price, MAI.Price_id, MAI.Status_id); err != nil {
//				return err
//			}
//		} else {
//			if err := Transaction.Transaction_QTTV_One(true, "Select", "metrics_add_info", "JSONMetric_idHash", SELECT_ID, MAI.Hash); err != nil && err.Error() != postgresql.SQL_NO_ROWS {
//				return err
//			}
//			ADD_INFO_JSON := Transaction.HashData
//			log.Println("ADD_INFO_ID:", ADD_INFO_JSON)
//			if ADD_INFO_JSON != nil {
//				var Metrics_add_info postgresql.Metrics_add_info
//				if err := json.Unmarshal([]byte(ADD_INFO_JSON.(string)), &Metrics_add_info); err != nil {
//					return err
//				}
//				if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_add_info", "AddCountPrice", Metrics_add_info.ID, MAI.Count, MAI.Price); err != nil {
//					return err
//				}
//			} else { //																					 metric_id, hash, 		name, 	count, 		units, 	  price, 	 price_id, 	   status_id
//				if err := Transaction.Transaction_QTTV_One(false, "Insert", "metrics_add_info", "Point", SELECT_ID, MAI.Hash, MAI.Name, MAI.Count, MAI.Units, MAI.Price, MAI.Price_id, MAI.Status_id); err != nil {
//					return err
//				}
//			}
//		}
//	}
//	return Transaction.Commit()
//}
