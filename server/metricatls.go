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

	"MetricsNew/action"
	"MetricsNew/config"
	fn "MetricsNew/function"
	db "MetricsNew/postgresql"
	"MetricsNew/structures"
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
	//go routing.Gin_Start()

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
				fmt.Println("ERROR Select: ", err)
				log.Println("ERROR Select: ", err)
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

	//	if m.Query == "PDF" {
	//		fmt.Println("*** PDF", m)
	//		return nil
	//	}

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
		case "ReportSaleNewByInterval":
			answer = &structures.ReportSale{}
		case "ReportSummOnTypePayments":
			answer = &structures.ReportSummOnTypePayments{}
		case "ReportCashboxNewByInterval":
			answer = &structures.ReportCashbox{}
		case "ReportOperatorsNewByInterval":
			answer = &structures.ReportOperator{}
		case "ReportCouriersNewByInterval":
			answer = &structures.ReportCourier{}
		case "ReportCouriersAddrByInterval":
			answer = &structures.ReportCourierDetailed{}
		case "ReportTimeDeliveryByInterval":
			answer = &structures.ReportTimeDelivery{}
		case "ReportCancelOrdersNewByInterval":
			answer = &structures.ReportCancelOrders{}
		case "ReportOrdersOnTime":
			answer = &structures.ReportOrdersOnTime{}
		default:
			return errors.New("Неизвестная таблица")
		}

		if err := answer.Record(Rows); err != nil {
			return fmt.Errorf("answer.Record: %v", err)
		}

		m.Tables[index].Values = append(m.Tables[index].Values, answer)
	}
	//	}

	if err := Rows.Err(); err != nil {
		log.Println("Rows.Err: ", err)
		fmt.Println("Rows.Err: ", err)
	}

	log.Println("Answer m:", m)
	return nil
}
