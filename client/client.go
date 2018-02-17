package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"

	"MetricsNew/config"
	"MetricsNew/structures"
)

func send(sendMessage []byte, conn net.Conn) {

	//	log.Println("Message:", string(sendMessage), " for "+conn.RemoteAddr().String())
	//	println("Message:", string(sendMessage), " for "+conn.RemoteAddr().String())
	s := strconv.Itoa(len(sendMessage))
	fmt.Println("s:", s)
	s += ":"
	sendMessage = append([]byte(s), sendMessage...)
	//fmt.Println(string(sendMessage))
	LenMess, err := conn.Write(sendMessage)
	if err != nil {
		log.Println("|||||ERROR:Gone for:"+conn.RemoteAddr().String(), " - ||||", string(sendMessage), LenMess, err)
		println("|||||ERROR:Gone for:"+conn.RemoteAddr().String(), " - ||||", string(sendMessage), LenMess, err.Error())
		return
	}
	//log.Println("|||||Gone for:"+conn.RemoteAddr().String(), " - ||||", string(sendMessage))
	//println("|||||Gone for:"+conn.RemoteAddr().String(), " - ||||", string(sendMessage))
}

func main() {

	/////////////////////////////////////////////////////////////////////////
	cert_b, err := ioutil.ReadFile(config.Config.TLS_pem)
	if err != nil {
		fmt.Println(time.Now, "SERVER_ERROR_READ_PEM_FILE", err)
	}

	priv_b, err := ioutil.ReadFile(config.Config.TLS_key)
	if err != nil {
		fmt.Println(time.Now, "SERVER_ERROR_READ_KEY_FILE", err)
	}

	priv, err := x509.ParsePKCS1PrivateKey(priv_b)
	if err != nil {
		fmt.Println(time.Now, "SERVER_ERROR_PARSE_PRIVATE_KEY", err)
	}

	cert := tls.Certificate{
		Certificate: [][]byte{cert_b},
		PrivateKey:  priv,
	}

	cfg := tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true}
	fmt.Println("1 ")

	conn, err := tls.Dial("tcp", config.Config.TLS_server+":"+config.Config.TLS_port, &cfg)
	if err != nil {
		fmt.Println("client: dial: %s", err)
	}

	defer conn.Close()

	fmt.Println("client: connected to: ", conn.RemoteAddr())

	state := conn.ConnectionState()

	fmt.Println("client: handshake: ", state.HandshakeComplete)

	fmt.Println("client: mutual: ", state.NegotiatedProtocolIsMutual)

	/////////////////////////////////////////////////////////////////////////

	var inputSquare float64
	//	var reply []byte

	Int := 1
	for true {

		fmt.Println("\n\n---------------- Удаление сессий Language ----------------------") /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		fmt.Scanf("%f\n", &inputSquare)

		Q := structures.QueryMessage{
			Table:         "Session",
			Query:         "Remove",
			TypeParameter: "Language"}

		for i := 0; i < 100000*Int; i++ {
			Q.Values = append(Q.Values, "TESTINGTESTINGSAADASDSADADASDASDASDSADSAD,")
		}
		Bytes1, _ := json.Marshal(Q)
		send([]byte(string(Bytes1)), conn)
		Int++
		//		n, _ := conn.Read(reply) //ожидание ответа

		//		id := reply[0:2]
		//		mes := reply[3:n]

		//		if string(id) == "00" {
		//			fmt.Println("no Delete sessions Error:", string(mes))
		//		} else {
		//			fmt.Println("Delete sessions")
		//		}
	}

}
