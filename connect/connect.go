package connect

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"

	"MetricsTest/config"
	fn "MetricsTest/function"
	"MetricsTest/structures"
)

type tlsConfig struct {
	ConfigTlsClient tls.Config
	ConfigTlsServer tls.Config
}

var TlsConfig tlsConfig

func initTlsSertification() error {
	cert2_b, err := ioutil.ReadFile(config.Config.TLS_pem)
	if err != nil {
		return err
	}
	priv2_b, err := ioutil.ReadFile(config.Config.TLS_key)
	if err != nil {
		return err
	}
	priv2, err := x509.ParsePKCS1PrivateKey(priv2_b)
	if err != nil {
		return err
	}
	cert := tls.Certificate{
		Certificate: [][]byte{cert2_b},
		PrivateKey:  priv2,
	}
	TlsConfig.ConfigTlsClient = tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
	return nil
}

func init() {
	initTlsSertification()
}

func CreateConnect(Address *string) (net.Conn, error) {
	conn, err := tls.Dial("tcp", *Address, &TlsConfig.ConfigTlsClient)
	return conn, err
}

func Select(conn *net.Conn, QM interface{}) (string, error) {
	Bytes1, err := json.Marshal(QM)
	if err != nil {
		return "", err
	}
	if err, _ := fn.Send([]byte(string(Bytes1)), *conn); err != nil {
		return "", err
	}
	reply, err := fn.Read(conn, false)
	return reply, err
}

func SelectMessage(conn *net.Conn, QM interface{}) (structures.Message, error) {
	var Answer structures.Message
	Bytes1, err := json.Marshal(QM)
	if err != nil {
		return Answer, err
	}
	if err, _ := fn.Send([]byte(string(Bytes1)), *conn); err != nil {
		return Answer, err
	}
	reply, err := fn.Read(conn, false)
	//fmt.Println("reply:", string(reply))
	if err := json.Unmarshal([]byte(reply), &Answer); err != nil {
		return Answer, err
	}
	if Answer.Error.Code != 0 {
		return Answer, errors.New("Ошибка запроса:" + Answer.Error.Type + ":" + Answer.Error.Description)
	}
	return Answer, err
}

func SelectMessageOLD(conn *net.Conn, QM, TQM interface{}) (structures.Message, error) {
	var Answer structures.Message
	Bytes1, err := json.Marshal(QM)
	if err != nil {
		return Answer, err
	}
	Bytes2, err := json.Marshal(TQM)
	if err != nil {
		return Answer, err
	}
	if err, _ := fn.Send([]byte(string(Bytes1)+string(Bytes2)), *conn); err != nil {
		return Answer, err
	}
	reply, err := fn.Read(conn, false)
	log.Println("reply:", string(reply))
	if err := json.Unmarshal([]byte(reply), &Answer); err != nil {
		return Answer, err
	}
	if Answer.Error.Code != 0 {
		return Answer, errors.New("Ошибка запроса:" + Answer.Error.Type + ":" + Answer.Error.Description)
	}
	return Answer, err
}

func SelectRows(conn *net.Conn, QM interface{}) ([]string, error) {
	var Answer []string
	Bytes1, err := json.Marshal(QM)
	if err != nil {
		return Answer, err
	}
	if err, _ := fn.Send([]byte(string(Bytes1)), *conn); err != nil {
		return Answer, err
	}
	for true {
		reply, err := fn.Read(conn, true)
		if err != nil {
			return Answer, err
		}
		if reply == "EOF" {
			break
		}
		Answer = append(Answer, reply)
	}
	return Answer, nil
}
