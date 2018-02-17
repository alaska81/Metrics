package function

import (
	"encoding/json"
	"errors"
	//"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"bytes"
	"encoding/gob"

	"MetricsNew/structures"
)

//Отправка сообщений
func Send(SM []byte, conn net.Conn) (error, bool) {
	_, err := conn.Write(append([]byte(strconv.Itoa(len(SM))+":"), SM...))
	return err, err != nil
}

//Чтение сообщений
func Read(conn *net.Conn, Mtk bool) (string, error) {

	Leng := make([]byte, 1)
	if _, err := io.ReadFull(*conn, Leng); err != nil {
		return "", err
	}
	CountLen := string(Leng)
	for strings.Index(CountLen, ":") == -1 {
		if _, err := io.ReadFull(*conn, Leng); err != nil {
			return "", err
		}
		CountLen += string(Leng)
	}
	if len(CountLen) == 0 {
		return "", errors.New("Не корректная длинна сообщения")
	}
	lenReply, err := strconv.Atoi(string(CountLen[:len(CountLen)-1]))
	if err != nil {
		return "", err
	}
	reply := make([]byte, lenReply)
	if _, err := io.ReadFull(*conn, reply); err != nil {
		return "", err
	}
	if Mtk {
		id, mes := string(reply[0:2]), string(reply[3:])
		if id == "00" {
			return "", errors.New(mes)
		}
		return mes, nil
	} else {
		return string(reply), nil
	}
}

func FormatDate(Time time.Time) string {
	return Time.Format("2006-01-02 15:04:05")
}

func FormatTime(Time time.Time) string {
	return Time.Format("15:04:05")
}

func FormatUTC(Time time.Time) time.Time {
	return time.Date(Time.Year(), Time.Month(), Time.Day(), Time.Hour(), Time.Minute(), Time.Second(), 0, time.UTC)
}

func StringToTime(Str string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", Str)
}

func AnswerParse(Answer string) (structures.Message, error) {
	//fmt.Println("\n-----------------\nAnswerParse:", Answer, "\n-----------------\n")
	Q_ANSWER := structures.Message{}
	if err := json.Unmarshal([]byte(Answer), &Q_ANSWER); err != nil {
		return Q_ANSWER, err
	}
	if Q_ANSWER.Error.Code != 0 {
		return Q_ANSWER, errors.New(Q_ANSWER.Error.Description + " " + Q_ANSWER.Error.Type)
	}
	return Q_ANSWER, nil
}

func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//func AnswerParse(Answer []interface) (structures.Message, error) {
//	fmt.Println("\n-----------------\nAnswerParse:", Answer, "\n-----------------\n")
//}
