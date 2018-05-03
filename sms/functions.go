package sms

import (
	"fmt"
	"net/http"
)

type SMS struct {
	Phone   string
	Message string
}

func (sms *SMS) Send() error {
	resp, err := http.Get("http://smsc.ru/sys/send.php?login=yapoki&psw=ff4501&phones=" + sms.Phone + "&mes=" + sms.Message + "&charset=utf-8")
	if err != nil {
		return fmt.Errorf("Ошибка СМС: %v", err)
	}

	defer resp.Body.Close()

	return nil
}
