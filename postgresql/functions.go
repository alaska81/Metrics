package postgresql

import (

	//	"database/sql"

	"errors"
	"fmt"
	"log"
)

var Default string = "DEFAULT"
var DefaultFloat64 float64 = -1
var DefaultDate string = "0001-01-01"

func Recover() {
	if r := recover(); r != nil {
		fmt.Println("Panic:", r)
		log.Println("\n*** Panic:", r)
	}
}

func (MSR *Metrics_step_request) Select(Query, Table, Type string, Value ...interface{}) error {
	if MSR.Roow.Rows, MSR.Err = Requests.Query(Query+"."+Table+"."+Type, Value...); MSR.Err != nil {
		return MSR.Err
	}
	defer MSR.Roow.Rows.Close()
	for MSR.Roow.Rows.Next() {
		if MSR.Err = MSR.Roow.Rows.Scan(&MSR.MS.ID, &MSR.MS.Name, &MSR.MS.Value, &MSR.MS.Duration); MSR.Err != nil { //Узнаем какие метрики с какими шагами существуют
			log.Println(MSR.Err.Error())
			return MSR.Err
		}
		MSR.MS_ARRAY = append(MSR.MS_ARRAY, MSR.MS)
	}
	return nil
}

func (M *Metrics_request) Select(Query, Table, Type string, Value ...interface{}) error {
	if M.Roow.Row, M.Err = Requests.QueryRow(Query+"."+Table+"."+Type, Value...); M.Err != nil {
		return M.Err
	}
	if M.Err = M.Roow.Row.Scan(&M.M.ID, &M.M.OwnHash, &M.M.OwnName, &M.M.Date, &M.M.Value, &M.M.Step_ID, &M.M.Parameter_ID); M.Err != nil {
		return M.Err
	}
	return M.Err //Узнаем какие метрики с какими шагами существуют
}

func (M *Metrics_request) Action(Query, Table, Type string, Value ...interface{}) error {
	M.Err = Requests.ExecTransact(Query+"."+Table+"."+Type, Value...)
	return M.Err
}

/* Транзакции */

//Открывает транзакцию
func (T *Transaction) Begin() error {
	var err error
	if T.Tx == nil {
		T.Tx, err = db.Begin()
	}
	if err == nil {
		log.Println("Открыл транзакцию")
		fmt.Println("Открыл транзакцию\n")
	}
	return err
}

//Откатывает транзакцию
func (T *Transaction) RollBack() {
	if T.Tx != nil {
		T.Tx.Rollback()
		log.Println("Откатил транзакцию")
		fmt.Println("\nОткатил транзакцию")
	}
}

//Закрывает транзакцию
func (T *Transaction) Commit() error {
	err := T.Tx.Commit()
	if err == nil {
		T.Tx = nil
		log.Println("Закрыл транзакцию")
		fmt.Println("\nЗакрыл транзакцию")
	}

	return err
}

//Один запрос в транзакции
func (T *Transaction) Transaction_One(RETURNING bool) error {
	var err error
	T.HashData = nil
	log.Println("\nЗапрос (TO): ", T.DataOne.Query+"."+T.DataOne.Table+"."+T.DataOne.Type, "\nПараметры: ", T.DataOne.Values)
	if _, ok := Requests.requestsList[T.DataOne.Query+"."+T.DataOne.Table+"."+T.DataOne.Type]; !ok {
		return errors.New("Missmatch request!")
	}
	if RETURNING {
		err = T.Tx.Stmt(Requests.requestsList[T.DataOne.Query+"."+T.DataOne.Table+"."+T.DataOne.Type]).QueryRow(T.DataOne.Values...).Scan(&T.HashData)
	} else {
		_, err = T.Tx.Stmt(Requests.requestsList[T.DataOne.Query+"."+T.DataOne.Table+"."+T.DataOne.Type]).Exec(T.DataOne.Values...)
	}
	T.DataOne.Values = nil
	return err
}

func (T *Transaction) Transaction_QTTV_One(RETURNING bool, Query, Table, Type string, Values ...interface{}) error {
	var err error
	T.HashData = nil
	//log.Println("Запрос (TQTTVO): ", Query+"."+Table+"."+Type, "\nПараметры: ", Values)
	if _, ok := Requests.requestsList[Query+"."+Table+"."+Type]; !ok {
		return errors.New("Missmatch request!")
	}
	if RETURNING {
		err = T.Tx.Stmt(Requests.requestsList[Query+"."+Table+"."+Type]).QueryRow(Values...).Scan(&T.HashData)
	} else {
		_, err = T.Tx.Stmt(Requests.requestsList[Query+"."+Table+"."+Type]).Exec(Values...)
	}
	return err
}

//Полностью закрывает используемую транзакцию
func (T *Transaction) Transaction(OPEN bool) error { //true - если надо закрыть транзакцию после выполнения
	var err error
	if OPEN { //Если надо открыть и закрыть ее
		if T.Tx == nil { //Если она еще не была открыта
			if T.Tx, err = db.Begin(); err != nil {
				return err
			}
			defer T.Tx.Rollback()
		}
	}
	for _, val := range T.Data {
		log.Println("\nЗапрос (T): ", val.Query+"."+val.Table+"."+val.Type, "\nПараметры: ", val.Values)
		if _, ok := Requests.requestsList[val.Query+"."+val.Table+"."+val.Type]; !ok {
			return errors.New("Missmatch request!")
		}
		//fmt.Println("В транзакции:", val.Values)
		if _, err = T.Tx.Stmt(Requests.requestsList[val.Query+"."+val.Table+"."+val.Type]).Exec(val.Values...); err != nil {
			return err
		}
	}
	if OPEN { //Закрыть если надо закрыть
		return T.Tx.Commit()
	}
	T.Data, T.DataOne, T.HashData = T.Data[0:0], TransactionAction{}, nil
	return nil
}
