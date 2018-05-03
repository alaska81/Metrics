package action

import (
	"database/sql"
	"net"
	//	"errors"
	"fmt"
	"log"
	//	"os"
	"encoding/json"
	"errors"
	//"strings"
	"math/rand"
	"sync"
	"time"

	"MetricsNew/config"
	"MetricsNew/connect"
	fn "MetricsNew/function"
	"MetricsNew/postgresql"
	"MetricsNew/redis"
	"MetricsNew/sms"

	"MetricsNew/structures"

	_ "github.com/lib/pq"
)

// statements 1 inputs, got 0 - ждем 1, получил 0

var comments bool

var Default string = "DEFAULT"
var DefaultFloat64 float64 = -1

type mutexs struct {
	m map[int64]*sync.RWMutex
}

var Mutex *mutexs

func NewMutex() *mutexs {
	return &mutexs{m: make(map[int64]*sync.RWMutex)}
}
func (M *mutexs) AddMutex(Table int64) {
	fmt.Println("Добавление в мап мьютексов:", Table)
	M.m[Table] = &sync.RWMutex{}
	fmt.Println("Добавлено в мап мьютексов:", Table)
}
func (M *mutexs) PrintMutexInfo(Action, After string, Table int64, Address string) {
	fmt.Println(After+" работы с мьютексом --------"+Action+"--"+Address+"------MUTEX---Состояние ", Table, ": ", M.m[Table])
	log.Println(After+" работы с мьютексом --------"+Action+"--"+Address+"------MUTEX---Состояние ", Table, ": ", M.m[Table])
}
func (M *mutexs) Lock(Table int64, Address string) {
	M.PrintMutexInfo("Lock", "До   ", Table, Address)
	M.m[Table].Lock()
	M.PrintMutexInfo("Lock", "После", Table, Address)
}
func (M *mutexs) Unlock(Table int64, Address string) {
	M.PrintMutexInfo("Unlock", "До   ", Table, Address)
	M.m[Table].Unlock()
	M.PrintMutexInfo("Unlock", "После", Table, Address)
}
func (M *mutexs) RLock(Table int64, Address string) {
	M.PrintMutexInfo("RLock", "До   ", Table, Address)
	M.m[Table].RLock()
	M.PrintMutexInfo("RLock", "После", Table, Address)
}
func (M *mutexs) RUnlock(Table int64, Address string) {
	M.PrintMutexInfo("RUnlock", "До   ", Table, Address)
	M.m[Table].RUnlock()
	M.PrintMutexInfo("RUnlock", "После", Table, Address)
}

type GlobConfig struct {
	MSR       postgresql.Metrics_step_request
	StartTime time.Time
}

var GlobalConfig GlobConfig

type StartMetrics struct {
	Row       *sql.Row
	Rows      *sql.Rows
	SMS_ARRAY []postgresql.SMS
}

var SM StartMetrics

func Recover() {
	if r := recover(); r != nil {
		fmt.Println("Panic:", r)
		log.Println("\n*** Panic:", r)
	}
}

func InitMetrics() {
	//получаем шаги

	err := GlobalConfig.MSR.Select("Select", "metrics_step", "")
	if err != nil {
		panic(err.Error())
	}
	GlobalConfig.StartTime, err = fn.StringToTime(config.Config.Start_time)
	if err != nil {
		panic(err.Error())
	}
	log.Println("Данные считаны: ", GlobalConfig.MSR.MS_ARRAY)
	log.Println("Данные считаны: ", GlobalConfig.StartTime)
	go func() {
		if err := SM.StartMetrics(); err != nil {
			log.Println(err.Error())
		}
	}()
}

//func SelectTables(ID int64) error {
//	Rows, err := postgresql.Requests.Query("Select.metrics_service_table.Service_id", ID)
//	if err != nil {
//		return err
//	}
//}

func (SM *StartMetrics) GetTables() error {
	Mutex = NewMutex()
	Rows, err := postgresql.Requests.Query("Select.metrics_link_step.")
	if err != nil {
		return err
	}
	defer Rows.Close()
	for Rows.Next() {
		var SMS postgresql.SMS
		if err = Rows.Scan(&SMS.MP.Min_Step_ID,
			&SMS.MSTEP.ID, &SMS.MSTEP.Name, &SMS.MSTEP.Value, &SMS.MSTEP.Duration,
			&SMS.MSTEPT.ID, &SMS.MSTEPT.Name,
			&SMS.MP.ID, &SMS.MP.ServiceTableId, &SMS.MP.Timeout, &SMS.MP.Own_ID, &SMS.MP.PendingDate, &SMS.MP.PendingId, &SMS.MP.Protocol_version, &SMS.MP.Update_allow, &SMS.MP.UpdateAt,
			&SMS.MST.ID, &SMS.MST.Query, &SMS.MST.TableName, &SMS.MST.TypeParameter, &SMS.MST.Service_ID, &SMS.MST.Activ,
			&SMS.MS.ID, &SMS.MS.Name, &SMS.MS.IP); err != nil {
			return err
		}

		SM.SMS_ARRAY = append(SM.SMS_ARRAY, SMS)
		Mutex.AddMutex(SMS.MP.ServiceTableId)
	}
	return nil
}

func (SM *StartMetrics) StartMetrics() error {

	if err := SM.GetTables(); err != nil {
		return err
	}

	log.Println("Метрики инициализированы:", time.Now())
	// for true {
	// 	go SM.StartComponentMetrics()
	// 	time.Sleep(time.Minute * 30)
	// }
	SM.StartComponentMetrics()

	return nil
}

func (SM *StartMetrics) StartComponentMetrics() {
	var isSendingSMS = make(map[int64]bool)

	log.Println("SM.SMS_ARRAY :", SM.SMS_ARRAY)
	for KEY := range SM.SMS_ARRAY { //Бежим по всем
		if !SM.SMS_ARRAY[KEY].MST.Activ { //Проверка на активность метрики
			continue
		}

		go func(SMS *postgresql.SMS) {
			time.Sleep(time.Second * time.Duration(rand.Intn(180)))

			isSendingSMS[SMS.MP.ID] = false

			for true {
				err := Go_Routines(SMS)
				if err != nil {
					log.Println(time.Now(), "!!!ERROR: ", fmt.Errorf("Go_Routines: %v", err))
					fmt.Println(time.Now(), "!!!ERROR: ", fmt.Errorf("Go_Routines: %v", err))

					if isSendingSMS[SMS.MP.ID] == false {
						var sms sms.SMS
						sms.Phone = "79058514789"
						sms.Message = time.Now().Format("2006-01-02 15:04:05") + " ERROR: " + err.Error()
						log.Println("SMS: ", SMS.MST.TableName)
						// if err := sms.Send(); err != nil {
						// 	log.Println(time.Now(), "!!!ERROR: ", fmt.Errorf("sms.Send(): %v", err))
						// 	fmt.Println(time.Now(), "!!!ERROR: ", fmt.Errorf("sms.Send(): %v", err))
						// }

						isSendingSMS[SMS.MP.ID] = true
					}

					time.Sleep(time.Minute * 10)
					continue
				}

				isSendingSMS[SMS.MP.ID] = false
				time.Sleep(time.Minute * time.Duration(SMS.MP.Timeout))
			}

		}(&SM.SMS_ARRAY[KEY])

	}
}

func Go_Routines(SMS *postgresql.SMS) error {
	Mutex.Lock(SMS.MP.ServiceTableId, "local")
	defer Mutex.Unlock(SMS.MP.ServiceTableId, "local")

	log.Println("МЕТРИКА: ", SMS.MST.TableName+"."+SMS.MST.TypeParameter)

	// Подклюючение к сторонним сервисам
	conn, err := connect.CreateConnect(&SMS.MS.IP)
	if err != nil {
		return fmt.Errorf("CreateConnect: %v", err)
	}
	defer conn.Close()

	log.Println("К сервису подключился: ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter)
	fmt.Println("К сервису подключился: ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter)

	// Запрос данных у стороннего сервиса
	fmt.Println("протокол", SMS.MP.Protocol_version)

	var M structures.Message

	// Protocol_version == 1
	if SMS.MP.Protocol_version == 1 {
		Q := structures.QueryMessage{Query: SMS.MST.Query, Table: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter, Limit: 99999, Offset: 0}

		// Values по протоколу 1 для План-Табель (PointRoleCountByTime), Бонусы (LogTransactionBonus), Склад (Rashod)
		services := map[int64]int64{78: 1, 88: 1, 95: 1}

		if _, ok := services[SMS.MST.ID]; ok {
			//if SMS.MST.ID == 78 || SMS.MST.ID == 88 {
			Q.Values = append(Q.Values, SMS.MP.PendingDate)
			//Q.Values = append(Q.Values, "2018-04-22T21:12:35Z")
		}
		//---
		log.Println("Запрос на:", SMS.MS.Name, "; Message:", Q)

		AnswerMessage, err := connect.SelectRows(&conn, Q)
		if err != nil {
			return fmt.Errorf("SelectRows: %v", err)
		}

		log.Println("***reply:", AnswerMessage)

		M = structures.Message{Query: SMS.MST.Query}
		Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}

		for _, val := range AnswerMessage {
			var M interface{}
			if err := json.Unmarshal([]byte(val), &M); err != nil {
				return err
			}
			Table.Values = append(Table.Values, M)
		}

		//Table.Values = append(Table.Values, AnswerMessage)
		M.Tables = append(M.Tables, Table)

		//fmt.Println("Answer Message:", M)
		//panic("exit")
	}

	// Protocol_version == 2
	if SMS.MP.Protocol_version == 2 {
		M = structures.Message{Query: SMS.MST.Query}
		Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}
		Table.Values = append(Table.Values, SMS.MP.PendingDate.String()[:19])
		M.Tables = append(M.Tables, Table)

		Table = structures.Table{Name: SMS.MST.TableName, TypeParameter: "PendingDate"}
		Table.Values = append(Table.Values, SMS.MP.PendingDate.String()[:19])
		M.Tables = append(M.Tables, Table)

		log.Println("Запрос на:", SMS.MS.Name, "; Message:", M)
		//fmt.Println("Запрос на:", SMS.MS.Name, "; Message:", M)

		if err := SelectMessage(&conn, &M); err != nil {
			return fmt.Errorf("SelectMessage: %v", err)
		}
	}

	//log.Println("Answer Message:", M)
	//fmt.Println("Answer Message:", M)

	if len(M.Tables) != 0 {
		if err := AddMetricsInDB(SMS, &M); err != nil {
			return fmt.Errorf("AddMetricsInDB: %v", err)
		}
	}

	return nil
}

func AddMetricsInDB(SMS *postgresql.SMS, M *structures.Message) error {
	defer Recover()

	var metrics postgresql.MetricsMetrics
	var values postgresql.MetricValues

	var pendingDate time.Time = SMS.MP.PendingDate
	var pendingID int64 = SMS.MP.PendingId
	var updateAt time.Time

	switch SMS.MST.TableName + "." + SMS.MST.TypeParameter {
	case "GetDataForMetricsNewCashbox.RangeCashbox":
		values = &postgresql.GetDataForMetricsCashbox{}
	case "GetDataForMetricsCashboxShift.RangeCashbox":
		values = &postgresql.GetDataForMetricsCashboxShift{}
	case "GetDataForMetricsNewOrders.RangeOrders":
		values = &postgresql.GetDataForMetricsOrders{}
	case "GetDataForMetricsNewOrdersLists.RangeOrdersLists":
		values = &postgresql.GetDataForMetricsOrdersLists{}
	case "UserRole.All":
		values = &postgresql.GetDataForMetricsRole{}
	case "UserGlobal.":
		values = &postgresql.GetDataForMetricsUser{}
	case "User.":
		values = &postgresql.GetDataForMetricsUsers{}
	case "Point.All":
		values = &postgresql.GetDataForMetricsPoint{}
	case "PointRoleCountByTime.Date":
		values = &postgresql.GetDataForMetricsPlan{}
	case "GetDataForMetricsEvents.RangeEvents":
		values = &postgresql.GetDataForMetricsEvents{}
	case "LogTransactionBonus.Date":
		values = &postgresql.GetDataForMetricsBonuses{}
	case "Rashod.SkladDate":
		values = &postgresql.GetDataForMetricsSklad{}
	default:
		//log.Println("Сбор этой метрики не реализован: ", SMS.MST.TableName+"."+SMS.MST.TypeParameter)
		return errors.New("Сбор этой метрики не реализован: " + SMS.MST.TableName + "." + SMS.MST.TypeParameter)
	}

	log.Println("*** BEGIN", SMS.MST.TableName+"."+SMS.MST.TypeParameter)
	fmt.Println("*** BEGIN", SMS.MST.TableName+"."+SMS.MST.TypeParameter)

	/*Начало транзакции в БД*/
	var Transaction postgresql.Transaction
	if err := Transaction.Begin(); err != nil {
		return fmt.Errorf("Transaction.Begin: %v", err)
	}
	defer Transaction.RollBack()

	fmt.Println("\nM:", M, "\n")

	//Начинаем бегать по результату ответа
	for k, val := range M.Tables[0].Values {
		if val == nil {
			return errors.New("критический сбой: cтруктура ответа = nil")
		}

		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("json marshal: %v", err)
		}
		if err := json.Unmarshal(b, &values); err != nil {
			return fmt.Errorf("json unmarshal: %v", err)
		}

		//log.Println("value:", values)
		//fmt.Println("\nvalue:", values)
		//fmt.Print(".")
		if err := InsertInDB(SMS, &Transaction, &metrics, values); err != nil {
			log.Println("InsertInDB:", fmt.Errorf("AddMetricsInDB: %v", err))
			fmt.Println("InsertInDB:", fmt.Errorf("AddMetricsInDB: %v", err))
			return fmt.Errorf("InsertInDB: %v", err)
		}

		fmt.Printf("\r%s\r", "                                                                                ")
		fmt.Printf("%v: %v / %v | %v / 500", SMS.MST.TableName, (k + 1), len(M.Tables[0].Values), SMS.MP.CountInserted)

		if pendingDate.String()[:19] < values.DateMethod().String()[:19] {
			pendingDate = values.DateMethod()
		}
		//fmt.Println("*******", pendingDate)

		pendingID = 0

		// if k%500 == 0 {
		// 	if err := redis.CopyKeys(SMS.MP.ID); err != nil {
		// 		return fmt.Errorf("redis.CopyKeys:(%v) %v", SMS.MP.ID, err)
		// 	}
		// }

		if SMS.MP.CountInserted >= 500 {
			log.Printf("%v: %v / %v | %v / 500", SMS.MST.TableName, (k + 1), len(M.Tables[0].Values), SMS.MP.CountInserted)
			fmt.Print(" begin commit \n")

			if err := Transaction.Commit(); err != nil {
				return fmt.Errorf("Transaction.Commit: %v", err)
			}

			if err := redis.CopyKeys(SMS.MP.ID); err != nil {
				return fmt.Errorf("redis.CopyKeys:(%v) %v", SMS.MP.ID, err)
			}

			if err := Transaction.Begin(); err != nil {
				return fmt.Errorf("Transaction.Begin: %v", err)
			}

			SMS.MP.CountInserted = 0
		}
	}

	log.Println("Update:", SMS.MST.TableName+"."+SMS.MST.TypeParameter)
	fmt.Println("\nUpdate:", SMS.MST.TableName+"."+SMS.MST.TypeParameter)

	///////////
	//if SMS.MP.Protocol_version == 1 {
	//SMS.MP.PendingDate = time.Now().AddDate(0,0,-1)
	//SMS.MP.PendingId = 0
	//}

	if SMS.MP.Protocol_version == 2 {
		var PD postgresql.GetPendingDate

		val := M.Tables[1].Values[0]
		//log.Println("val:", val)
		if val == nil {
			return errors.New("критический сбой: cтруктура ответа ТАБЛИЦА 2 = nil")
		}

		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("json marshal: %v", err)
		}
		if err := json.Unmarshal(b, &PD); err != nil {
			return fmt.Errorf("json unmarshal: %v", err)
		}

		pendingDate = PD.Min_date
		pendingID = PD.Min_id
	}

	updateAt = time.Now()

	// Обновляем инфу в metrics_parameters
	if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_parameters", "PendingDateAndId", SMS.MP.ServiceTableId, pendingDate, pendingID, updateAt); err != nil {
		return fmt.Errorf("Update.metrics_parameters.PendingDateAndId: %v", err)
	}

	log.Println("Update metrics_parameters:", pendingDate, "для", SMS.MST.TableName+"."+SMS.MST.TypeParameter, "- at:", updateAt)
	fmt.Println("\nUpdate metrics_parameters:", pendingDate, "для", SMS.MST.TableName+"."+SMS.MST.TypeParameter, "- at:", updateAt)

	/*Конец Транзакции*/
	if err := Transaction.Commit(); err != nil {
		return fmt.Errorf("Transaction.Commit: %v", err)
	}

	SMS.MP.CountInserted = 0
	SMS.MP.PendingDate = pendingDate
	SMS.MP.PendingId = pendingID
	SMS.MP.UpdateAt = updateAt

	if err := redis.SwitchKeys(SMS.MP.ID); err != nil {
		return fmt.Errorf("redis.SwitchKeys:(%v) %v", SMS.MP.ID, err)
	}

	log.Println("*** END", SMS.MST.TableName+"."+SMS.MST.TypeParameter, " Сбор отработан.")
	fmt.Println("*** END", SMS.MST.TableName+"."+SMS.MST.TypeParameter, " Сбор отработан.")

	return nil
}

func InsertInDB(SMS *postgresql.SMS, Transaction *postgresql.Transaction, metrics *postgresql.MetricsMetrics, values postgresql.MetricValues) error {
	defer Recover()

	//fmt.Println("\n----------------------", "\nHash:\n", hash, "\n", end_hash, "\nSMS.MP.ID:\n", SMS.MP.ID, "\n", endmpid, "\nSMS.MP.Min_Step_ID:\n", SMS.MP.Min_Step_ID, "\n", endmsid, "\nusing_date:\n", using_date[:10], "\n", end_date, "\nSELECT_ID:", SELECT_ID)
	if metrics.Id == nil || values.HashMethod() != metrics.Ownhash || values.DateMethod().String()[:10] != metrics.Date.String()[:10] || SMS.MP.ID != metrics.Parameter_id {
		//log.Println("Проверка наличия SELECT_ID metrics: ")
		if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "", values.DateMethod().String()[:10], SMS.MP.ID, values.HashMethod()); err != nil && err.Error() != "sql: no rows in result set" {
			return fmt.Errorf("SelectID.metrics.: %v", err)
		}

		metrics.Id = Transaction.HashData
		metrics.Ownhash = values.HashMethod()
		metrics.Date = values.DateMethod()
		metrics.Parameter_id = SMS.MP.ID

		if metrics.Id == nil {
			//log.Println("Создание SELECT_ID metrics")
			//fmt.Println("Создание SELECT_ID metrics")
			if err := Transaction.Transaction_QTTV_One(false, "Insert", "metrics", "", values.HashMethod(), Default, values.DateMethod().String()[:10], -1, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
				return fmt.Errorf("Insert.metrics.: %v", err)
			}

			if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "", values.DateMethod().String()[:10], SMS.MP.ID, values.HashMethod()); err != nil && err.Error() != "sql: no rows in result set" {
				return fmt.Errorf("SelectID.metrics. (2): %v", err)
			}
			metrics.Id = Transaction.HashData
		}
	}

	// Вставляем данные в соответствующие таблицы
	if err := values.Insert(SMS, Transaction, metrics); err != nil {
		return fmt.Errorf("values.Insert: %v", err)
	}

	return nil
}

func SelectMessage(conn *net.Conn, M *structures.Message) error {

	Bytes1, err := json.Marshal(M)
	if err != nil {
		return fmt.Errorf("json.Marshal: %v", err)
	}
	if err, _ := fn.Send([]byte(string(Bytes1)), *conn); err != nil {
		return fmt.Errorf("Send: %v", err)
	}
	reply, err := fn.Read(conn, false)
	log.Println("***reply for ", M, ":", string(reply))
	if err := json.Unmarshal([]byte(reply), &M); err != nil {
		return fmt.Errorf("json.Unmarshal: %v", err)
	}

	return nil
}

// func TypeTransform(in *interface{}, out *interface{}) error {

// 	j, err := json.Marshal(in)
// 	if err != nil {
// 		return fmt.Errorf("json marshal: %v", err)
// 	}
// 	if err := json.Unmarshal(j, &out); err != nil {
// 		return fmt.Errorf("json unmarshal: %v", err)
// 	}

// 	return nil
// }
