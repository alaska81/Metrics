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
	"sync"
	"time"

	"MetricsTest/config"
	"MetricsTest/connect"
	fn "MetricsTest/function"
	"MetricsTest/postgresql"

	"MetricsTest/structures"

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

func (SM *StartMetrics) StartMetrics() error {
	//	// Organization_service
	//	log.Println("Начинаю обрабатывать Metrics_dop_data")
	//	var MDD postgresql.Metrics_dop_data
	//	//выкачиваем организации(иерархию) и роли для хранения истории изменений

	//	go func() {
	//		for true {
	//			if err := MDD.Start_Dop_Data(); err != nil {
	//				fmt.Println("Ошибка Metrics_dop_data", err)
	//				log.Println("Ошибка Metrics_dop_data", err)
	//				return
	//			}
	//			fmt.Println("Metrics_dop_data успешно обработана")
	//			log.Println("Metrics_dop_data успешно обработана")
	//			time.Sleep(time.Hour * 2)
	//		}
	//	}()

	/*  -  */
	if err := SM.GetTables(); err != nil {
		return err
	}

	//	var Min int = 5
	//	T := time.Now().Minute()
	//	for T%Min != 0 {
	//		T = time.Now().Minute()
	//		time.Sleep(time.Second)
	//	}

	log.Println("Время старта метрик инициализировано:", time.Now())
	for true {
		go SM.StartComponentMetrics()
		time.Sleep(time.Hour * 1)
	}

	//go Sklad()
	//go Rashod()

	return nil
}

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
			&SMS.MP.ID, &SMS.MP.Interface_ID, &SMS.MP.Type_Mod_ID, &SMS.MP.Own_ID,
			&SMS.MSD.ID, &SMS.MSD.Service_table, &SMS.MSD.End_date, &SMS.MSD.End_ID,
			&SMS.MST.ID, &SMS.MST.Query, &SMS.MST.TableName, &SMS.MST.TypeParameter, &SMS.MST.Service_ID, &SMS.MST.Activ,
			&SMS.MS.ID, &SMS.MS.Name, &SMS.MS.IP); err != nil {
			return err
		}
		SM.SMS_ARRAY = append(SM.SMS_ARRAY, SMS) //Узнаем какие метрики с какими шагами существуют
		Mutex.AddMutex(SMS.MSD.Service_table)
	}
	return nil
}

func (SM *StartMetrics) StartComponentMetrics() {
	log.Println("SM.SMS_ARRAY :", SM.SMS_ARRAY)
	for KEY, _ := range SM.SMS_ARRAY { //Бежим по всем
		if !SM.SMS_ARRAY[KEY].MST.Activ { //Проверка на активность метрики
			continue
		}

		go func(SMS *postgresql.SMS) {
			err := Go_Routines(SMS)
			if err != nil {
				log.Println("ERROR: ", fmt.Errorf("Go_Routines: %v", err))
				fmt.Println("ERROR: ", fmt.Errorf("Go_Routines: %v", err))
			}

		}(&SM.SMS_ARRAY[KEY])
	}
}

func Go_Routines(SMS *postgresql.SMS) error {
	Mutex.Lock(SMS.MSD.Service_table, "local")

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
	M := structures.Message{Query: SMS.MST.Query}
	Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}
	Table.Values = append(Table.Values, SMS.MSD.End_date.String()[:19])
	M.Tables = append(M.Tables, Table)

	Table = structures.Table{Name: SMS.MST.TableName, TypeParameter: "PendingDate"}
	Table.Values = append(Table.Values, SMS.MSD.End_date.String()[:19])
	M.Tables = append(M.Tables, Table)

	log.Println("Запрос на:", SMS.MS.Name, "; Message:", M)
	//fmt.Println("Запрос на:", SMS.MS.Name, "; Message:", M)

	if err := SelectMessage(&conn, &M); err != nil {
		return fmt.Errorf("SelectMessage: %v", err)
	}

	log.Println("Answer Message:", M)
	//fmt.Println("Answer Message:", M)

	if len(M.Tables) != 0 {
		if err := AddMetricsInDB(SMS, &M); err != nil {
			return fmt.Errorf("AddMetricsInDB: %v", err)
		}

	}

	Mutex.Unlock(SMS.MSD.Service_table, "local")

	return nil
}

func AddMetricsInDB(SMS *postgresql.SMS, M *structures.Message) error {
	var metrics postgresql.MetricsMetrics
	var values postgresql.MetricValues

	switch SMS.MST.TableName + "." + SMS.MST.TypeParameter {
	case "GetDataForMetricsNewCashbox.RangeCashbox":
		values = &postgresql.GetDataForMetricsCashbox{}
	case "GetDataForMetricsNewOrders.RangeOrders":
		values = &postgresql.GetDataForMetricsOrders{}
	case "GetDataForMetricsNewOrdersLists.RangeOrdersLists":
		values = &postgresql.GetDataForMetricsOrdersLists{}
	default:
		//log.Println("Сбор этой метрики не реализован: ", SMS.MST.TableName+"."+SMS.MST.TypeParameter)
		return errors.New("Сбор этой метрики не реализован: " + SMS.MST.TableName + "." + SMS.MST.TypeParameter)
	}

	log.Println("******** BEGIN ********")
	fmt.Println("\n******** BEGIN ********")
	log.Println(SMS.MST.TableName + "." + SMS.MST.TypeParameter)
	fmt.Println(SMS.MST.TableName + "." + SMS.MST.TypeParameter)

	/*Начало транзакции в БД*/
	log.Println("Открыл транзакцию")
	fmt.Println("Открыл транзакцию\n")
	var Transaction postgresql.Transaction
	if err := Transaction.Begin(); err != nil {
		return fmt.Errorf("Transaction.Begin: %v", err)
	}
	defer Transaction.RollBack()

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

		log.Println("value:", values)
		//fmt.Println("\nvalue:", values)
		//fmt.Print(".")
		fmt.Print("\r")
		fmt.Printf("%v: %v / %v", SMS.MST.TableName+"."+SMS.MST.TypeParameter, k, len(M.Tables[0].Values))

		if err := InsertInDB(SMS, &Transaction, &metrics, values); err != nil {
			log.Println("InsertInDB:", fmt.Errorf("AddMetricsInDB: %v", err))
			return fmt.Errorf("AddMetricsInDB: %v", err)
		}
	}

	log.Println("Update:", SMS.MST.TableName+"."+SMS.MST.TypeParameter)
	fmt.Println("\nUpdate:", SMS.MST.TableName+"."+SMS.MST.TypeParameter)

	///////////
	var PD postgresql.GetPendingDate
	val := M.Tables[1].Values[0]
	log.Println("val:", val)
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

	log.Println("Update metrics_service_data:", PD)
	fmt.Println("\nUpdate metrics_service_data:", PD)

	// Обновляем инфу в metrics_service_data
	if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_service_data", "Id-end_dateId-end_id", SMS.MSD.ID, PD.Min_date.String()[:19], PD.Min_id); err != nil {
		return fmt.Errorf("Update.metrics_service_data.Id-end_dateId-end_id: %v", err)
	}

	/*Конец Транзакции*/
	if err := Transaction.Commit(); err != nil {
		return fmt.Errorf("Transaction.Commit: %v", err)
	}

	log.Println("Закрыл транзакцию")
	fmt.Println("\nЗакрыл транзакцию")

	log.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	fmt.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")

	log.Println("******** END ********")
	fmt.Println("******** END ********\n")
	return nil
}

func InsertInDB(SMS *postgresql.SMS, Transaction *postgresql.Transaction, metrics *postgresql.MetricsMetrics, values postgresql.MetricValues) error {
	defer Recover()

	//fmt.Println("\n----------------------", "\nHash:\n", hash, "\n", end_hash, "\nSMS.MP.ID:\n", SMS.MP.ID, "\n", endmpid, "\nSMS.MP.Min_Step_ID:\n", SMS.MP.Min_Step_ID, "\n", endmsid, "\nusing_date:\n", using_date[:10], "\n", end_date, "\nSELECT_ID:", SELECT_ID)
	if metrics.Id == nil || values.Hash() != metrics.Ownhash || values.Date().String()[:10] != metrics.Date.String()[:10] || SMS.MP.ID != metrics.Parameter_id {
		log.Println("Проверка наличия SELECT_ID metrics: ")
		if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "", values.Date().String()[:10], SMS.MP.ID, values.Hash()); err != nil && err.Error() != "sql: no rows in result set" {
			return fmt.Errorf("SelectID.metrics.: %v", err)
		}

		metrics.Id = Transaction.HashData
		metrics.Ownhash = values.Hash()
		metrics.Date = values.Date()
		metrics.Parameter_id = SMS.MP.ID

		if metrics.Id == nil {
			log.Println("Создание SELECT_ID metrics")
			//fmt.Println("Создание SELECT_ID metrics")
			if err := Transaction.Transaction_QTTV_One(false, "Insert", "metrics", "", values.Hash(), Default, values.Date().String()[:10], -1, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
				return fmt.Errorf("Insert.metrics.: %v", err)
			}

			if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "", values.Date().String()[:10], SMS.MP.ID, values.Hash()); err != nil && err.Error() != "sql: no rows in result set" {
				return fmt.Errorf("SelectID.metrics. (2): %v", err)
			}
			metrics.Id = Transaction.HashData
		}
	}

	// Вставляем данные в соответствующие таблицы
	values.Insert(Transaction, metrics)

	return nil
}

func SelectMessage(conn *net.Conn, M *structures.Message) error {

	Bytes1, err := json.Marshal(M)
	if err != nil {
		return err
	}
	if err, _ := fn.Send([]byte(string(Bytes1)), *conn); err != nil {
		return err
	}
	reply, err := fn.Read(conn, false)
	log.Println("***reply for ", M, ":", string(reply))
	if err := json.Unmarshal([]byte(reply), &M); err != nil {
		return err
	}

	return nil
}

func EndIDFor(SMS *postgresql.SMS) error {
	defer Recover()

	ConnOrder, err := connect.CreateConnect(&SMS.MS.IP)
	if err != nil {
		return err
	}
	defer ConnOrder.Close()

	log.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter)
	fmt.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter)

	/*Транзакция*/
	var Transaction postgresql.Transaction
	if err := Transaction.Begin(); err != nil {
		return err
	}
	defer Transaction.RollBack()

	log.Println("Запуск новый день:", " Начало:", SMS.MSD.End_ID)
	fmt.Println("Запуск новый день:", " Начало:", SMS.MSD.End_ID)

	Q := structures.Message{Query: SMS.MST.Query}
	var using_date string
	var using_id int64 //ID для запроса
	var max_id int64   //ID для сохранения итога -ВЕТЕР В ВОЛОСАХ

	var endhash string
	var endmpid int64
	var endmsid int64
	var enddate string
	var SELECT_ID interface{}

	Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}
	Table.Values = append(Table.Values, SMS.MSD.End_ID)
	Q.Tables = append(Q.Tables, Table)

	log.Println("Запрос:", Q)
	fmt.Println("Запрос:", Q)

	Answer_Message, err := connect.SelectMessage(&ConnOrder, Q) //Спросили по промежутку или чего-то там
	if err != nil {
		return err
	}

	log.Println("Answer_Message:", Answer_Message)
	fmt.Println("Answer_Message:", Answer_Message)

	if len(Answer_Message.Tables) != 0 { //Если данных не прилетело, то не надо в метрику данные записывать
		for _, VAL := range Answer_Message.Tables[0].Values { //Начинаем бегать по результату ответа
			if VAL == nil {
				return errors.New("Критический сбой: cтруктура ответа = nil")
			}
			var Answer structures.Common_Answer_cashbox
			B, err := json.Marshal(VAL)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(B, &Answer); err != nil {
				return err
			} //Парсим ответ

			log.Println("\nAnswer_Cashbox:", Answer)
			fmt.Println("\nAnswer_Cashbox:", Answer)

			if using_id != Answer.CashRegister {
				using_id = Answer.CashRegister
				using_date = fn.FormatDate(Answer.Action_time)
				if max_id < using_id { // -ВЕТЕР В ВОЛОСАХ
					max_id = using_id
				}
			}

			var Hash string

			switch SMS.MS.ID {
			case 8:
				switch SMS.MS.ID {
				case 8:
					Hash = Answer.PointHash
				}
			}

			if Hash != endhash || endmpid != SMS.MP.ID || endmsid != SMS.MP.Min_Step_ID || enddate != using_date[:10] || SELECT_ID == nil {
				fmt.Println("\n----------------------", "\nHash:\n", Hash, "\n", endhash, "\nSMS.MP.ID:\n", SMS.MP.ID, "\n", endmpid, "\nSMS.MP.Min_Step_ID:\n", SMS.MP.Min_Step_ID, "\n", endmsid, "\nusing_date:\n", using_date[:10], "\n", enddate, "\nSELECT_ID:", SELECT_ID)
				if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "DateStep_idParameter_idMS("+fmt.Sprint(SMS.MP.Min_Step_ID)+")", using_date, SMS.MP.ID, Hash); err != nil && err.Error() != "sql: no rows in result set" {
					fmt.Println("\n\nERROR DateStep_idParameter_idMS:", err)
					log.Println("\n\nERROR DateStep_idParameter_idMS:", err)
					return err
				}
				endhash = Hash
				endmpid = SMS.MP.ID
				endmsid = SMS.MP.Min_Step_ID
				enddate = using_date[:10]
				fmt.Println("Читали SELECT_ID: ", Transaction.HashData, "\n----------------------")
				SELECT_ID = Transaction.HashData
			}

			switch SMS.MS.ID {
			case 8: // 8 - Cashbox - Кассовый отчёт
				//				if err = Transaction.Transaction_Insert_Cashbox(SELECT_ID, SMS, &using_date, &Answer, true); err != nil {
				//					log.Println(SMS, "\n\nCashbox ERROR:", err.Error(), "\n\n")
				//					fmt.Println(SMS, "\n\nCashbox ERROR:", err.Error(), "\n\n")
				//				}
			default:
				log.Println("\n\nEndIDFor - default SMS.MS.ID (service_id): ", SMS.MS.ID)
				log.Println("!!!\nSMS: ", SMS)
			}

		}
		if len(Answer_Message.Tables[0].Values) > 0 {

			SMS.MSD.End_ID = max_id
			if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_service_data", "Id-end_dateId-end_id", SMS.MSD.ID, using_date, max_id); err != nil {
				return err
			}

			log.Println("Увеличил ID:", max_id)
			fmt.Println("Увеличил ID:", max_id)

			/*Конец Транзакции*/
			if err := Transaction.Commit(); err != nil {
				return err
			}
			log.Println("Закрыл\n-------------\n")
			fmt.Println("Закрыл\n-------------\n")
		}

	}
	log.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	fmt.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	return nil
}

func EndIDForOrders(SMS *postgresql.SMS) error {
	defer Recover()

	conn, err := connect.CreateConnect(&SMS.MS.IP)
	if err != nil {
		return fmt.Errorf("CreateConnect: %v", err)
	}
	defer conn.Close()

	log.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))
	fmt.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))

	/*Начало транзакции в БД*/
	var Transaction postgresql.Transaction
	if err := Transaction.Begin(); err != nil {
		return fmt.Errorf("Transaction.Begin: %v", err)
	}
	defer Transaction.RollBack()

	log.Println("Запуск новый ID:", " Начало:", SMS.MSD.End_ID)
	fmt.Println("Запуск новый ID:", " Начало:", SMS.MSD.End_ID)

	var using_date string
	var max_id int64 //ID для сохранения максимального id

	var end_hash string
	var end_date string
	var SELECT_ID interface{}

	// Запрос данных у стороннего сервиса (SpecOrders)
	Q := structures.Message{Query: SMS.MST.Query}
	Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}
	Table.Values = append(Table.Values, SMS.MSD.End_ID)
	Q.Tables = append(Q.Tables, Table)

	log.Println("Запрос:", Q)
	fmt.Println("Запрос:", Q)

	Answer_Message, err := connect.SelectMessage(&conn, Q)
	if err != nil {
		return fmt.Errorf("Answer_Message: %v", err)
	}

	log.Println("Answer_Message:", Answer_Message)

	if len(Answer_Message.Tables) != 0 { //Если данных не прилетело, то не надо в метрику данные записывать
		for _, VAL := range Answer_Message.Tables[0].Values { //Начинаем бегать по результату ответа
			if VAL == nil {
				return errors.New("критический сбой: Answer_Message cтруктура ответа = nil")
			}

			var answer structures.GetDataForMetricsOrders
			j, err := json.Marshal(VAL)
			if err != nil {
				return fmt.Errorf("json marshal: %v", err)
			}
			if err := json.Unmarshal(j, &answer); err != nil {
				return fmt.Errorf("json unmarshal: %v", err)
			}

			log.Println("answer:", answer)
			fmt.Println("\nanswer:", answer)

			var hash string = answer.Point_hash
			using_date = fn.FormatDate(answer.Creator_time)

			// получаем ID из metrics /SelectID.metrics.DateStep_idParameter_idMS(3)/
			fmt.Println("\n----------------------", "\nHash: ", hash, "\n", end_hash, "\nusing_date: ", using_date[:10], "\nSELECT_ID:", SELECT_ID)
			if hash != end_hash || end_date[:10] != using_date[:10] || SELECT_ID == nil {
				//SMS.MP.ID = 48
				//SMS.MP.Min_Step_ID = 3

				if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "DateStep_idParameter_idMS("+fmt.Sprint(SMS.MP.Min_Step_ID)+")", using_date, SMS.MP.ID, hash); err != nil && err.Error() != "sql: no rows in result set" {
					return fmt.Errorf("SelectID.metrics.DateStep_idParameter_idMS: %v", err)
				}

				end_hash = hash
				end_date = using_date
				fmt.Println("Получили SELECT_ID: ", Transaction.HashData, "\n----------------------")
				SELECT_ID = Transaction.HashData
			}

			// вставляем данные в metrics_orders_info и metrics
			//ok := false
			//if _, err = Transaction.Transaction_Insert_OrdersInfo(&SELECT_ID, SMS, &using_date, &answer); err != nil {
			//	return fmt.Errorf("Transaction_Insert_OrdersInfo: %v", err)
			//}
			//			if ok {
			//				// вставляем данные в metrics_orders_list_info
			//				if err = EndIDForOrdersList(SMS, &Transaction, SELECT_ID, &answer); err != nil {
			//					return fmt.Errorf("func EndIDForOrdersList: %v", err)
			//				}
			//			}

			if max_id < answer.Order_id {
				max_id = answer.Order_id
			}
		}

		if max_id > 0 {
			// обновляем End_ID в metrics_service_data
			SMS.MSD.End_ID = max_id
			if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_service_data", "Id-end_dateId-end_id", SMS.MSD.ID, using_date, max_id); err != nil {
				return fmt.Errorf("Update.metrics_service_data: %v", err)
			}
			log.Println("Увеличил ID:", max_id)
			fmt.Println("Увеличил ID:", max_id)
		}

		/*Конец Транзакции*/
		if err := Transaction.Commit(); err != nil {
			return fmt.Errorf("Transaction.Commit: %v", err)
		}
	}

	log.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	fmt.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	return nil
}

func EndDateFor(SMS *postgresql.SMS) error {
	defer Recover()

	ConnOrder, err := connect.CreateConnect(&SMS.MS.IP)
	if err != nil {
		return err
	}
	defer ConnOrder.Close()

	log.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))
	fmt.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))

	var MDD postgresql.Metrics_dop_data
	var endhash string
	var endmpid int64
	var endmsid int64
	var enddate string
	var SELECT_ID interface{}
	/*Транзакция*/
	for NextStep(SMS) { //Если

		log.Println("Запуск новый день:", " Начало:", SMS.MSD.End_date, "-", SMS.MSD.StartDate, " SMS.MP.Min_Step_ID:", SMS.MP.Min_Step_ID)
		fmt.Println("Запуск новый день:", " Начало:", SMS.MSD.End_date, "-", SMS.MSD.StartDate, " SMS.MP.Min_Step_ID:", SMS.MP.Min_Step_ID)

		var Transaction postgresql.Transaction
		if err := Transaction.Begin(); err != nil {
			return err
		}
		defer Transaction.RollBack()

		Q := structures.Message{Query: SMS.MST.Query}
		Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}
		var using_date string
		if using_date, Table.Values, err = SetTimeStep(SMS, true); err != nil {
			return err
		}
		Q.Tables = append(Q.Tables, Table)

		log.Println("Запрос:", Q)
		fmt.Println("Запрос:", Q)

		Answer_Message, err := connect.SelectMessage(&ConnOrder, Q) //Спросили по промежутку или чего то там
		if err != nil {
			fmt.Println("_________________________________________________")
			return err
		}
		log.Println("Answer_Message:", Answer_Message)
		fmt.Println("Answer_Message:", Answer_Message)

		//return err

		if len(Answer_Message.Tables) != 0 {
			for _, VAL := range Answer_Message.Tables[0].Values { //Начинаем бегать по результату ответа
				var Answer structures.Common_Answer
				Byte, err := json.Marshal(VAL)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(Byte, &Answer); err != nil {
					return err
				}

				log.Println("\nAnswer:", Answer)
				fmt.Println("\nAnswer:", Answer)

				var Hash string

				log.Println("\nSMS.MS.ID (service_id): ", SMS.MS.ID)
				fmt.Println("\nSMS.MS.ID (service_id): ", SMS.MS.ID)

				switch SMS.MS.ID {
				case 1, 2, 5, 6:
					switch SMS.MS.ID {
					case 1:
						Hash = Answer.Sklad
					case 2:
						Hash = Answer.Str
					case 5:
						Hash = Answer.OrgHash
					case 6:
						Hash = Answer.Hash
					}
					if enddate != using_date[:10] || Hash != endhash {
						if err := MDD.Select("Select", "metrics_dop_data", "Franchise_hierarchy", Hash, using_date[:10]); err != nil {
							if err.Error() != "sql: no rows in result set" {
								return err
							}
							MDD.MFH.Hash, MDD.MFH.Parent_hash, MDD.MFH.Name = Hash, "НЕ ИДЕНТИФИЦИРОВАН", "НЕ ИДЕНТИФИЦИРОВАН"
						}
					}
				case 3, 4, 7:
					Hash = Answer.Str
				}

				if Hash != endhash || endmpid != SMS.MP.ID || endmsid != SMS.MP.Min_Step_ID || enddate != using_date[:10] || SELECT_ID == nil {
					fmt.Println("\n----------------------", "\nHash:\n", Hash, "\n", endhash, "\nSMS.MP.ID:\n", SMS.MP.ID, "\n", endmpid, "\nSMS.MP.Min_Step_ID:\n", SMS.MP.Min_Step_ID, "\n", endmsid, "\nusing_date:\n", using_date[:10], "\n", enddate, "\nSELECT_ID:", SELECT_ID)
					if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "DateStep_idParameter_idMS("+fmt.Sprint(SMS.MP.Min_Step_ID)+")", using_date, SMS.MP.ID, Hash); err != nil && err.Error() != "sql: no rows in result set" {
						return err
					}
					endhash = Hash
					endmpid = SMS.MP.ID
					endmsid = SMS.MP.Min_Step_ID
					enddate = using_date[:10]
					fmt.Println("Читали SELECT_ID: ", Transaction.HashData, "\n----------------------")
					SELECT_ID = Transaction.HashData
				}

				switch SMS.MS.ID {
				case 1: // 1 - склад
					if err := Transaction.Transaction_Insert_Sklad(SELECT_ID, SMS, &MDD, &using_date, &Answer, true); err != nil {
						log.Println(SMS, "\n\nSklad ERROR:", err.Error(), "\n\n")
						fmt.Println(SMS, "\n\nSklad ERROR:", err.Error(), "\n\n")
						return err
					}
				case 2: // 2 - точки (???)
					/*
						RangeByDayAverageSumOrdersForPoint
						RangeByDayCountOrderPoint
						RangeByDayCountOrderPointWithWriteOff
						RangeByDayCountOrderPointWithNotWriteOff
						RangeByDayTypeOrder
						RangeCountOrdersWithdrawn
						RangeCountOrdersDiscount
						RangeCountOrdersNotDiscount
						!RangeCountOrdersSumDiscount 31
						RangeCountOrdersCountItemsAlteration
						RangeCountOrdersCountDish
						RangeCountOrdersCountDishDelivery

						RangeCountPaymentsByPayment
						RangeSumPaymentsByPayment
						RangeCountOrdersByChanelCreate

						!RangeAverageTimePreparationOrders 45(нет данных)

						!RangeAverageTimeDeliveryOrders 46(нет данных) //Вернул какое то одно время? А надо хеш точки и время как и везде.
						!RangeAverageTimeToCook 47(нет данных)
						RangeStructOrderQueuePerHour ?
					*/
					switch SMS.MST.TypeParameter {
					case "RangeByDayTypeOrder", "RangeCountOrdersCountDish", "RangeCountPaymentsByPayment", "RangeSumPaymentsByPayment", "RangeCountOrdersByChanelCreate":
						{
							if err = Transaction.Transaction_Insert_Points(SELECT_ID, SMS, &MDD, &using_date, &Answer, true); err != nil {
								log.Println(SMS, "\n\nPoints ERROR:", err.Error(), "\n\n")
								fmt.Println(SMS, "\n\nPoints ERROR:", err.Error(), "\n\n")
								return err
							}
						}
					default:
						{
							if err = Transaction.Transaction_Insert_Points(SELECT_ID, SMS, &MDD, &using_date, &Answer, false); err != nil {
								log.Println(SMS, "\n\nPoints ERROR:", err.Error(), "\n\n")
								fmt.Println(SMS, "\n\nPoints ERROR:", err.Error(), "\n\n")
								return err
							}
						}
					}

				case 3: // 3 - повар
					if err = Transaction.Transaction_Insert_Cook(SMS, &Default, &using_date, &Answer, false); err != nil {
						log.Println(SMS, "\n\nCook ERROR:", err.Error(), "\n\n")
						fmt.Println(SMS, "\n\nCook ERROR:", err.Error(), "\n\n")
						return err
					}
				case 4: // 4 - оператора
					if err = Transaction.Transaction_Insert_Metrics_Cook(SELECT_ID, SMS, &Default, &using_date, &Answer, false); err != nil {
						log.Println(SMS, "\n\nOperator ERROR:", err.Error(), "\n\n")
						fmt.Println(SMS, "\n\nOperator ERROR:", err.Error(), "\n\n")
						return err
					}
				case 5: // 5 - Кто сколько отработал
					if err := Transaction.Transaction_Insert_Session(SELECT_ID, SMS, &MDD, &Answer, &using_date, true); err != nil {
						log.Println(SMS, "\n\nSession ERROR:", err.Error(), "\n\n")
						fmt.Println(SMS, "\n\nSession ERROR:", err.Error(), "\n\n")
						return err
					}
				case 6: // 6 - Какие эл. меню по точкам сколько и цена(ОТЧЁТ ПО ПРОДАЖАМ)
					if err = Transaction.NEW_Transaction_Insert_Points(SELECT_ID, SMS, &MDD, &using_date, &Answer, true); err != nil {
						log.Println(SMS, "\n\nNewPoints ERROR:", err.Error(), "\n\n")
						fmt.Println(SMS, "\n\nNewPoints ERROR:", err.Error(), "\n\n")
						return err
					}
				case 7: // 7 - Массивы ID доставленных заказов по курьерам
					if err = Transaction.Transaction_Insert_Courier(SELECT_ID, SMS, &using_date, &Answer, true); err != nil {
						log.Println(SMS, "\n\nCourier ERROR:", err.Error(), "\n\n")
						fmt.Println(SMS, "\n\nCourier ERROR:", err.Error(), "\n\n")
						return err
					}
					//				case 9: // 9 - OrdersInfo - Инфрмация о заказах
					//					if err = Transaction.Transaction_Insert_OrdersInfo(SELECT_ID, SMS, &using_date, &Answer, true); err != nil {
					//						log.Println(SMS, "\n\nOrdersInfo ERROR:", err.Error(), "\n\n")
					//						fmt.Println(SMS, "\n\nOrdersInfo ERROR:", err.Error(), "\n\n")
					//						return err
					//					}
				default:
					log.Println("\n\nEndDateFor - default SMS.MS.ID (service_id): ", SMS.MS.ID)
					log.Println("!!!\nSMS: ", SMS)
				}
			}
		}

		log.Println("Выполнил с датой:", using_date)
		fmt.Println("Выполнил с датой:", using_date)

		if _, _, err = SetTimeStep(SMS, false); err != nil {
			return err
		}
		if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_service_data", "Id-end_date", SMS.MSD.ID, fn.FormatDate(SMS.MSD.End_date)); err != nil {
			return err
		}

		log.Println("Увеличил дату:", SMS.MSD.End_date)
		fmt.Println("Увеличил дату:", SMS.MSD.End_date)

		if err := Transaction.Commit(); err != nil {
			log.Println("Ошибка закрытия\n-------------\n")
			fmt.Println("Ошибка закрытия\n-------------\n")
			return err
		}

		/*Конец Транзакции*/
		log.Println("Закрыл\n-------------\n")
		fmt.Println("Закрыл\n-------------\n")

		fmt.Println("Последние TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter)
		fmt.Println("SMS.MSD.StartDate:", SMS.MSD.StartDate, " SMS.MSD.End_date:", SMS.MSD.End_date, " NextStep:", NextStep(SMS))
	}
	log.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	fmt.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	return nil
}

func EndIDForOrdersList(SMS *postgresql.SMS) error {
	defer Recover()

	conn, err := connect.CreateConnect(&SMS.MS.IP)
	if err != nil {
		return fmt.Errorf("CreateConnect: %v", err)
	}
	defer conn.Close()

	log.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))
	fmt.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))

	/*Начало транзакции в БД*/
	var Transaction postgresql.Transaction
	if err := Transaction.Begin(); err != nil {
		return fmt.Errorf("Transaction.Begin: %v", err)
	}
	defer Transaction.RollBack()

	log.Println("Запуск новый ID:", " Начало:", SMS.MSD.End_ID)
	fmt.Println("Запуск новый ID:", " Начало:", SMS.MSD.End_ID)

	var using_date string
	var max_id int64 //ID для сохранения максимального id

	var end_hash string
	var end_date string
	var SELECT_ID interface{}

	// Запрос данных у стороннего сервиса (SpecOrders)
	Q := structures.Message{Query: SMS.MST.Query}
	Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}
	Table.Values = append(Table.Values, SMS.MSD.End_ID)
	Q.Tables = append(Q.Tables, Table)

	log.Println("Запрос на SpecOrders:", Q)
	fmt.Println("Запрос на SpecOrders:", Q)

	Answer_Message, err := connect.SelectMessage(&conn, Q)
	if err != nil {
		return fmt.Errorf("Answer_Message: %v", err)
	}

	log.Println("Answer_Message:", Answer_Message)

	if len(Answer_Message.Tables) != 0 { //Если данных не прилетело, то не надо в метрику данные записывать
		for _, VAL := range Answer_Message.Tables[0].Values { //Начинаем бегать по результату ответа
			if VAL == nil {
				return errors.New("критический сбой: Answer_Message cтруктура ответа = nil")
			}

			var answer structures.GetDataForMetricsOrdersLists
			j, err := json.Marshal(VAL)
			if err != nil {
				return fmt.Errorf("json marshal: %v", err)
			}
			if err := json.Unmarshal(j, &answer); err != nil {
				return fmt.Errorf("json unmarshal: %v", err)
			}

			log.Println("answer:", answer)
			fmt.Println("\nanswer:", answer)

			//велосипед для получения food_cost со склада
			//			if err = Real_food_cost(SMS, &answer); err != nil {
			//				return fmt.Errorf("func Real_food_cost: %v", err)
			//			}

			var hash string = answer.Point_hash
			using_date = fn.FormatDate(answer.Start_time)

			// получаем ID из metrics /SelectID.metrics.DateStep_idParameter_idMS(3)/
			fmt.Println("\n----------------------", "\nHash: ", hash, "\n", end_hash, "\nusing_date: ", using_date[:10], "\nSELECT_ID:", SELECT_ID)
			log.Println("\n----------------------", "\nHash: ", hash, "\n", end_hash, "\nusing_date: ", using_date[:10], "\nSELECT_ID:", SELECT_ID)
			if hash != end_hash || end_date[:10] != using_date[:10] || SELECT_ID == nil {
				//SMS.MP.ID = 48
				//SMS.MP.Min_Step_ID = 3

				if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "DateStep_idParameter_idMS("+fmt.Sprint(SMS.MP.Min_Step_ID)+")", using_date, SMS.MP.ID, hash); err != nil && err.Error() != "sql: no rows in result set" {
					return fmt.Errorf("SelectID.metrics.DateStep_idParameter_idMS: %v", err)
				}

				end_hash = hash
				end_date = using_date
				fmt.Println("Получили SELECT_ID: ", Transaction.HashData, "\n----------------------")
				SELECT_ID = Transaction.HashData
			}

			// вставляем данные в metrics_orders_list info
			//			if err = Transaction.Transaction_Insert_OrdersListInfo(SELECT_ID, SMS, &using_date, &answer); err != nil {
			//				return fmt.Errorf("Transaction_Insert_OrdersListInfo: %v", err)
			//			}

			//			if max_id < answer.Order_id {
			//				max_id = answer.Order_id
			//			}
		}

		if max_id > 0 {
			// обновляем End_ID в metrics_service_data
			SMS.MSD.End_ID = max_id
			if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_service_data", "Id-end_dateId-end_id", SMS.MSD.ID, using_date, max_id); err != nil {
				return fmt.Errorf("Update.metrics_service_data: %v", err)
			}
			log.Println("Увеличил ID:", max_id)
			fmt.Println("Увеличил ID:", max_id)
		}

		/*Конец Транзакции*/
		if err := Transaction.Commit(); err != nil {
			return fmt.Errorf("Transaction.Commit: %v", err)
		}
	}

	log.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	fmt.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	return nil
}

func EndIDFor2(SMS *postgresql.SMS) error {
	defer Recover()

	conn, err := connect.CreateConnect(&SMS.MS.IP)
	if err != nil {
		return fmt.Errorf("CreateConnect: %v", err)
	}
	defer conn.Close()

	log.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))
	fmt.Println("К сервису подключился ", SMS.MS.IP, " TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " NextStep:", NextStep(SMS))

	/*Начало транзакции в БД*/
	var Transaction postgresql.Transaction
	if err := Transaction.Begin(); err != nil {
		return fmt.Errorf("Transaction.Begin: %v", err)
	}
	defer Transaction.RollBack()

	log.Println("Запуск новый ID:", " Начало:", SMS.MSD.End_ID)
	fmt.Println("Запуск новый ID:", " Начало:", SMS.MSD.End_ID)

	var using_date string
	var max_id int64 //ID для сохранения максимального id

	var end_hash string
	var end_date string
	var SELECT_ID interface{}

	// Запрос данных у стороннего сервиса (SpecOrders)
	Q := structures.Message{Query: SMS.MST.Query}
	Table := structures.Table{Name: SMS.MST.TableName, TypeParameter: SMS.MST.TypeParameter}
	Table.Values = append(Table.Values, SMS.MSD.End_ID)
	Q.Tables = append(Q.Tables, Table)

	log.Println("Запрос на SpecOrders:", Q)
	fmt.Println("Запрос на SpecOrders:", Q)

	Answer_Message, err := connect.SelectMessage(&conn, Q)
	if err != nil {
		return fmt.Errorf("Answer_Message: %v", err)
	}

	log.Println("Answer_Message:", Answer_Message)

	if len(Answer_Message.Tables) != 0 { //Если данных не прилетело, то не надо в метрику данные записывать
		for _, VAL := range Answer_Message.Tables[0].Values { //Начинаем бегать по результату ответа
			if VAL == nil {
				return errors.New("критический сбой: Answer_Message cтруктура ответа = nil")
			}

			var answer postgresql.MetricValues

			//answer = &GetDataForMetricsOrdersLists{}

			j, err := json.Marshal(VAL)
			if err != nil {
				return fmt.Errorf("json marshal: %v", err)
			}
			if err := json.Unmarshal(j, &answer); err != nil {
				return fmt.Errorf("json unmarshal: %v", err)
			}

			//answer = jresult //.(structures.GetDataForMetricsOrdersLists)
			//answer = jresult.(reflect.TypeOf("GetDataForMetricsOrdersLists"))

			log.Println("answer:", answer)
			fmt.Println("\nanswer:", answer)

			var hash string = answer.Hash()
			using_date = fn.FormatDate(answer.Date())

			// получаем ID из metrics /SelectID.metrics.DateStep_idParameter_idMS(3)/
			fmt.Println("\n----------------------", "\nHash: ", hash, "\n", end_hash, "\nusing_date: ", using_date[:10], "\nSELECT_ID:", SELECT_ID)
			log.Println("\n----------------------", "\nHash: ", hash, "\n", end_hash, "\nusing_date: ", using_date[:10], "\nSELECT_ID:", SELECT_ID)
			if hash != end_hash || end_date[:10] != using_date[:10] || SELECT_ID == nil {
				//MetricsOrders		 SMS.MP.ID = 48 SMS.MP.Min_Step_ID = 3
				//MetricsOrdersLists SMS.MP.ID = 49 SMS.MP.Min_Step_ID = 3

				if err := Transaction.Transaction_QTTV_One(true, "SelectID", "metrics", "DateStep_idParameter_idMS("+fmt.Sprint(SMS.MP.Min_Step_ID)+")", using_date, SMS.MP.ID, hash); err != nil && err.Error() != "sql: no rows in result set" {
					return fmt.Errorf("SelectID.metrics.DateStep_idParameter_idMS: %v", err)
				}

				end_hash = hash
				end_date = using_date
				fmt.Println("Получили SELECT_ID: ", Transaction.HashData, "\n----------------------")
				SELECT_ID = Transaction.HashData
			}

			//велосипед для получения food_cost со склада
			//			if err = Real_food_cost(SMS, answer.(*structures.GetDataForMetricsOrdersLists)); err != nil {
			//				return fmt.Errorf("func Real_food_cost: %v", err)
			//			}

			//вставляем данные в metrics_orders_list info
			//			if err = Transaction.Transaction_Insert_OrdersListInfo(SELECT_ID, SMS, &using_date, answer.(*GetDataForMetricsOrdersLists)); err != nil {
			//				return fmt.Errorf("Transaction_Insert_OrdersListInfo: %v", err)
			//			}

			//			if max_id < answer.OrderId() {
			//				max_id = answer.OrderId()
			//			}
		}

		if max_id > 0 {
			// обновляем End_ID в metrics_service_data
			SMS.MSD.End_ID = max_id
			if err := Transaction.Transaction_QTTV_One(false, "Update", "metrics_service_data", "Id-end_dateId-end_id", SMS.MSD.ID, &using_date, max_id); err != nil {
				return fmt.Errorf("Update.metrics_service_data: %v", err)
			}
			log.Println("Увеличил ID:", max_id)
			fmt.Println("Увеличил ID:", max_id)
		}

		/*Конец Транзакции*/
		if err := Transaction.Commit(); err != nil {
			return fmt.Errorf("Transaction.Commit: %v", err)
		}
	}

	log.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	fmt.Println("TABLE:", SMS.MST.TableName, " Type:", SMS.MST.TypeParameter, " Сбор отработан.")
	return nil
}

func TypeTransform(in *interface{}, out *interface{}) error {

	j, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("json marshal: %v", err)
	}
	if err := json.Unmarshal(j, &out); err != nil {
		return fmt.Errorf("json unmarshal: %v", err)
	}

	return nil
}
