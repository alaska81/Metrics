package postgresql

import (
	"MetricsTest/config"
	"MetricsTest/connect"
	fn "MetricsTest/function"
	"MetricsTest/structures"

	//	"database/sql"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

var Default string = "DEFAULT"
var DefaultFloat64 float64 = -1

var DefaultDate string = "2017-09-20"

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
	return err
}

//Закрывает транзакцию
func (T *Transaction) RollBack() {
	T.Tx.Rollback()
}

//Закрывает транзакцию
func (T *Transaction) Commit() error {
	return T.Tx.Commit()
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
	log.Println("Запрос (TQTTVO): ", Query+"."+Table+"."+Type, "\nПараметры: ", Values)
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

func (T *Transaction) NEW_Transaction_Insert_Points(SELECT_ID interface{}, SMS *SMS, MDD *Metrics_dop_data, Date *string, Answer *structures.Common_Answer, Add bool) error {

	var err error
	if SELECT_ID == nil {
		log.Println("Создание SELECT_ID metrics")
		fmt.Println("Создание SELECT_ID metrics")
		if err = T.Transaction_QTTV_One(Add, "Insert", "metrics", "", MDD.MFH.Hash, MDD.MFH.Name, Date, -1, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
			return err
		}
		SELECT_ID = T.HashData
	}

	log.Println("SELECT_ID:", SELECT_ID)
	fmt.Println("SELECT_ID:", SELECT_ID)

	if Add {
		if err = T.Transaction_QTTV_One(true, "Select", "metrics_add_info", "JSONMetric_idPrice_id", SELECT_ID, fmt.Sprint(Answer.Price_id)); err != nil && err.Error() != "sql: no rows in result set" {
			return err
		}
		if T.HashData == nil {
			var real_food_cost interface{} = 0.00

			//RangeCountAndSumProductsByPoint - оно тут
			if SMS.MST.TypeParameter == "RangeCountAndSumProductsByPoint" {
				fc := structures.FoodCost{
					Date:     *Date,
					Price_ID: Answer.Price_id,
					Count:    Answer.Count,
				}
				real_food_cost, err = Sklad(SMS, fc)
				if err != nil {
					return err
				}

			}
			if err = T.Transaction_QTTV_One(false, "Insert", "metrics_add_info", "Point", SELECT_ID, Default, Answer.Price_name, Answer.Type_id, Answer.Type_name, Answer.Count, Default, Answer.Price, fmt.Sprint(Answer.Price_id), -1, real_food_cost); err != nil {
				return err
			}
		}
	}
	return nil
}

func Sklad(SMS *SMS, fc structures.FoodCost) (interface{}, error) {
	fmt.Println("\nIP:", SMS.MS.IP, "\n\n")
	ip := "192.168.0.130:50040"
	conn_sklad, err := connect.CreateConnect(&ip)
	if err != nil {
		return nil, err
	}
	defer conn_sklad.Close()

	Q := structures.QueryMessage{Query: "Select", Table: "FoodCost", TypeParameter: "Price_ID"}

	//Q.Values = append(Q.Values, fc)
	log.Println("Запрос на склад:", Q)
	log.Println("Sklad fc:", fc)
	answer_message, err := connect.SelectMessageOLD(&conn_sklad, Q, fc)

	if err != nil {
		return nil, err
	}
	log.Println("Ответ от склада:", answer_message)

	if len(answer_message.Tables) != 0 {
		for _, val := range answer_message.Tables[0].Values {
			return val, nil
		}
	}
	return nil, errors.New("Нет ответа в структуре")
}

func (T *Transaction) Transaction_Insert_Points(SELECT_ID interface{}, SMS *SMS, MDD *Metrics_dop_data, Date *string, Answer *structures.Common_Answer, Add bool) error {

	if SELECT_ID == nil {
		log.Println("Создание SELECT_ID metrics")
		fmt.Println("Создание SELECT_ID metrics")
		if err := T.Transaction_QTTV_One(Add, "Insert", "metrics", "", MDD.MFH.Hash, MDD.MFH.Name, Date, Answer.Val1, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
			return err
		}
		SELECT_ID = T.HashData
	}

	log.Println("SELECT_ID:", SELECT_ID)
	fmt.Println("SELECT_ID:", SELECT_ID)

	if Add {
		if err := T.Transaction_QTTV_One(true, "Select", "metrics_add_info", "JSONMetric_idHash", SELECT_ID, Answer.Val2); err != nil && err.Error() != "sql: no rows in result set" {
			return err
		}
		if T.HashData == nil {
			//metric_id, hash, name, count, units, price
			if err := T.Transaction_QTTV_One(false, "Insert", "metrics_add_info", "", SELECT_ID, fmt.Sprint(Answer.Val2), Default, Answer.Val1, Default, -1); err != nil {
				return err
			}
			if err := T.Update_Metrics_AddValue(SELECT_ID, Answer.Val1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (T *Transaction) Transaction_Insert_Sklad(SELECT_ID interface{}, SMS *SMS, MDD *Metrics_dop_data, Date *string, Answer *structures.Common_Answer, Add bool) error {

	if SELECT_ID == nil {
		log.Println("Создание SELECT_ID metrics")
		fmt.Println("Создание SELECT_ID metrics")
		if err := T.Transaction_QTTV_One(Add, "Insert", "metrics", "", MDD.MFH.Hash, MDD.MFH.Name, Date, -1, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
			return err
		}
		SELECT_ID = T.HashData
	}

	log.Println("SELECT_ID:", SELECT_ID)
	fmt.Println("SELECT_ID:", SELECT_ID)

	if Add {
		if err := T.Transaction_QTTV_One(true, "Select", "metrics_add_info", "JSONMetric_idHash", SELECT_ID, Answer.Hash); err != nil && err.Error() != "sql: no rows in result set" {
			return err
		}
		if T.HashData != nil {
			var Metrics_add_info Metrics_add_info
			log.Println("Metrics_add_infoJSON", T.HashData)
			if err := json.Unmarshal([]byte((T.HashData.(string))), &Metrics_add_info); err != nil {
				return err
			}
			log.Println("Metrics_add_info:", Metrics_add_info)
			if Metrics_add_info.Count != Answer.Count || Metrics_add_info.Price != Answer.Price {
				if err := T.Transaction_QTTV_One(false, "Update", "metrics_add_info", "CountPriceId", Metrics_add_info.ID, Answer.Count, Answer.Price); err != nil {
					return err
				}
			}
		} else {
			if err := T.Transaction_QTTV_One(false, "Insert", "metrics_add_info", "", SELECT_ID, Answer.Hash, Answer.Name, Answer.Count, Answer.Uint, Answer.Price); err != nil {
				return err
			}
		}
	}
	return nil
}

func (T *Transaction) Transaction_Insert_Session(SELECT_ID interface{}, SMS *SMS, MDD *Metrics_dop_data, Answer *structures.Common_Answer, Date *string, Add bool) error {

	if SELECT_ID == nil {
		log.Println("Создание SELECT_ID metrics")
		fmt.Println("Создание SELECT_ID metrics")
		if err := T.Transaction_QTTV_One(Add, "Insert", "metrics", "", Answer.UserHash, Default, Date, Answer.JobTime, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
			return err
		}
		SELECT_ID = T.HashData
	} else {
		if err := T.Update_Metrics_Value(T.HashData, Answer.JobTime); err != nil {
			return err
		}
	}

	log.Println("SELECT_ID:", SELECT_ID)
	fmt.Println("SELECT_ID:", SELECT_ID)

	if Add {
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_user_work_info", "", SELECT_ID, MDD.MR.Hash, MDD.MR.Name, MDD.MFH.Hash, MDD.MFH.Name); err != nil {
			return err
		}
	}
	return nil
}

func (T *Transaction) Transaction_Insert_Courier(SELECT_ID interface{}, SMS *SMS, Date *string, Answer *structures.Common_Answer, Add bool) error {

	Len := len(Answer.ArrayInt64)

	if Len > 0 {

		if SELECT_ID == nil {
			log.Println("Создание SELECT_ID metrics")
			fmt.Println("Создание SELECT_ID metrics")
			if err := T.Transaction_QTTV_One(Add, "Insert", "metrics", "", Answer.Str, Default, Date, -1, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
				return err
			}
			SELECT_ID = T.HashData
		}
		log.Println("SELECT_ID:", SELECT_ID)
		fmt.Println("SELECT_ID:", SELECT_ID)

		if Add {
			var TA TransactionAction
			TA.Query, TA.Table, TA.Type = "Insert", "metrics_add_array", ""
			//			for i := 0; i < Len; i++ {
			//				fmt.Println(Answer.Hash, Answer.ArrayInt64[i])
			//				TA.Values = nil
			//				TA.Values = append(TA.Values, SELECT_ID, Answer.Hash, Answer.ArrayInt64[i])
			//				T.Data = append(T.Data, TA)
			//			}
			fmt.Println(Answer.Hash, Answer.ArrayInt64)
			TA.Values = nil
			TA.Values = append(TA.Values, SELECT_ID, Answer.Hash, Answer.ArrayInt64)
			T.Data = append(T.Data, TA)
			if err := T.Transaction(false); err != nil {
				return err
			}
		}
	}
	return nil
}

//OrdersInfo Cashbox metrics_cashbox
func (T *Transaction) Transaction_Insert_Cashbox(m *MetricsMetrics, values *GetDataForMetricsCashbox) error {
	log.Println("\n***Transaction_Insert_Cashbox***")

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_cashbox", "Order_Id", values.Order_id, values.Action_time); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_cashbox: %v", err)
	}
	if T.HashData == nil {
		log.Println("New: Insert.metrics_cashbox (Order_id): ", values.Order_id)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_cashbox", "", m.Id, values.Order_id, values.CashRegister, values.Action_time, values.UserHash, values.Info, values.Type_payments, values.Cash, values.Date_preorder); err != nil {
			return fmt.Errorf("Insert.metrics_cashbox: %v", err)
		}
	} else {
		log.Println("Already: Insert.metrics_cashbox (Order_id): ", values.Order_id)
	}

	log.Println("******\n")

	return nil
}

//OrdersInfo metrics_orders_info
func (T *Transaction) Transaction_Insert_OrdersInfo(m *MetricsMetrics, values *GetDataForMetricsOrders) error {
	log.Println("\n***Transaction_Insert_OrdersInfo***")

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_orders_info", "Order_id", values.Order_id); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_orders_info: %v", err)
	}
	if T.HashData == nil {
		log.Println("New: Insert.metrics_orders_info (Order_id): ", values.Order_id)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_orders_info", "", m.Id, values.Order_id, values.Chain_hash, values.Org_hash, values.Point_hash, values.Id_day_point, values.Cashregister_id, values.Count_elements, values.Date_preorder_cook, values.Side_order, values.Type_delivery, values.Type_payments, values.Price, values.Bonus, values.Discount_id, values.Discount_name, values.Discount_percent, values.City, values.Street, values.House, values.Building, values.Creator_hash, values.Creator_role_hash, values.Creator_time, values.Duration_of_create, values.Duration_of_select_element, values.Cook_start_time, values.Cook_end_time, values.Collector_hash, values.Collector_time, values.Courier_hash, values.Courier_start_time, values.Courier_end_time, values.Cancel_hash, values.Cancel_time, values.Cancellation_reason_id, values.Cancellation_reason_note, values.Crash_user_hash, values.Crash_user_role_hash, values.Compensation, values.Type_compensation, values.Type); err != nil {
			return fmt.Errorf("Insert.metrics_orders_info: %v", err)
		}
	} else {
		log.Println("Already: Insert.metrics_orders_info (Order_id): ", values.Order_id)
	}

	log.Println("******\n")

	return nil
}

//OrdersListInfo metrics_orders_list_info
func (T *Transaction) Transaction_Insert_OrdersListInfo(m *MetricsMetrics, values *GetDataForMetricsOrdersLists) error {
	log.Println("\n***Transaction_Insert_OrdersListInfo***")

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_orders_list_info", "IdItem_OrderId", values.Id_item, values.Order_id); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_orders_list_info: %v", err)
	}
	if T.HashData == nil {

		//велосипед для получения food_cost со склада
		if err := Real_food_cost(values); err != nil {
			return fmt.Errorf("func Real_food_cost: %v", err)
		}
		////

		log.Println("New: Insert.metrics_orders_list_info (Order_id, Id_item): ", values.Order_id, values.Id_item)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_orders_list_info", "", m.Id, values.Order_id, values.Id_item, values.Id_parent_item, values.Price_id, values.Price_name, values.Type_id, values.Cooking_tracker, values.Discount_id, values.Discount_name, values.Discount_percent, values.Price, values.Cook_hash, values.Start_time, values.End_time, values.Fail_id, values.Fail_user_hash, values.Fail_comments, values.Real_foodcost, values.Count, values.Type_name, values.Over_status_id); err != nil {
			return fmt.Errorf("Insert.metrics_orders_list_info: %v", err)
		}
	} else {
		log.Println("Already: Insert.metrics_orders_list_info (Order_id, Id_item): ", values.Order_id, values.Id_item)
	}

	log.Println("******\n")

	return nil
}

//?
func (T *Transaction) Transaction_Insert_Metrics_Cook(SELECT_ID interface{}, SMS *SMS, UserName *string, Date *string, Answer *structures.Common_Answer, Add bool) error {

	var Status int64 = 0
	switch SMS.MST.TypeParameter {
	case "RangeCountDishCookingForCook":
		Add, Status = true, 8
	case "RangeCountWhoSentRemakeDish":
		Add, Status = true, 14
	case "RangeSumRemakingDish": //Это сумма, сумма уже не учавствует
		Add = false
	case "RangeCountSumExecuted": //Это сумма, сумма уже не учавствует
		Add = false
	default:
		fmt.Println("Неиспользуемый Тип")
		log.Println("Неиспользуемый Тип")
		return nil
	}

	if SELECT_ID == nil {
		log.Println("Создание SELECT_ID metrics")
		fmt.Println("Создание SELECT_ID metrics")
		if err := T.Transaction_QTTV_One(Add, "Insert", "metrics", "", Answer.Str, UserName, Date, Answer.Val1, SMS.MP.Min_Step_ID, SMS.MP.ID); err != nil {
			return err
		}
		SELECT_ID = T.HashData
	}

	log.Println("SELECT_ID:", SELECT_ID)
	fmt.Println("SELECT_ID:", SELECT_ID)

	if Add {
		//T.DataOne.Query, T.DataOne.Table, T.DataOne.Type = "Insert", "metrics_add_info", "Cook"
		//log.Println("Transaction_Insert_Add_Info_Points (T.DataOne.Values):", T.DataOne.Values)
		var I float64
		for I = 0; I < Answer.Val1; I++ {
			if err := T.Transaction_QTTV_One(false, "Insert", "metrics_add_info", "Cook", SELECT_ID, Answer.Val2, Status); err != nil {
				return err
			}
			//log.Println("Создаем: metrics_add_info (", I, ")", T.DataOne.Values)
			//			T.DataOne.Values = append(T.DataOne.Values, SELECT_ID, Answer.Val2, Status)
			//			if err := T.Transaction_One(false); err != nil {
			//				return err
			//			}

		}
		log.Println("Закончили (TINC)")
	}
	return nil
}

//?
func (T *Transaction) Transaction_Insert_Cook(SMS *SMS, UserName *string, Date *string, Answer *structures.Common_Answer, Add bool) error {

	//	if err := T.Transaction_QTTV_One(true, "SelectID", "metrics", "DateStep_idParameter_idMS("+fmt.Sprint(SMS.MP.Min_Step_ID)+")", Date, SMS.MP.ID, Answer.Str); err != nil && err.Error() != "sql: no rows in result set" {
	//		return err
	//	}

	//	log.Println("SELECT_ID:", T.HashData)

	return T.Transaction_QTTV_One(Add, "Insert", "metrics_cook_count", "", Answer.Str, SMS.MP.ID, SMS.MP.Min_Step_ID, Date)
}

//Для обновления Value в metrics(Добавление)
func (T *Transaction) Update_Metrics_AddValue(SELECT_ID interface{}, Value float64) error {

	//	T.DataOne.Query, T.DataOne.Table, T.DataOne.Type, T.DataOne.Values, T.HashData = "Update", "metrics", "AddValueById", nil, nil
	//	T.DataOne.Values = append(T.DataOne.Values, SELECT_ID, Value)
	//	return T.Transaction_One(false)

	return T.Transaction_QTTV_One(false, "Update", "metrics", "AddValueById", SELECT_ID, Value)

}

//Для обновления Value в metrics(Перезапись)
func (T *Transaction) Update_Metrics_Value(SELECT_ID interface{}, Value float64) error {

	//	T.DataOne.Query, T.DataOne.Table, T.DataOne.Type, T.DataOne.Values, T.HashData = "Update", "metrics", "ValueById", nil, nil
	//	T.DataOne.Values = append(T.DataOne.Values, SELECT_ID, Value)
	//	return T.Transaction_One(false)

	return T.Transaction_QTTV_One(false, "Update", "metrics", "ValueById", SELECT_ID, Value)

}

/* --/--/--/-- */

/* Конец для транзакции */

func (M *Metrics_dop_data) Action(Query, Table, Type string, Value ...interface{}) error {
	return Requests.ExecTransact(Query+"."+Table+"."+Type, Value...)
}

func (M *Metrics_dop_data) Select(Query, Table, Type string, Value ...interface{}) error {
	Row, err := Requests.QueryRow(Query+"."+Table+"."+Type, Value...)
	if err != nil {
		return err
	}
	switch Type {
	case "Franchise_hierarchy":
		return Row.Scan(&M.MFH.ID, &M.MFH.Hash, &M.MFH.Parent_hash, &M.MFH.Name)
	case "Role":
		return Row.Scan(&M.MR.ID, &M.MR.Hash, &M.MR.Name)
	default:
		return errors.New("Тип выборки не поддерживается")
	}
}

func (M *Metrics_dop_data) Selects(Query, Table, Type string, Value ...interface{}) (*sql.Rows, error) {
	Rows, err := Requests.Query(Query+"."+Table+"."+Type, Value...)
	return Rows, err
}

//выкачиваем организации и роли для хранения истории изменений
func (M *Metrics_dop_data) Start_Dop_Data() error {
	//Если доп дата не выгрузилась, то нужно будет доделать выборку самых последних сохранённых данных

	//fmt.Println("1.Start_Dop_Data")

	M.q = structures.QueryMessage{Table: "Hierarchy"}
	if M.conn, M.err = connect.CreateConnect(&config.Config.Organization_service); M.err != nil {
		return M.err
	}
	defer M.conn.Close()

	if M.answerRows, M.err = connect.SelectRows(&M.conn, M.q); M.err != nil {
		return M.err
	}

	//fmt.Println("2.Start_Dop_Data")

	M.err = M.JobWithArray(1)
	if M.err != nil {
		return M.err
	}

	M.conn.Close()

	//////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////

	M.q = structures.QueryMessage{Table: "UserRole", Query: "Select", TypeParameter: "All", Limit: 9999}
	if M.conn, M.err = connect.CreateConnect(&config.Config.Role_service); M.err != nil {
		return M.err
	}
	if M.answerRows, M.err = connect.SelectRows(&M.conn, M.q); M.err != nil {
		return M.err
	}

	//fmt.Println("3.Start_Dop_Data")

	M.err = M.JobWithArray(2)
	if M.err != nil {
		return M.err
	}

	return nil
}

func (M *Metrics_dop_data) JobWithArray(data_id int64) error {
	//fmt.Println("JobWithArray")

	var IN_MFH, DB_MFH []Metrics_franchise_hierarchy

	var i, d int64
	var typeparameter string

	//fmt.Println("1.JobWithArray")
	for _, Ans := range M.answerRows {
		M.MFH.ID, M.MFH.Hash, M.MFH.Name, M.MFH.Parent_hash = "", "", "", ""
		if M.err = json.Unmarshal([]byte(Ans), &M.MFH); M.err != nil {
			return M.err
		}
		//fmt.Println(M.MFH.Hash, M.MFH.Parent_hash, M.MFH.Name)
		i = i + 1
		IN_MFH = append(IN_MFH, M.MFH)
	}

	//fmt.Println("2.JobWithArray")

	switch data_id {
	case 1:
		typeparameter = "All_Franchise_hierarchy"
	case 2:
		typeparameter = "All_Role"
	}

	Rows, err := M.Selects("Select", "metrics_dop_data", typeparameter)
	if err != nil {
		//fmt.Println(err)
		return err
	}

	for Rows.Next() {
		switch typeparameter {
		case "All_Franchise_hierarchy":
			err = Rows.Scan(&M.MFH.ID, &M.MFH.Hash, &M.MFH.Parent_hash, &M.MFH.Name)
		case "All_Role":
			err = Rows.Scan(&M.MFH.ID, &M.MFH.Hash, &M.MFH.Name)
		}
		//fmt.Println(err)
		d = d + 1
		DB_MFH = append(DB_MFH, M.MFH)
	}

	if len(DB_MFH) == 0 {
		return errors.New("Не прочитало с базы")
	}

	//fmt.Println("3.JobWithArray")

	flag, err := СompareMFHArray(data_id, IN_MFH, DB_MFH)
	if err != nil {
		return err
	}

	//fmt.Println("4.JobWithArray")

	if i != d || flag == false {
		//fmt.Println("5.JobWithArray")

		t := time.Now()
		for i := 0; i < len(IN_MFH); i++ {
			switch data_id {
			case 1:
				if M.err = M.common.Action("Insert", "metrics_dop_data", "", IN_MFH[i].Hash, IN_MFH[i].Parent_hash, IN_MFH[i].Name, DefaultFloat64, DefaultFloat64, DefaultFloat64, fn.FormatDate(t), data_id); M.err != nil {
					return M.err
				}
			case 2:
				if M.err = M.common.Action("Insert", "metrics_dop_data", "", IN_MFH[i].Hash, IN_MFH[i].Name, &Default, DefaultFloat64, DefaultFloat64, DefaultFloat64, fn.FormatDate(t), data_id); M.err != nil {
					return M.err
				}
			}

		}
		//fmt.Println("6.JobWithArray")
	}
	return nil
}

func СompareMFHArray(data_id int64, IN, DB []Metrics_franchise_hierarchy) (bool, error) {

	//fmt.Println("_____________________________________________СompareMFHArray")

	var err error
	var flag bool

	var flags []bool

	for i := 0; i < len(IN); i++ {
		flags = append(flags, false)

		for d := 0; d < len(DB); d++ {
			flag, err = IN[i].СompareMFH(data_id, DB[d])
			if flag {
				flags[i] = flag
			}
			if err != nil {
				return flag, err
			}
		}
	}

	//fmt.Println("flags------------------", flags)

	for i := 0; i < len(IN); i++ {
		if flags[i] == false {
			return false, nil
		}
	}
	return true, err
}

func (IN *Metrics_franchise_hierarchy) СompareMFH(data_id int64, DB Metrics_franchise_hierarchy) (bool, error) {

	//fmt.Println("СompareMFH")

	switch data_id {
	case 1:
		//		fmt.Println(IN.Hash == DB.Hash && IN.Name == DB.Name && IN.Parent_hash == DB.Parent_hash)
		//		fmt.Println(IN.Hash, IN.Name, IN.Parent_hash)
		//		fmt.Println(DB.Hash, DB.Name, DB.Parent_hash)
		return (IN.Hash == DB.Hash && IN.Name == DB.Name && IN.Parent_hash == DB.Parent_hash), nil
	case 2:
		//		fmt.Println(IN.Hash == DB.Hash && IN.Name == DB.Name)
		//		fmt.Println(IN.Hash, IN.Name)
		//		fmt.Println(DB.Hash, DB.Name)
		return (IN.Hash == DB.Hash && IN.Name == DB.Name), nil
	}
	return false, errors.New("Отcутствует такая data_id")
}

type Common struct {
	JS_Select structures.JS_Select

	Data []interface{}
}

func (C *Common) Check(Table, Type string, Value ...interface{}) (bool, error) {
	log.Println("Check." + Table + "." + Type)
	Row, err := Requests.QueryRow("Check."+Table+"."+Type, Value...)
	if err != nil {
		return false, err
	}
	var BOOL bool
	if err = Row.Scan(&BOOL); err != nil {
		return false, err
	}
	return BOOL, err
}

func (M *Common) Action(Query, Table, Type string, Value ...interface{}) error {
	return Requests.ExecTransact(Query+"."+Table+"."+Type, Value...)
}

func (M *Common) Select_Row(Query, Table, Type string, Value ...interface{}) error {
	row, err := Requests.QueryRow(Query+"."+Table+"."+Type, Value...)
	if err != nil {
		return err
	}
	return row.Scan(&M.Data)
}

func (C *Common) Select_Common(Query, Table, Type string, Value ...interface{}) error {
	C.Data = nil
	Rows, err := Requests.Query(Query+"."+Table+"."+Type, Value...)
	if err != nil {
		return err
	}
	defer Rows.Close()
	for Rows.Next() {
		switch Table {
		case "metrics":
			{
				switch Type {
				case "ReportSale":
					{ //mai.id, mai.metric_id, mai.hash, mai.name, mai.units, mai.price, mai.price_id, mai.status_id, mai.count
						var M Metrics_add_info
						if err = Rows.Scan(&M.Metric_id, &M.Hash, &M.Name, &M.Type_id, &M.Type_name, &M.Units, &M.Price, &M.Price_id, &M.Status_id, &M.Count, &M.Real_food_cost); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportSaleByInterval":
					{
						var M Metrics_add_info //mai.hash, mai.name, mai.units, sum(mai.price), mai.price_id, mai.status_id, sum(mai.Count)
						if err = Rows.Scan(&M.Hash, &M.Name, &M.Type_id, &M.Type_name, &M.Units, &M.Price, &M.Price_id, &M.Status_id, &M.Count, &M.Real_food_cost); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportSaleNewByInterval":
					{
						var M ReportSale
						if err = Rows.Scan(&M.Name, &M.Type_id, &M.Type_name, &M.Price, &M.Price_id, &M.Count, &M.Real_food_cost); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportSummaOnTypePaymentsFromCashBox":
					{
						var M Result_summ
						if err = Rows.Scan(&M.Val); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ParametersByInterval":
					{
						var M Metrics
						if err = Rows.Scan(&M.ID, &M.OwnHash, &M.OwnName, &M.Date, &M.Value, &M.Step_ID, &M.Parameter_ID); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "Report":
					{
						var M Metrics_add_info
						if err = Rows.Scan(&M.ID, &M.Metric_id, &M.Hash, &M.Name, &M.Units, &M.Price, &M.Price_id, &M.Status_id, &M.Count); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportByInterval":
					{
						var M Metrics_add_info
						if err = Rows.Scan(&M.Hash, &M.Name, &M.Units, &M.Price, &M.Price_id, &M.Status_id, &M.Count); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportCourier":
					{
						var M Metrics_courier_info
						if err = Rows.Scan(&M.Hash, &M.Count, &M.ArrayOrdersID); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportCourierByInterval":
					{
						var M Metrics_courier_info
						if err = Rows.Scan(&M.Hash, &M.Count, &M.ArrayOrdersID); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportOperator":
					{
						var M Metrics_courier_info
						if err = Rows.Scan(&M.Hash, &M.Count); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportOperatorByInterval":
					{
						var M Metrics_courier_info
						if err = Rows.Scan(&M.Hash, &M.Count); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				case "ReportCashbox":
					{
						var M Metrics_cashbox
						if err = Rows.Scan(&M.CashRegister, &M.Action_time, &M.UserHash, &M.Info, &M.Type_payments, &M.Cash, &M.Date_preorder); err != nil {
							return err
						}
						M.Action_timeStr = fn.FormatDate(M.Action_time)
						C.Data = append(C.Data, M)
					}
				case "ReportCashboxByInterval":
					{
						var M Metrics_cashbox
						if err = Rows.Scan(&M.CashRegister, &M.Action_time, &M.UserHash, &M.Info, &M.Type_payments, &M.Cash, &M.Date_preorder); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				default:
					{
						var M Metrics
						if err = Rows.Scan(&M.ID, &M.OwnHash, &M.OwnName, &M.Date, &M.Value, &M.Step_ID, &M.Parameter_ID); err != nil {
							return err
						}
						C.Data = append(C.Data, M)
					}
				}
			}
		case "metrics_link_type_and_mod":
			{
				var M Metrics_link_type_and_mod
				if err = Rows.Scan(&M.ID, &M.Mod_ID, &M.Type_ID, &M.Info); err != nil {
					return err
				}
				C.Data = append(C.Data, M)
			}
		case "metrics_link_type_and_mod_or_names":
			{
				var M Metrics_link_type_and_mod
				if err = Rows.Scan(&M.ID, &M.Type_ID, &M.Type_Name, &M.Mod_ID, &M.Mod_Name, &M.Info); err != nil {
					return err
				}
				C.Data = append(C.Data, M)
			}
		case "metrics_mod", "metrics_type":
			{
				var M Metrics_type_mod
				if err = Rows.Scan(&M.ID, &M.Parent_ID, &M.Name); err != nil {
					return err
				}
				C.Data = append(C.Data, M)
			}
		case "metrics_parameters":
			{
				var M Metrics_parameters
				if err = Rows.Scan(&M.ID, &M.Interface_ID, &M.Type_Mod_ID, &M.Own_ID, &M.Min_Step_ID); err != nil {
					return err
				}
				C.Data = append(C.Data, M)
			}
		case "metrics_service":
			{
				var M Metrics_service
				if err = Rows.Scan(&M.ID, &M.Name, &M.IP); err != nil {
					return err
				}
				C.Data = append(C.Data, M)
			}
		case "metrics_service_table":
			{
				var M Metrics_service_table
				if err = Rows.Scan(&M.ID, &M.Query, &M.TableName, &M.TypeParameter, &M.Service_ID); err != nil {
					return err
				}
				C.Data = append(C.Data, M)
			}
		case "metrics_service_data":
			{
				var M Metrics_service_data
				if err = Rows.Scan(&M.ID, &M.Service_table, &M.End_date, &M.End_ID); err != nil {
					return err
				}
				C.Data = append(C.Data, M)
			}
		case "metrics_dop_data":
			{
				switch Type {
				case "LoadHierarchy":
					var M Metrics_franchise_hierarchy
					if err = Rows.Scan(&M.ID, &M.Hash, &M.Parent_hash, &M.Name); err != nil {
						return err
					}
					C.Data = append(C.Data, M)
				default:
					return errors.New("Неизвестный тип для таблицы (" + Table + "), тип: (" + Type + ")")
				}
			}
		default:
			return errors.New("Неизвестная таблица в запросе: (" + Table + ")")
		}
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type MetricValues interface {
	Hash() string
	Date() time.Time
	Insert(Transaction *Transaction, m *MetricsMetrics) error
}

// *** metrics_cashbox ***
func (mc GetDataForMetricsCashbox) Hash() string    { return mc.PointHash }
func (mc GetDataForMetricsCashbox) Date() time.Time { return mc.Action_time }
func (mc GetDataForMetricsCashbox) Insert(Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.Transaction_Insert_Cashbox(m, &mc); err != nil {
		return fmt.Errorf("Transaction_Insert_Cashbox: %v", err)
	}
	return nil
}

// *** metrics_orders_info ***
func (mo GetDataForMetricsOrders) Hash() string    { return mo.Point_hash }
func (mo GetDataForMetricsOrders) Date() time.Time { return mo.Creator_time }
func (mo GetDataForMetricsOrders) Insert(Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.Transaction_Insert_OrdersInfo(m, &mo); err != nil {
		return fmt.Errorf("Transaction_Insert_OrdersInfo: %v", err)
	}
	return nil
}

// *** metrics_orders_list_info ***
func (mol GetDataForMetricsOrdersLists) Hash() string    { return mol.Point_hash }
func (mol GetDataForMetricsOrdersLists) Date() time.Time { return mol.Order_time }
func (mol GetDataForMetricsOrdersLists) Insert(Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.Transaction_Insert_OrdersListInfo(m, &mol); err != nil {
		return fmt.Errorf("Transaction_Insert_OrdersListInfo: %v", err)
	}
	return nil
}

//велосипед для получения real_food_cost со Склада
func Real_food_cost(answer *GetDataForMetricsOrdersLists) error {
	defer Recover()

	ip := "192.168.0.130:50040"
	conn, err := connect.CreateConnect(&ip)
	if err != nil {
		return fmt.Errorf("CreateConnect: %v", err)
	}
	defer conn.Close()

	// Запрос данных у стороннего сервиса (Sklad)
	Q := structures.QueryMessage{Query: "Select", Table: "FoodCost", TypeParameter: "Price_ID"}

	log.Println("Запрос на склад:", Q)
	//fmt.Println("Запрос на склад:", Q)

	fc := structures.FoodCost{
		Date:     fn.FormatDate(answer.Start_time),
		Price_ID: float64(answer.Price_id),
		Count:    answer.Count,
	}

	log.Println("fc:", fc)

	Answer_Message, err := connect.SelectMessageOLD(&conn, Q, fc)
	if err != nil {
		return fmt.Errorf("Answer_Message: %v", err)
	}

	log.Println("Answer_Message:", Answer_Message)

	if len(Answer_Message.Tables) != 0 { //Если данных не прилетело, то не надо в метрику данные записывать
		for _, VAL := range Answer_Message.Tables[0].Values { //Начинаем бегать по результату ответа
			if VAL == nil {
				return errors.New("критический сбой: Answer_Message cтруктура ответа = nil")
			}

			answer.Real_foodcost = VAL.(float64)

			log.Println("answer:", answer)
			//fmt.Println("\nanswer:", answer)

			return nil
		}
	}

	return errors.New("Нет ответа в структуре")
}
