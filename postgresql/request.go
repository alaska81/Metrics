package postgresql

import (
	"MetricsNew/connect"
	fn "MetricsNew/function"
	"MetricsNew/redis"
	"MetricsNew/structures"

	"errors"
	"fmt"
	"log"
	"time"
)

/////////////////////////
// Transaction_Insert_ //
/////////////////////////

//OrdersInfo Cashbox metrics_cashbox
func (T *Transaction) Transaction_Insert_Cashbox(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsCashbox) error {
	//log.Println("\n***Transaction_Insert_Cashbox***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.Order_id, values.Action_time}); err != nil {
		return fmt.Errorf("Transaction_Insert_Cashbox - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, []interface{}{values.Order_id, values.Action_time}) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_cashbox", "Order_Id", values.Order_id, values.Action_time); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_cashbox: %v", err)
	}
	if T.HashData == nil {
		//log.Println("New: Insert.metrics_cashbox (Order_id): ", values.Order_id)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_cashbox", "", m.Id, values.Order_id, values.CashRegister, values.Action_time, values.UserHash, values.UserName, values.Info, values.Type_payments, values.Cash, values.Date_preorder); err != nil {
			return fmt.Errorf("Insert.metrics_cashbox: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_cashbox (Order_id): ", values.Order_id)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_cashbox", "", values.Order_id, values.Action_time, m.Id, values.CashRegister, values.UserHash, values.UserName, values.Info, values.Type_payments, values.Cash, values.Date_preorder); err != nil {
				return fmt.Errorf("Update.metrics_cashbox: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//OrdersInfo metrics_orders_info
func (T *Transaction) Transaction_Insert_OrdersInfo(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsOrders) error {
	//log.Println("\n***Transaction_Insert_OrdersInfo***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Order_id); err != nil {
		return fmt.Errorf("Transaction_Insert_OrdersInfo - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.Order_id) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_orders_info", "Order_id", values.Order_id); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_orders_info: %v", err)
	}
	if T.HashData == nil {
		//log.Println("New: Insert.metrics_orders_info (Order_id): ", values.Order_id)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_orders_info", "", m.Id, values.Order_id, values.Chain_hash, values.Org_hash, values.Point_hash, values.Id_day_point, values.Cashregister_id, values.Count_elements, values.Date_preorder_cook, values.Side_order, values.Type_delivery, values.Type_payments, values.Price, values.Bonus, values.Discount_id, values.Discount_name, values.Discount_percent, values.City, values.Street, values.House, values.Building, values.Creator_hash, values.Creator_role_hash, values.Creator_time, values.Duration_of_create, values.Duration_of_select_element, values.Cook_start_time, values.Cook_end_time, values.Collector_hash, values.Collector_time, values.Courier_hash, values.Courier_start_time, values.Courier_end_time, values.Cancel_hash, values.Cancel_time, values.Cancellation_reason_id, values.Cancellation_reason_note, values.Crash_user_hash, values.Crash_user_role_hash, values.Compensation, values.Type_compensation, values.Type, values.Customer_phone); err != nil {
			return fmt.Errorf("Insert.metrics_orders_info: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_orders_info (Order_id): ", values.Order_id)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_orders_info", "", values.Order_id, m.Id, values.Chain_hash, values.Org_hash, values.Point_hash, values.Id_day_point, values.Cashregister_id, values.Count_elements, values.Date_preorder_cook, values.Side_order, values.Type_delivery, values.Type_payments, values.Price, values.Bonus, values.Discount_id, values.Discount_name, values.Discount_percent, values.City, values.Street, values.House, values.Building, values.Creator_hash, values.Creator_role_hash, values.Creator_time, values.Duration_of_create, values.Duration_of_select_element, values.Cook_start_time, values.Cook_end_time, values.Collector_hash, values.Collector_time, values.Courier_hash, values.Courier_start_time, values.Courier_end_time, values.Cancel_hash, values.Cancel_time, values.Cancellation_reason_id, values.Cancellation_reason_note, values.Crash_user_hash, values.Crash_user_role_hash, values.Compensation, values.Type_compensation, values.Type, values.Customer_phone); err != nil {
				return fmt.Errorf("Update.metrics_orders_info: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//OrdersListInfo metrics_orders_list_info
func (T *Transaction) Transaction_Insert_OrdersListInfo(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsOrdersLists) error {
	//log.Println("\n***Transaction_Insert_OrdersListInfo***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.Id_item, values.Order_id}); err != nil {
		return fmt.Errorf("Transaction_Insert_OrdersListInfo - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, []interface{}{values.Id_item, values.Order_id}) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_orders_list_info", "IdItem_OrderId", values.Id_item, values.Order_id); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_orders_list_info: %v", err)
	}
	if T.HashData == nil {

		//велосипед для получения food_cost со склада
		if err := Real_food_cost(values); err != nil {
			return fmt.Errorf("func Real_food_cost: %v", err)
		}
		////

		//log.Println("New: Insert.metrics_orders_list_info (Order_id, Id_item): ", values.Order_id, values.Id_item)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_orders_list_info", "", m.Id, values.Order_id, values.Id_item, values.Id_parent_item, values.Price_id, values.Price_name, values.Type_id, values.Cooking_tracker, values.Discount_id, values.Discount_name, values.Discount_percent, values.Price, values.Cook_hash, values.Start_time, values.End_time, values.Fail_id, values.Fail_user_hash, values.Fail_comments, values.Real_foodcost, values.Count, values.Type_name, values.Over_status_id, values.Time_cook, values.Time_fry, values.Set, values.Cook_role); err != nil {
			return fmt.Errorf("Insert.metrics_orders_list_info: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_orders_list_info (Order_id, Id_item): ", values.Order_id, values.Id_item)
		if SMS.MP.Update_allow == true {

			//велосипед для получения food_cost со склада
			if err := Real_food_cost(values); err != nil {
				return fmt.Errorf("func Real_food_cost: %v", err)
			}
			////

			if err := T.Transaction_QTTV_One(false, "Update", "metrics_orders_list_info", "", values.Id_item, values.Order_id, m.Id, values.Id_parent_item, values.Price_id, values.Price_name, values.Type_id, values.Cooking_tracker, values.Discount_id, values.Discount_name, values.Discount_percent, values.Price, values.Cook_hash, values.Start_time, values.End_time, values.Fail_id, values.Fail_user_hash, values.Fail_comments, values.Real_foodcost, values.Count, values.Type_name, values.Over_status_id, values.Time_cook, values.Time_fry, values.Set, values.Cook_role); err != nil {
				return fmt.Errorf("Update.metrics_orders_list_info: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//Role metrics_hash_name
func (T *Transaction) Transaction_Insert_Role(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsRole) error {
	//log.Println("\n***Transaction_Insert_Role***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Hash); err != nil {
		return fmt.Errorf("Transaction_Insert_Role - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.Hash) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_hash_name", "Hash", values.Hash); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_hash_name: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_hash_name (Hash): ", values.Hash)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_hash_name", "", m.Id, values.Hash, values.Name, values.CreateTime); err != nil {
			return fmt.Errorf("Insert.metrics_hash_name: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_hash_name (Hash): ", values.Hash)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_hash_name", "", values.Hash, m.Id, values.Name, values.CreateTime); err != nil {
				return fmt.Errorf("Update.metrics_hash_name: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//User metrics_hash_name
func (T *Transaction) Transaction_Insert_User(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsUser) error {
	//log.Println("\n***Transaction_Insert_User***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Hash); err != nil {
		return fmt.Errorf("Transaction_Insert_OrdersListInfo - redis.AddValueInTmp: %v", err)
	}

	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.Hash) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_hash_name", "Hash", values.Hash); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_hash_name: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_hash_name (Hash): ", values.Hash)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_hash_name", "", m.Id, values.Hash, values.Name, time.Time{}); err != nil {
			return fmt.Errorf("Insert.metrics_hash_name: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_hash_name (Hash): ", values.Hash)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_hash_name", "", values.Hash, m.Id, values.Name, time.Time{}); err != nil {
				return fmt.Errorf("Update.metrics_hash_name: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//Plan metrics_plan
func (T *Transaction) Transaction_Insert_Plan(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsPlan) error {
	//log.Println("\n***Transaction_Insert_Plan***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.PlanDate, values.PointHash, values.RoleHash}); err != nil {
		return fmt.Errorf("Transaction_Insert_Plan - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, []interface{}{values.PlanDate, values.PointHash, values.RoleHash}) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_plan", "Data_Point_Role", values.PlanDate, values.PointHash, values.RoleHash); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_plan: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_hash_name (Hash): ", values.Hash)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_plan", "", m.Id, values.PlanDate, values.PointHash, values.RoleHash, values.Counts); err != nil {
			return fmt.Errorf("Insert.metrics_plan: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_plan (Hash): ", values.Hash)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_plan", "", values.PlanDate, values.PointHash, values.RoleHash, m.Id, values.Counts); err != nil {
				return fmt.Errorf("Update.metrics_plan: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//Plan metrics_plan
func (T *Transaction) Transaction_Insert_Point(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsPoint) error {
	//log.Println("\n***Transaction_Insert_Point***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Hash); err != nil {
		return fmt.Errorf("Transaction_Insert_Point - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.Hash) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_hash_name", "Hash", values.Hash); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_hash_name: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_hash_name (Hash): ", values.Hash)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_hash_name", "", m.Id, values.Hash, values.City+","+values.Street+","+values.House, values.CreateTime); err != nil {
			return fmt.Errorf("Insert.metrics_hash_name: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_point (Hash): ", values.Hash)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_hash_name", "", values.Hash, m.Id, values.City+","+values.Street+","+values.House, values.CreateTime); err != nil {
				return fmt.Errorf("Update.metrics_hash_name: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

/////////////////////////
/////////////////////////
/////////////////////////

//велосипед для получения real_food_cost со Склада
func Real_food_cost(answer *GetDataForMetricsOrdersLists) error {
	defer Recover()

	ip := "91.240.87.193:50040"
	conn, err := connect.CreateConnect(&ip)
	if err != nil {
		return fmt.Errorf("CreateConnect: %v", err)
	}
	defer conn.Close()

	// Запрос данных у стороннего сервиса (Sklad)
	Q := structures.QueryMessage{Query: "Select", Table: "FoodCost", TypeParameter: "Price_ID"}

	log.Println("Запрос на склад:", Q)
	//fmt.Println("Запрос на склад:", Q)

	fc := FoodCost{
		Date:     fn.FormatDate(answer.Start_time),
		Price_ID: float64(answer.Price_id),
		Count:    answer.Count,
	}

	log.Println("fc:", fc)

	Answer_Message, err := connect.SelectMessageOLD(&conn, Q, fc)
	if err != nil {
		return fmt.Errorf("Answer_Message: %v", err)
	}

	log.Println("Ответ со склада:", Answer_Message)

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
