package postgresql

import (
	"MetricsNew/connect"
	fn "MetricsNew/function"
	"MetricsNew/redis"
	"MetricsNew/structures"

	"errors"
	"fmt"
	"time"
)

/////////////////////////
// Transaction_Insert_ //
/////////////////////////

//TransactionInsertCashbox  metrics_cashbox
func (T *Transaction) TransactionInsertCashbox(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsCashbox) error {
	//log.Println("\n***Transaction_Insert_Cashbox***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.Order_id, values.Action_time}); err != nil {
		return fmt.Errorf("TransactionInsertCashbox - redis.AddValueInTmp: %v", err)
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

//TransactionInsertOrdersInfo metrics_orders_info
func (T *Transaction) TransactionInsertOrdersInfo(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsOrders) error {
	//log.Println("\n***Transaction_Insert_OrdersInfo***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Order_id); err != nil {
		return fmt.Errorf("TransactionInsertOrdersInfo - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.Order_id) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_orders_info", "Order_id", values.Order_id); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_orders_info: %v", err)
	}
	if T.HashData == nil {
		//log.Println("New: Insert.metrics_orders_info (Order_id): ", values.Order_id)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_orders_info", "", m.Id, values.Order_id, values.Chain_hash, values.Org_hash, values.Point_hash, values.Id_day_point, values.Cashregister_id, values.Count_elements, values.Date_preorder_cook, values.Side_order, values.Type_delivery, values.Type_payments, values.Price, values.Bonus, values.Discount_id, values.Discount_name, values.Discount_percent, values.City, values.Street, values.House, values.Building, values.Creator_hash, values.Creator_role_hash, values.Creator_time, values.Duration_of_create, values.Duration_of_select_element, values.Cook_start_time, values.Cook_end_time, values.Collector_hash, values.Collector_time, values.Courier_hash, values.Courier_start_time, values.Courier_end_time, values.Cancel_hash, values.Cancel_time, values.Cancellation_reason_id, values.Cancellation_reason_note, values.Crash_user_hash, values.Crash_user_role_hash, values.Compensation, values.Type_compensation, values.Type, values.Customer_phone, values.PriceWithDiscount, values.Division); err != nil {
			return fmt.Errorf("Insert.metrics_orders_info: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_orders_info (Order_id): ", values.Order_id)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_orders_info", "", values.Order_id, m.Id, values.Chain_hash, values.Org_hash, values.Point_hash, values.Id_day_point, values.Cashregister_id, values.Count_elements, values.Date_preorder_cook, values.Side_order, values.Type_delivery, values.Type_payments, values.Price, values.Bonus, values.Discount_id, values.Discount_name, values.Discount_percent, values.City, values.Street, values.House, values.Building, values.Creator_hash, values.Creator_role_hash, values.Creator_time, values.Duration_of_create, values.Duration_of_select_element, values.Cook_start_time, values.Cook_end_time, values.Collector_hash, values.Collector_time, values.Courier_hash, values.Courier_start_time, values.Courier_end_time, values.Cancel_hash, values.Cancel_time, values.Cancellation_reason_id, values.Cancellation_reason_note, values.Crash_user_hash, values.Crash_user_role_hash, values.Compensation, values.Type_compensation, values.Type, values.Customer_phone, values.PriceWithDiscount, values.Division); err != nil {
				return fmt.Errorf("Update.metrics_orders_info: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//TransactionInsertOrdersListInfo metrics_orders_list_info
func (T *Transaction) TransactionInsertOrdersListInfo(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsOrdersLists) error {
	//log.Println("\n***Transaction_Insert_OrdersListInfo***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.Id_item, values.Order_id}); err != nil {
		return fmt.Errorf("TransactionInsertOrdersListInfo - redis.AddValueInTmp: %v", err)
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
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_orders_list_info", "", m.Id, values.Order_id, values.Id_item, values.Id_parent_item, values.Price_id, values.Price_name, values.Type_id, values.Cooking_tracker, values.Discount_id, values.Discount_name, values.Discount_percent, values.Price, values.Cook_hash, values.Start_time, values.End_time, values.Fail_id, values.Fail_user_hash, values.Fail_comments, values.Real_foodcost, values.Count, values.Type_name, values.Over_status_id, values.Time_cook, values.Time_fry, values.Set, values.Cook_role, values.Code_consist, values.PriceWithDiscount); err != nil {
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

			if err := T.Transaction_QTTV_One(false, "Update", "metrics_orders_list_info", "", values.Id_item, values.Order_id, m.Id, values.Id_parent_item, values.Price_id, values.Price_name, values.Type_id, values.Cooking_tracker, values.Discount_id, values.Discount_name, values.Discount_percent, values.Price, values.Cook_hash, values.Start_time, values.End_time, values.Fail_id, values.Fail_user_hash, values.Fail_comments, values.Real_foodcost, values.Count, values.Type_name, values.Over_status_id, values.Time_cook, values.Time_fry, values.Set, values.Cook_role, values.Code_consist, values.PriceWithDiscount); err != nil {
				return fmt.Errorf("Update.metrics_orders_list_info: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//TransactionInsertRole metrics_hash_name
func (T *Transaction) TransactionInsertRole(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsRole) error {
	//log.Println("\n***Transaction_Insert_Role***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Hash); err != nil {
		return fmt.Errorf("TransactionInsertRole - redis.AddValueInTmp: %v", err)
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

//TransactionInsertUser metrics_hash_name
func (T *Transaction) TransactionInsertUser(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsUser) error {
	//log.Println("\n***Transaction_Insert_User***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Hash); err != nil {
		return fmt.Errorf("TransactionInsertUser - redis.AddValueInTmp: %v", err)
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

//TransactionInsertPlan metrics_plan
func (T *Transaction) TransactionInsertPlan(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsPlan) error {
	//log.Println("\n***Transaction_Insert_Plan***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.PlanDate, values.PointHash, values.RoleHash}); err != nil {
		return fmt.Errorf("TransactionInsertPlan - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, []interface{}{values.PlanDate, values.PointHash, values.RoleHash}) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_plan", "Data_Point_Role", values.PlanDate, values.PointHash, values.RoleHash); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_plan: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_plan (Hash): ", values.Hash)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_plan", "", m.Id, values.PlanDate, values.PointHash, values.RoleHash, values.UserCounts, values.UserHashes); err != nil {
			return fmt.Errorf("Insert.metrics_plan: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_plan (Hash): ", values.Hash)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_plan", "", values.PlanDate, values.PointHash, values.RoleHash, m.Id, values.UserCounts, values.UserHashes); err != nil {
				return fmt.Errorf("Update.metrics_plan: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//TransactionInsertPoint metrics_hash_name
func (T *Transaction) TransactionInsertPoint(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsPoint) error {
	//log.Println("\n***Transaction_Insert_Point***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.Hash); err != nil {
		return fmt.Errorf("TransactionInsertPoint - redis.AddValueInTmp: %v", err)
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
		//log.Println("Already: Insert.metrics_hash_name (Hash): ", values.Hash)
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

//TransactionInsertEvents metrics_events
func (T *Transaction) TransactionInsertEvents(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsEvents) error {
	//log.Println("\n***Transaction_Insert_Events***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.OrderID, values.UserHash, values.TypeEvent, values.TimeEvent}); err != nil {
		return fmt.Errorf("TransactionInsertEvents - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, []interface{}{values.OrderID, values.UserHash, values.TypeEvent, values.TimeEvent}) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_events", "OrderID_UserHash_TypeEvent_TimeEvent", values.OrderID, values.UserHash, values.TypeEvent, values.TimeEvent); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_events: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_events (OrderID): ", values.OrderID)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_events", "", m.Id, values.OrderID, values.UserHash, values.UserRole, values.TypeEvent, values.TimeEvent, values.DurationEvent, values.Description, values.PointHash); err != nil {
			return fmt.Errorf("Insert.metrics_events: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_events (OrderID): ", values.OrderID)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_events", "", values.OrderID, values.UserHash, values.TypeEvent, values.TimeEvent, m.Id, values.UserRole, values.DurationEvent, values.Description, values.PointHash); err != nil {
				return fmt.Errorf("Update.metrics_events: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//TransactionInsertBonuses metrics_bonuses
func (T *Transaction) TransactionInsertBonuses(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsBonuses) error {
	//log.Println("\n***Transaction_Insert_Bonuses***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.BonusID); err != nil {
		return fmt.Errorf("TransactionInsertBonuses - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.BonusID) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_bonuses", "BonusID", values.BonusID); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_bonuses: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_bonuses (Phone): ", values.Phone)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_bonuses", "", m.Id, values.BonusID, values.Phone, values.TransactionBonus, values.TypeBonus, values.Note, values.ActionTime); err != nil {
			return fmt.Errorf("Insert.metrics_bonuses: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_bonuses (Phone): ", values.Phone)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_bonuses", "", values.BonusID, m.Id, values.Phone, values.TransactionBonus, values.TypeBonus, values.Note, values.ActionTime); err != nil {
				return fmt.Errorf("Update.metrics_bonuses: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//TransactionInsertUsers metrics_users
func (T *Transaction) TransactionInsertUsers(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsUsers) error {
	//log.Println("\n***Transaction_Insert_Users***")

	if err := redis.AddValueInTmp(m.Parameter_id, []interface{}{values.UserHash, values.UpdateTime}); err != nil {
		return fmt.Errorf("TransactionInsertUsers - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, []interface{}{values.UserHash, values.UpdateTime}) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_users", "UserHash_UpdatedTime", values.UserHash, values.UpdateTime); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_users: %v", err)
	}
	if T.HashData == nil {
		fullName := values.LastName + " " + values.FirstName + " " + values.SecondName
		//log.Println("New: Insert.metrics_users (UserHash): ", values.UserHash)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_users", "", m.Id, values.UserHash, values.UID, values.Password, values.LastName, values.FirstName, values.SecondName, values.RoleHash, values.PointHash, values.Phone, values.INN, values.HourRate, values.CountRate, values.VPNNumber, values.VPNPassword, values.Language, values.Level, values.LevelChangeTime, values.CheckPlan, values.CreateTime, values.DeleteTime, values.UpdateTime, fullName); err != nil {
			return fmt.Errorf("Insert.metrics_users: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		if SMS.MP.Update_allow == true {
			fullName := values.LastName + " " + values.FirstName + " " + values.SecondName
			//log.Println("Already: Insert.metrics_users (UserHash): ", values.UserHash)
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_users", "", values.UserHash, values.UpdateTime, m.Id, values.UID, values.Password, values.LastName, values.FirstName, values.SecondName, values.RoleHash, values.PointHash, values.Phone, values.INN, values.HourRate, values.CountRate, values.VPNNumber, values.VPNPassword, values.Language, values.Level, values.LevelChangeTime, values.CheckPlan, values.CreateTime, values.DeleteTime, fullName); err != nil {
				return fmt.Errorf("Update.metrics_users: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//TransactionInsertCashboxShift table: metrics_cashbox_shift
func (T *Transaction) TransactionInsertCashboxShift(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsCashboxShift) error {
	//log.Println("\n***Transaction_Insert_CashboxShift***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.CashRegister); err != nil {
		return fmt.Errorf("TransactionInsertCashboxShift - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.CashRegister) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_cashbox_shift", "CashRegister", values.CashRegister); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_cashbox_shift: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_cashbox_shift (CashRegister): ", values.CashRegister)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_cashbox_shift", "", m.Id, values.CashRegister, values.UserHash, values.PointHash, values.BeginTime, values.EndTime); err != nil {
			return fmt.Errorf("Insert.metrics_cashbox_shift: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_cashbox_shift (CashRegister): ", values.CashRegister)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_cashbox_shift", "", values.CashRegister, m.Id, values.UserHash, values.PointHash, values.BeginTime, values.EndTime); err != nil {
				return fmt.Errorf("Update.metrics_cashbox_shift: %v", err)
			}

			SMS.MP.CountInserted++
		}
	}

	//log.Println("******\n")

	return nil
}

//TransactionInsertSklad table: metrics_sklad
func (T *Transaction) TransactionInsertSklad(SMS *SMS, m *MetricsMetrics, values *GetDataForMetricsSklad) error {
	//log.Println("\n***TransactionInsertSklad***")

	if err := redis.AddValueInTmp(m.Parameter_id, values.SkladListID); err != nil {
		return fmt.Errorf("TransactionInsertSklad - redis.AddValueInTmp: %v", err)
	}
	if SMS.MP.Update_allow == false && redis.ExistValue(m.Parameter_id, values.SkladListID) {
		return nil
	}

	if err := T.Transaction_QTTV_One(true, "Select", "metrics_sklad", "SkladListID", values.SkladListID); err != nil && err.Error() != "sql: no rows in result set" {
		return fmt.Errorf("Select.metrics_sklad: %v", err)
	}
	if T.HashData == nil {

		//log.Println("New: Insert.metrics_sklad (CashRegister): ", values.CashRegister)
		if err := T.Transaction_QTTV_One(false, "Insert", "metrics_sklad", "", m.Id, values.SkladListID, values.OrderID, values.PointHash, values.SkladHash, values.PriceID, values.ProductHash, values.ProductName, values.Count, values.TypeUnits, values.ActionTime); err != nil {
			return fmt.Errorf("Insert.metrics_sklad: %v", err)
		}

		SMS.MP.CountInserted++
	} else {
		//log.Println("Already: Insert.metrics_sklad (CashRegister): ", values.CashRegister)
		if SMS.MP.Update_allow == true {
			if err := T.Transaction_QTTV_One(false, "Update", "metrics_sklad", "", values.SkladListID, m.Id, values.OrderID, values.PointHash, values.SkladHash, values.PriceID, values.ProductHash, values.ProductName, values.Count, values.TypeUnits, values.ActionTime); err != nil {
				return fmt.Errorf("Update.metrics_sklad: %v", err)
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

	//log.Println("Запрос на склад:", Q)
	//fmt.Println("Запрос на склад:", Q)

	fc := FoodCost{
		Date:     fn.FormatDate(answer.Start_time),
		Price_ID: float64(answer.Price_id),
		Count:    answer.Count,
	}

	//log.Println("fc:", fc)

	Answer_Message, err := connect.SelectMessageOLD(&conn, Q, fc)
	if err != nil {
		return fmt.Errorf("Answer_Message: %v", err)
	}

	//log.Println("Ответ со склада:", Answer_Message)

	if len(Answer_Message.Tables) != 0 { //Если данных не прилетело, то не надо в метрику данные записывать
		for _, VAL := range Answer_Message.Tables[0].Values { //Начинаем бегать по результату ответа
			if VAL == nil {
				return errors.New("критический сбой: Answer_Message cтруктура ответа = nil")
			}

			answer.Real_foodcost = VAL.(float64)

			//log.Println("answer:", answer)
			//fmt.Println("\nanswer:", answer)

			return nil
		}
	}

	return errors.New("Нет ответа в структуре")
}
