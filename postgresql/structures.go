package postgresql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

/*Глобальный мап работы функции*/
var GlobMapUsing map[string]bool
var SQL_NO_ROWS string = "sql: no rows in result set"

/*Для передач в транзакцию*/
type Transaction struct {
	DataOne  TransactionAction
	Data     []TransactionAction
	Tx       *sql.Tx
	Row      *sql.Row
	HashData interface{}
}

type TransactionAction struct {
	Table, Query, Type string
	Values             []interface{}
}

/*Конец для передач в транзакцию*/
/* Какие то либо общие структуры */
type Metrics_step struct {
	ID            int64
	Name, Value   string
	DurationInt64 int64
	Duration      float64
}

type Rowsing struct {
	Row  *sql.Row
	Rows *sql.Rows
}

type Metrics_step_request struct {
	MS       Metrics_step
	MS_ARRAY []Metrics_step

	Roow Rowsing
	Err  error
}

type Metrics_link_step_coc struct {
	ID, Step_id int64
}

type Metrics_request struct {
	M       Metrics
	M_ARRAY []Metrics

	Roow Rowsing
	Err  error
}

type SMS struct {
	MSTEP  Metrics_step
	MSTEPT Metrics_step_type
	MP     Metrics_parameters
	MSD    Metrics_service_data
	MST    Metrics_service_table
	MS     Metrics_service
}

type Metrics_step_type struct {
	ID   int64
	Name string
}

/* Какие-либо общие структуры */
/*Красный блок*/
type Metrics_add_info struct {
	ID             int64
	Metric_id      int64
	Hash           string
	Name           string
	Type_id        int64
	Type_name      string
	Count          float64
	Units          string
	Price          float64
	Price_id       float64
	Status_id      int64
	Real_food_cost float64
}

type ReportSale struct {
	Name           string
	Type_id        int64
	Type_name      string
	Price          float64
	Price_id       float64
	Count          float64
	Real_food_cost float64
}

type Result_summ struct {
	Val float64
}

type Metrics_user_info struct {
	UserHash, UserName, RoleHash, RoleName, PointHash, PointName string
}

type Metrics_user_work_info struct {
	ID, Metric_ID                            int64
	RoleHash, RoleName, PointHash, PointName string
}

type Metrics struct {
	ID               int64
	OwnHash, OwnName string
	Date             time.Time
	Value            float64
	Step_ID          int64
	Parameter_ID     int64
}

type Metrics_parameters struct {
	ID, ServiceTableId, Timeout, Own_ID, Min_Step_ID, StepType_ID int64
	PendingDate                                                   time.Time
	PendingId                                                     int64
	Protocol_version                                              int
	Update_allow                                                  bool
	UpdateAt                                                      time.Time

	CountInserted int64
	Lock          bool
}

/*Конец красного блока*/
/*Левый блок метрики*/
type Metrics_type_mod struct {
	ID, Parent_ID int64
	Name          string
}

type Metrics_link_type_and_mod struct {
	ID, Type_ID, Mod_ID, Tab_ID   int64
	Type_Name, Mod_Name, Tab_Name string
	Info                          string
}

/*Конец левого блока метрики*/
/*Сервис дата*/
type Metrics_service_data struct {
	ID, Service_table int64
	End_date          time.Time
	End_dateStr       string
	End_ID            int64

	/* Дополнительные поля для локальной работы */
	StartDate    time.Time
	StartDateStr string
}

type Metrics_service struct {
	ID   int64
	Name string
	IP   string
}

type Metrics_service_table struct {
	ID                              int64
	Query, TableName, TypeParameter string
	Service_ID                      int64
	Activ                           bool
}

/*Конец сервис дата*/

//////////////////////////////////////////
//////////////////////////////////////////

//MetricsMetrics struct
type MetricsMetrics struct {
	Id           interface{}
	Ownhash      string
	Date         time.Time
	Parameter_id int64
}

//GetDataForMetricsCashbox struct
type GetDataForMetricsCashbox struct {
	ID, Order_id, CashRegister, Type_payments int64
	Action_time, Date_preorder                time.Time
	PointHash, UserHash, UserName, Info       string
	Cash                                      float64
}

//GetDataForMetricsOrders struct
type GetDataForMetricsOrders struct {
	Metric_id                  int64
	Order_id                   int64
	Chain_hash                 string
	Org_hash                   string
	Point_hash                 string
	Id_day_point               int64
	Cashregister_id            int64 //[]uint8
	Count_elements             int64
	Date_preorder_cook         time.Time
	Side_order                 int64
	Type_delivery              int64
	Type_payments              int64
	Price                      float64
	Bonus                      int64
	Discount_id                int64
	Discount_name              string
	Discount_percent           int64
	City                       string
	Street                     string
	House                      int64
	Building                   string
	Creator_hash               string
	Creator_role_hash          string
	Creator_time               time.Time
	Duration_of_create         int64
	Duration_of_select_element int64
	Cook_start_time            time.Time
	Cook_end_time              time.Time
	Collector_hash             string
	Collector_time             time.Time
	Courier_hash               string
	Courier_start_time         time.Time
	Courier_end_time           time.Time
	Cancel_hash                string
	Cancel_time                time.Time
	Cancellation_reason_id     int64 //string
	Cancellation_reason_note   string
	Crash_user_hash            string
	Crash_user_role_hash       string
	Compensation               bool
	Type_compensation          int64
	Type                       int64
	Customer_phone             string
	PriceWithDiscount          float64
	Division                   string
}

//GetDataForMetricsOrdersLists struct
type GetDataForMetricsOrdersLists struct {
	Metric_id         int64
	Order_id          int64
	Id_item           int64
	Id_parent_item    int64
	Price_id          int64
	Price_name        string
	Type_id           int64
	Type_name         string
	Cooking_tracker   int64
	Discount_id       int64
	Discount_name     string
	Discount_percent  int64
	Price             float64
	Cook_hash         string
	Start_time        time.Time
	End_time          time.Time
	Fail_id           int64
	Fail_user_hash    string
	Fail_comments     string //pq.StringArray
	Real_foodcost     float64
	Count             float64
	Point_hash        string
	Order_time        time.Time
	Over_status_id    int64
	Time_cook         int64
	Time_fry          int64
	Set               bool
	Cook_role         string
	Code_consist      int64
	PriceWithDiscount float64
}

//GetPendingDate struct
type GetPendingDate struct {
	Min_date time.Time
	Min_id   int64
}

//type GetDataForMetricsHashName struct {  //надо будет привести к этому виду
//	Metric_id        int64
//	OwnHash, OwnName string
//	CreatedTime      time.Time
//}

//GetDataForMetricsRole struct
type GetDataForMetricsRole struct {
	Hash          string
	Name          string
	Premission    int64
	TTL           int64
	ConnectInfo   string
	CreateTime    time.Time
	CreateTimeStr string
	NameInterface string
	TypeWage      string
	Salary        float64
	Deal          float64
	Rate          float64
}

//GetDataForMetricsUser struct
type GetDataForMetricsUser struct {
	Hash        string
	Name        string
	RoleHash    string
	OrgHash     string
	PhoneNumber string
}

//GetDataForMetricsPoint struct
type GetDataForMetricsPoint struct {
	Hash          string
	HashOrg       string
	City          string
	Street        string
	House         string
	CreateTime    time.Time
	CreateTimeStr string
	NameSklad     string
	Active        bool
	Lat           string
	Lon           string
}

//GetDataForMetricsPlan struct
type GetDataForMetricsPlan struct {
	PlanDate   time.Time
	PointHash  string
	RoleHash   string
	UserCounts pq.Int64Array
	UserHashes pq.StringArray
}

//GetDataForMetricsEvents struct
type GetDataForMetricsEvents struct {
	OrderID       int64
	UserHash      string
	UserRole      string
	TypeEvent     int64
	TimeEvent     time.Time
	DurationEvent int64
	Description   string
	PointHash     string
}

//GetDataForMetricsBonuses struct
type GetDataForMetricsBonuses struct {
	BonusID          int64     `json:"ID"`
	Phone            string    `json:"Phone"`
	TransactionBonus int64     `json:"Transaction_bonus"`
	TypeBonus        int64     `json:"TypeBonus"`
	Note             string    `json:"Note"`
	ActionTime       time.Time `json:"Activ_date"`
}

//GetDataForMetricsUsers struct
type GetDataForMetricsUsers struct {
	UserHash        string    `json:"UserHash"`
	UID             string    `json:"UID"`
	Password        string    `json:"Password"`
	LastName        string    `json:"SurName"`
	FirstName       string    `json:"FirstName"`
	SecondName      string    `json:"SecondName"`
	RoleHash        string    `json:"RoleHash"`
	PointHash       string    `json:"OrgHash"`
	Phone           string    `json:"PhoneNumber"`
	INN             string    `json:"INN"`
	HourRate        float64   `json:"HourRate"`
	CountRate       float64   `json:"CountRate"`
	VPNNumber       string    `json:"VPNNumber"`
	VPNPassword     string    `json:"VPNPassword"`
	Language        string    `json:"Lang"`
	Level           int64     `json:"Level"`
	LevelChangeTime time.Time `json:"LevelChangeTime"`
	CheckPlan       bool      `json:"Check_plan"`
	CreateTime      time.Time `json:"CreationTime"`
	DeleteTime      time.Time `json:"DelDate"`
	UpdateTime      time.Time `json:"UpdateTime"`
}

//GetDataForMetricsCashboxShift struct
type GetDataForMetricsCashboxShift struct {
	CashRegister int64
	UserHash     string
	PointHash    string
	BeginTime    time.Time
	EndTime      time.Time
}

//GetDataForMetricsSklad struct
type GetDataForMetricsSklad struct {
	SkladListID int64     `json:"ID"`
	OrderID     int64     `json:"OrderID"`
	PriceID     int64     `json:"PriceID"`
	ProductHash string    `json:"Hash"`
	ProductName string    `json:"Name"`
	Count       float64   `json:"Count"`
	TypeUnits   string    `json:"Uint"`
	PointHash   string    `json:"OrgHash"`
	SkladHash   string    `json:"Sklad"`
	ActionTime  time.Time `json:"ActionTime"`
}

//////////////////////////////

type FoodCost struct {
	Date      string
	Price_ID  float64
	Count     float64
	CostPrice float64
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type MetricValues interface {
	HashMethod() string
	DateMethod() time.Time
	Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error
}

/* *** metrics_cashbox *** */
func (mc GetDataForMetricsCashbox) HashMethod() string    { return mc.PointHash }
func (mc GetDataForMetricsCashbox) DateMethod() time.Time { return mc.Action_time }
func (mc GetDataForMetricsCashbox) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertCashbox(SMS, m, &mc); err != nil {
		return fmt.Errorf("TransactionInsertCashbox: %v", err)
	}
	return nil
}

// *** metrics_orders_info ***
func (mo GetDataForMetricsOrders) HashMethod() string    { return mo.Point_hash }
func (mo GetDataForMetricsOrders) DateMethod() time.Time { return mo.Creator_time }
func (mo GetDataForMetricsOrders) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertOrdersInfo(SMS, m, &mo); err != nil {
		return fmt.Errorf("TransactionInsertOrdersInfo: %v", err)
	}
	return nil
}

// *** metrics_orders_list_info ***
func (mol GetDataForMetricsOrdersLists) HashMethod() string    { return mol.Point_hash }
func (mol GetDataForMetricsOrdersLists) DateMethod() time.Time { return mol.Order_time }
func (mol GetDataForMetricsOrdersLists) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertOrdersListInfo(SMS, m, &mol); err != nil {
		return fmt.Errorf("TransactionInsertOrdersListInfo: %v", err)
	}
	return nil
}

// *** metrics_role ***
func (v GetDataForMetricsRole) HashMethod() string    { return v.Hash }
func (v GetDataForMetricsRole) DateMethod() time.Time { return v.CreateTime }
func (v GetDataForMetricsRole) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertRole(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertRole: %v", err)
	}
	return nil
}

// *** metrics_user ***
func (v GetDataForMetricsUser) HashMethod() string    { return v.Hash }
func (v GetDataForMetricsUser) DateMethod() time.Time { return time.Time{} }
func (v GetDataForMetricsUser) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertUser(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertUser: %v", err)
	}
	return nil
}

// *** metrics_plan ***
func (v GetDataForMetricsPlan) HashMethod() string    { return v.PointHash }
func (v GetDataForMetricsPlan) DateMethod() time.Time { return v.PlanDate }
func (v GetDataForMetricsPlan) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertPlan(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertPlan: %v", err)
	}
	return nil
}

// *** metrics_point ***
func (v GetDataForMetricsPoint) HashMethod() string    { return v.Hash }
func (v GetDataForMetricsPoint) DateMethod() time.Time { return v.CreateTime }
func (v GetDataForMetricsPoint) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertPoint(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertPoint: %v", err)
	}
	return nil
}

// *** metrics_events ***
func (v GetDataForMetricsEvents) HashMethod() string    { return v.UserHash }
func (v GetDataForMetricsEvents) DateMethod() time.Time { return v.TimeEvent }
func (v GetDataForMetricsEvents) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertEvents(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertEvents: %v", err)
	}
	return nil
}

// *** metrics_bonuses ***
func (v GetDataForMetricsBonuses) HashMethod() string    { return v.Phone }
func (v GetDataForMetricsBonuses) DateMethod() time.Time { return v.ActionTime }
func (v GetDataForMetricsBonuses) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertBonuses(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertBonuses: %v", err)
	}
	return nil
}

// *** metrics_users ***
func (v GetDataForMetricsUsers) HashMethod() string    { return v.UserHash }
func (v GetDataForMetricsUsers) DateMethod() time.Time { return v.UpdateTime }
func (v GetDataForMetricsUsers) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertUsers(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertUsers: %v", err)
	}
	return nil
}

// *** metrics_cashbox_shift ***
func (v GetDataForMetricsCashboxShift) HashMethod() string    { return v.PointHash }
func (v GetDataForMetricsCashboxShift) DateMethod() time.Time { return v.BeginTime }
func (v GetDataForMetricsCashboxShift) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	if err := Transaction.TransactionInsertCashboxShift(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertCashboxShift: %v", err)
	}
	return nil
}

// *** metrics_sklad ***
func (v GetDataForMetricsSklad) HashMethod() string    { return v.PointHash }
func (v GetDataForMetricsSklad) DateMethod() time.Time { return v.ActionTime }
func (v GetDataForMetricsSklad) Insert(SMS *SMS, Transaction *Transaction, m *MetricsMetrics) error {
	//v.PriceID = 0
	if err := Transaction.TransactionInsertSklad(SMS, m, &v); err != nil {
		return fmt.Errorf("TransactionInsertSklad: %v", err)
	}
	return nil
}
