package postgresql

import (
	"MetricsTest/structures"
	"database/sql"
	"net"
	"time"

	pq "github.com/lib/pq"
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

type Result_summ struct {
	val  string
	val2 string
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
	ID, Interface_ID, Type_Mod_ID, Own_ID, Min_Step_ID, StepType_ID int64
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
/*Самостоятельные*/
type Metrics_franchise_hierarchy struct {
	ID, Hash, Name, Parent_hash string
}
type Metrics_role struct {
	ID, Hash, Name string
}

type Metrics_dop_data struct {
	MFH Metrics_franchise_hierarchy
	MR  Metrics_role

	q          structures.QueryMessage
	common     Common
	answerRows []string
	conn       net.Conn
	err        error
}

type Metrics_own struct {
	ID   int64
	Name string
}

type Metrics_cook_time struct {
	ID                                          int64
	UserHash, Price_ID                          string
	Date, MinTime, MidTime, MaxTime             time.Time
	DateStr, MinTimeStr, MidTimeStr, MaxTimeStr string
}

/*Конец самостоятельных*/
/*Повара*/
type Metrics_cook_count struct {
	ID       int64
	UserHash string
	Step_Id  int64
	Date     time.Time
	DateStr  string

	Made    int64
	SumMade float64

	Fail    int64
	SumFail float64

	Remake    int64
	SumRemake float64

	Slow    int64
	SumSlow float64
}

type Metrics_cook_prices struct {
	ID, Cook_count_ID, Price_ID, Status_Id int64
}

type Metrics_status struct {
	ID            int64
	Name, AddInfo string
}

/*Конец повара*/
/*Начало оператора*/
type Metrics_casher_count struct {
	ID       int64
	UserHash string
	Step_ID  int64
	Date     time.Time
	DateStr  string

	MadeOrders    int64
	SumMadeOrders float64

	MadeItems int64

	FailOrders    int64
	SumFailOrders float64

	CanceledOrders    int64
	SumCanceledOrders float64
}

type Metrics_operator_cells_count struct {
	ID, Metric_ID, MadeCalls, AcceptedCalls, FailCalls, BreakCalls int64
}

type Metrics_operator_time struct {
	Order_ID                           int64
	Cell_ID                            string
	Answer, FillingOrder, TimeDialogue time.Time
}

type Metrics_casher_time struct {
	Order_ID, Casher_ID   int64
	TimeForOrder          time.Time
	CountCheckSprinted    int64
	MidTimeSelectMenu     time.Time
	Status_ID             int64
	SumCountCheckSprinted float64
}

/*Конец оператора*/
/*На визуалку*/
//Отчеты

type Metrics_courier_info struct {
	Hash          string
	Count         float64
	ArrayOrdersID pq.Int64Array
}

type Metrics_cashbox struct {
	ID, Metric_ID, CashRegister, Type_payments int64
	Action_time, Date_preorder                 time.Time
	PointHash, UserHash, Info                  string
	Cash                                       float64

	Action_timeStr string
}

//////////////////////////////////////////////////////////////////////////
type GetDataForMetricsCashbox struct {
	ID, Order_id, CashRegister, Type_payments int64
	Action_time, Date_preorder                time.Time
	PointHash, UserHash, Info                 string
	Cash                                      float64
}

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
}

type GetDataForMetricsOrdersLists struct {
	Metric_id        int64
	Order_id         int64
	Id_item          int64
	Id_parent_item   int64
	Price_id         int64
	Price_name       string
	Type_id          int64
	Type_name        string
	Cooking_tracker  int64
	Discount_id      int64
	Discount_name    string
	Discount_percent int64
	Price            float64
	Cook_hash        string
	Start_time       time.Time
	End_time         time.Time
	Fail_id          int64
	Fail_user_hash   string
	Fail_comments    string //pq.StringArray
	Real_foodcost    float64
	Count            float64
	Point_hash       string
	Order_time       time.Time
}

type GetPendingDate struct {
	Min_date time.Time
	Min_id   int64
}

type MetricsMetrics struct {
	Id           interface{}
	Ownhash      string
	Date         time.Time
	Parameter_id int64
}