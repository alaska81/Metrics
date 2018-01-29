package structures

import (
	"database/sql"
	"time"

	pq "github.com/lib/pq"
)

type QueryMessage struct { //старая
	Table         string
	Query         string
	TypeParameter string

	Values []interface{}
	Limit  int64
	Offset int64
}

type Message struct { //новая
	Tables []Table
	Query  string
	Error  Error
}

type Table struct {
	Name          string
	TypeParameter string
	Values        []interface{}
	Limit         int
	Offset        int
}

type Error struct {
	Code        int
	Type        string
	Description string
}

// *********************************
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
	Val float64
}

type BD_READ interface {
	Record(rows *sql.Rows) error
}

func (mai *Metrics_add_info) Record(rows *sql.Rows) error {
	return rows.Scan(&mai.Hash, &mai.Name, &mai.Type_id, &mai.Type_name, &mai.Units, &mai.Price, &mai.Price_id, &mai.Status_id, &mai.Count, &mai.Real_food_cost)
}
func (s *Result_summ) Record(rows *sql.Rows) error {
	return rows.Scan(&s.Val)
}

// ****************************

///*Метрики для Склада */
//type Comp_Sklad struct {
//	Hash  string
//	Name  string
//	Count float64
//	Price float64
//	Uint  string
//	Sklad string
//}

/*Метрики для точек*/
type Common_Answer struct {
	Str        string
	Val1       float64
	Val2       float64
	ArrayInt64 pq.Int64Array

	Hash  string
	Name  string
	Count float64
	Uint  string
	Sklad string

	Price      float64
	Price_id   float64
	Price_name string

	Type_id   int64
	Type_name string

	UserHash string
	OrgHash  string
	JobTime  float64
}

//type Common_Answer_New struct {
//	Hash       string
//	Price_id   float64
//	Price_name string
//	Count      float64
//	Price      float64
//}

///*Метрики по людям*/
//type MetrikaJobTime struct {
//	UserHash string
//	OrgHash  string
//	JobTime  float64
//}

/*То что летит с JS*/
type JS_Select struct {
	Table          string
	Query          string
	TypeParameter  string
	ParameterQuery int64
	Values         []interface{}
	Limit          int64
	Offset         int64
}

type Common_Answer_cashbox struct {
	ID, Order_id, Metric_id, CashRegister, Type_payments int64
	Action_time, Date_preorder                           time.Time
	PointHash, UserHash, Info                            string
	Cash                                                 float64
}

type MetricsMetrics struct {
	Id           interface{}
	Ownhash      string
	Date         time.Time
	Parameter_id int64
}

type GetDataForMetricsCashbox struct {
	ID, Order_id, CashRegister, Type_payments int64
	Action_time, Date_preorder                time.Time
	PointHash, UserHash, Info                 string
	Cash                                      float64
}

type GetDataForMetricsOrders struct {
	Id                         int64
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

type FoodCost struct {
	Date      string
	Price_ID  float64
	Count     float64
	CostPrice float64
}
