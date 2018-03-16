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

////////////////////////////////////////
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
	IsModifier     bool
}

type ReportCashboxPrepay struct {
	Summ float64
}

type ReportCashboxPostpay struct {
	Summ float64
}

type ReportCashboxReturn struct {
	Summ float64
}

type ReportCashbox struct {
	CashRegister, Type_payments int64
	Action_time, Date_preorder  time.Time
	UserHash, UserName, Info    string
	Cash                        float64
	IsOrder                     bool

	Action_timeStr string
}

type ReportSummOnTypePayments struct {
	TypePayment   int64
	CountPayments int64
	Summa         float64
	IsOrder       bool
}

type ReportOperator struct {
	Name  string
	Hash  string
	Count float64
}

type ReportCourier struct {
	Name          string
	Hash          string
	Count         int64
	OrdersArr     pq.Int64Array
	AvgTime       string
	CountOvertime int64
}

type ReportCourierDetailed struct {
	Name         string
	Hash         string
	City         string
	Street       string
	House        string
	Building     string
	Price        float64
	TimeDelivery string
	TimeTaken    string
}
type ReportTimeDelivery struct {
	TimeDelivery string
}

type ReportCancelOrders struct {
	OrderId    int64
	OrderTime  time.Time
	CancelTime time.Time
	UserName   string
	UserHash   string
	ReasonId   int64
	ReasonNote string
}

type ReportOrdersOnTime struct {
	Dates          time.Time
	Times          time.Time
	CountOrders    int64
	CountPreorders int64
	CountDelivery  int64
	CountTakeout   int64
}

type ReportPredictCouriersOnTime struct {
	Dates         time.Time
	Times         time.Time
	CountCouriers int64
}

type ReportPredictCollectorOnTime struct {
	Dates           time.Time
	Times           time.Time
	CountCollectors int64
}

type ReportAvgTimeRelayOnTime struct {
	Dates                   time.Time
	Times                   time.Time
	AvgTimeCourier          string
	AvgTimeTransferOuter    string
	AvgTimeTransferCourier  string
	AvgTimeTransferCustomer string
}

type ReportWorkloadOnTime struct {
	CookingType int64
	PointHash   string
	PointName   string
	Dates       time.Time
	Times       time.Time
	CountCook   int64
	Workload    int64
}

type ReportCook struct {
	Name           string
	Role           string
	Count          int64
	AvgTime        string
	CountOvertime  int64
	CookingTracker int64
}

//type Result_summ struct {
//	Val float64
//}

type BD_READ interface {
	Record(rows *sql.Rows) error
}

func (mai *Metrics_add_info) Record(rows *sql.Rows) error {
	return rows.Scan(&mai.Hash, &mai.Name, &mai.Type_id, &mai.Type_name, &mai.Units, &mai.Price, &mai.Price_id, &mai.Status_id, &mai.Count, &mai.Real_food_cost)
}
func (v *ReportSale) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Name, &v.Type_id, &v.Type_name, &v.Price, &v.Price_id, &v.Count, &v.Real_food_cost, &v.IsModifier)
}
func (v *ReportCashboxPrepay) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Summ)
}
func (v *ReportCashboxPostpay) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Summ)
}
func (v *ReportCashboxReturn) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Summ)
}
func (v *ReportCashbox) Record(rows *sql.Rows) error {
	return rows.Scan(&v.CashRegister, &v.Action_time, &v.UserHash, &v.UserName, &v.Info, &v.Type_payments, &v.Cash, &v.Date_preorder, &v.IsOrder)
}
func (v *ReportSummOnTypePayments) Record(rows *sql.Rows) error {
	return rows.Scan(&v.TypePayment, &v.CountPayments, &v.Summa, &v.IsOrder)
}
func (v *ReportOperator) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Name, &v.Hash, &v.Count)
}
func (v *ReportCourier) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Name, &v.Hash, &v.Count, &v.OrdersArr, &v.AvgTime, &v.CountOvertime)
}
func (v *ReportCourierDetailed) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Name, &v.Hash, &v.City, &v.Street, &v.House, &v.Building, &v.Price, &v.TimeDelivery, &v.TimeTaken)
}
func (v *ReportTimeDelivery) Record(rows *sql.Rows) error {
	return rows.Scan(&v.TimeDelivery)
}
func (v *ReportCancelOrders) Record(rows *sql.Rows) error {
	return rows.Scan(&v.OrderId, &v.OrderTime, &v.CancelTime, &v.UserName, &v.UserHash, &v.ReasonId, &v.ReasonNote)
}
func (v *ReportOrdersOnTime) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Dates, &v.Times, &v.CountOrders, &v.CountPreorders, &v.CountDelivery, &v.CountTakeout)
}
func (v *ReportPredictCouriersOnTime) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Dates, &v.Times, &v.CountCouriers)
}
func (v *ReportPredictCollectorOnTime) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Dates, &v.Times, &v.CountCollectors)
}
func (v *ReportAvgTimeRelayOnTime) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Dates, &v.Times, &v.AvgTimeCourier, &v.AvgTimeTransferCustomer, &v.AvgTimeTransferCourier, &v.AvgTimeTransferOuter)
}
func (v *ReportWorkloadOnTime) Record(rows *sql.Rows) error {
	return rows.Scan(&v.CookingType, &v.Dates, &v.Times, &v.PointHash, &v.PointName, &v.CountCook, &v.Workload)
}
func (v *ReportCook) Record(rows *sql.Rows) error {
	return rows.Scan(&v.Name, &v.Role, &v.Count, &v.AvgTime, &v.CountOvertime, &v.CookingTracker)
}
