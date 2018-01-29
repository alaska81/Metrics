package postgresql

// Работа с postgresql

import (
	"database/sql"
	"errors"
	"log"
	//	"sync"

	"MetricsTest/config"

	_ "github.com/lib/pq"
)

var db *sql.DB
var Requests dbRequests

type dbRequests struct {
	//	rlock        *sync.RWMutex
	requestsList map[string]*sql.Stmt
}

func (dbr *dbRequests) initRequests() error {

	//	dbr.rlock = &sync.RWMutex{}
	//	dbr.rlock.Lock()
	//	defer dbr.rlock.Unlock()

	dbr.requestsList = make(map[string]*sql.Stmt)
	var err error

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics
	{
		/*Выгрузки на визуалку*/

		dbr.requestsList["Select.metrics.ReportSale"], err = db.Prepare(
			`SELECT mai.metric_id, mai.hash, mai.name, mai.type_id, mai.type_name, mai.units, sum(mai.price), mai.price_id, mai.status_id, sum(mai.Count), sum(mai.real_foodcost) 
			FROM metrics m 
			inner join metrics_add_info mai on m.id = mai.metric_id
			WHERE m.parameter_id=$1 and m.ownhash = $2 and date(m.date) = date($3)
			GROUP BY mai.metric_id, mai.type_id, mai.type_name, mai.name, mai.units, mai.price_id, mai.hash, mai.status_id
			ORDER BY mai.name`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportSaleByInterval"], err = db.Prepare(
			`SELECT mai.hash, mai.name, mai.type_id, mai.type_name, mai.units, sum(mai.price), mai.price_id, mai.status_id, sum(mai.Count), sum(mai.real_foodcost) 
			FROM metrics m 
			inner join metrics_add_info mai on m.id = mai.metric_id
			WHERE m.parameter_id=$1 and m.ownhash = $2 and date(m.date) >= date($3) and date(m.date) <= date($4)
			GROUP BY mai.type_id, mai.type_name, mai.name, mai.units, mai.price_id, mai.hash, mai.status_id
			ORDER BY mai.name`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportSaleNewByInterval"], err = db.Prepare(`
			SELECT moli.price_name, moli.type_id, 
			coalesce(moli.type_name, ''), 
			sum(moli.price-(moli.price*moli.discount_percent/100)), moli.price_id, sum(moli.count), sum(moli.real_foodcost)
			FROM metrics m 
			inner join metrics_orders_info moi on m.id = moi.metric_id
			inner join metrics_orders_list_info moli on moi.order_id = moli.order_id AND moli.id_parent_item = 0
			WHERE m.parameter_id = $1 AND m.ownhash = $2 AND 
				((date(moi.date_preorder_cook) = date('0001-01-01') AND date(moi.creator_time) >= date($3) AND date(moi.creator_time) <= (date($4) - interval '1 day'))
				OR (moi.date_preorder_cook >= $3 AND moi.date_preorder_cook < $4))
				AND moi.type <> 4
			GROUP BY moli.price_name, moli.type_id, moli.type_name, moli.price_id
			ORDER BY moli.price_name
			`)
		//--OR (date(moi.date_preorder_cook) = (date('2018-01-14')+interval '1 day') AND moi.date_preorder_cook::time < '06:00:00'::time)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportSummaOnTypePaymentsFromCashBox"], err = db.Prepare(`
			SELECT coalesce(sum(mc.cash), 0)
			FROM metrics m 
			inner join metrics_orders_info moi on m.id = moi.metric_id
			inner join metrics_cashbox mc on moi.order_id = mc.order_id
			WHERE m.parameter_id = $1 AND m.ownhash = $2 AND 
				((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= $3 AND moi.creator_time <= $4)
				OR (moi.date_preorder_cook >= $3 AND moi.date_preorder_cook <= $4))
				AND moi.type <> 4 
				AND mc.type_payments = $5
			`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportCourier"], err = db.Prepare(
			`SELECT dd.hash, count(dd.arr) as c, array_agg( dd.arr ) 
			FROM 
			(
				SELECT m.id,maa.hash, unnest(order_id) as arr
				FROM metrics_add_array maa
				INNER JOIN metrics m on m.id = maa.metric_id
				WHERE m.parameter_id=$1 and m.ownhash = $2 and date(m.date) = date($3)
			) dd GROUP BY dd.hash ORDER BY c DESC`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportCourierByInterval"], err = db.Prepare(
			`SELECT dd.hash, count(dd.arr) as c, array_agg( dd.arr ) 
			FROM 
			(
				SELECT m.id,maa.hash, unnest(order_id) as arr
				FROM metrics_add_array maa
				INNER JOIN metrics m on m.id = maa.metric_id
				WHERE m.parameter_id=$1 and m.ownhash = $2 and date(m.date) >= date($3) and date(m.date) <= date($4)
			) dd GROUP BY dd.hash ORDER BY c DESC`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportOperator"], err = db.Prepare(
			`SELECT m.ownhash, m.value
			FROM metrics m 
			WHERE m.parameter_id=$1 and date(m.date) = date($2)
			ORDER BY m.value DESC`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportOperatorByInterval"], err = db.Prepare(
			`SELECT m.ownhash, sum(m.value) as val
			FROM metrics m 
			WHERE m.parameter_id=$1 and date(m.date) >= date($2) and date(m.date) <= date($3)
			GROUP BY m.ownhash ORDER BY val DESC`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportCashbox"], err = db.Prepare(
			`SELECT cashregister, action_time, userhash, info, type_payments, cash, date_preorder
				FROM metrics m
				inner join metrics_cashbox mc on mc.metric_id = m.id
				WHERE m.parameter_id=$1 and m.ownhash = $2 and cashregister in (SELECT cashregister FROM metrics_cashbox WHERE date(action_time) = date($3) and action_time::time > '06:00:00'::time)
				ORDER BY cashregister, action_time`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportCashboxByInterval"], err = db.Prepare(
			`SELECT cashregister, action_time, userhash, info, type_payments, cash, date_preorder
				FROM metrics m
				inner join metrics_cashbox mc on mc.metric_id = m.id
				WHERE m.parameter_id=$1 and m.ownhash = $2 and cashregister in (SELECT cashregister FROM metrics_cashbox WHERE date(action_time) >= date($3) and date(action_time) <= date($4) and action_time::time > '06:00:00'::time)
				ORDER BY cashregister, action_time`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.Report"], err = db.Prepare(
			`SELECT mai.id, mai.metric_id, mai.hash, mai.name, mai.units, mai.price, mai.price_id, mai.status_id, mai.Count 
		  FROM metrics m 
			inner join metrics_add_info mai on m.id = mai.metric_id
		   WHERE m.parameter_id=$1 and m.ownhash = $2 and date(m.date) = date($3)`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ReportByInterval"], err = db.Prepare(
			`SELECT mai.hash, mai.name, mai.units, sum(mai.price), mai.price_id, mai.status_id, sum(mai.Count) 
			FROM metrics m 
				inner join metrics_add_info mai on m.id = mai.metric_id
			WHERE m.parameter_id=$1 and m.ownhash = $2 and date(m.date) >= date($3) and date(m.date) <= date($4)
			GROUP BY mai.hash, mai.name, mai.units,	mai.price_id, mai.status_id `)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.Parameters"], err = db.Prepare(
			`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID 
			FROM metrics 
			WHERE 	Parameter_ID=$1 and 
					OwnHash=$2 and 
					date(Date) = date($3)`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.ParametersByInterval"], err = db.Prepare(
			`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID 
			FROM metrics 
			WHERE 	Parameter_ID=$1 and 
					OwnHash=$2 and 
					date(Date) >= date($3) and 
					date(Date) <= date($4)`)
		if err != nil {
			return err
		}

		/*Конец выкгрузок на визуалку*/

		dbr.requestsList["Insert.metrics."], err = db.Prepare(`INSERT INTO metrics(OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID) VALUES ($1, $2, date($3), $4, $5, $6) RETURNING ID`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics.Id"], err = db.Prepare(`UPDATE metrics SET OwnHash=$2, OwnName=$3, Date=$4, Value=$5, Step_ID=$6, Parameter_ID=$7 WHERE ID=$1`)
		if err != nil {
			return err
		}

		dbr.requestsList["Update.metrics.AddValueById"], err = db.Prepare(`UPDATE metrics SET Value=Value + $2 WHERE ID=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics.ValueById"], err = db.Prepare(`UPDATE metrics SET Value=$2 WHERE ID=$1`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics."], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics.Id"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE ID=$1`)
		if err != nil {
			return err
		}

		/*dbr.requestsList["Select.metrics.Parameter_idMS(1)"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE Parameter_ID=$1 and Step_ID = 1 and EXTRACT(minute FROM Date) = EXTRACT(minute FROM $2::TIMESTAMP ) and date(Date) = date($2) LIMIT 1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics.Parameter_idMS(3)"], err = db.Prepare(`SELECT distinct m.ID, m.OwnHash, m.OwnName, m.Date, m.Value, m.Step_ID, m.Parameter_ID
			FROM metrics m
			right join metrics_add_info mai on mai.metric_id = m.id and mai.hash = $3
			WHERE m.Parameter_ID=$1 and m.Step_ID = 3 and
			date(m.Date) = date($2) LIMIT 1`)
		if err != nil {
			return err
		}*/

		dbr.requestsList["Select.metrics.Parameter_idMS(3)"], err = db.Prepare(
			`SELECT distinct m.ID, m.OwnHash, m.OwnName, m.Date, m.Value, m.Step_ID, m.Parameter_ID 
		FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash = $3 
		WHERE m.Parameter_ID=$1 and m.Step_ID = 3 and 
		date(m.Date) = date($2) LIMIT 1`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.Parameter_idMS(4)"], err = db.Prepare(`SELECT distinct m.ID, m.OwnHash, m.OwnName, m.Date, m.Value, m.Step_ID, m.Parameter_ID 
		FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash =  $3 
		WHERE  m.Parameter_ID=$1 and m.Step_ID = 4 and 
		EXTRACT(MONTH FROM m.Date) = EXTRACT(MONTH FROM $2::TIMESTAMP) and 
		EXTRACT(YEAR FROM m.Date) = EXTRACT(YEAR FROM $2::TIMESTAMP) LIMIT 1`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics.Parameter_idMS(5)"], err = db.Prepare(`SELECT distinct m.ID, m.OwnHash, m.OwnName, m.Date, m.Value, m.Step_ID, m.Parameter_ID 
		FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash =  $3 
		WHERE   m.Parameter_ID=$1 and m.Step_ID = 5 and 
		EXTRACT(YEAR FROM m.Date) = EXTRACT(YEAR FROM $2::TIMESTAMP) LIMIT 1`)
		if err != nil {
			return err
		}

		/*Для выборок ID*/
		dbr.requestsList["SelectID.metrics.Parameter_idMS(1)"], err = db.Prepare(`SELECT m.ID FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash = $4 
		WHERE m.Parameter_ID=$1 and date(m.Date) = date($2) 
		and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $2::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $2::TIMESTAMP) 
		and m.OwnHash = $3 and m.Step_ID = 2 LIMIT 1`)
		if err != nil {
			return err
		}
		dbr.requestsList["SelectID.metrics.Parameter_idMS(2)"], err = db.Prepare(`SELECT m.ID FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash = $4 
		WHERE m.Parameter_ID=$1 and date(m.Date) = date($2) 
		and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $2::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $2::TIMESTAMP) 
		and m.OwnHash = $3 and m.Step_ID = 2 LIMIT 1`)
		if err != nil {
			return err
		}

		dbr.requestsList["SelectID.metrics.Parameter_idMS(3)"], err = db.Prepare(`SELECT m.ID FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash = $4 
		WHERE m.Parameter_ID=$1 and date(m.Date) = date($2) and m.OwnHash = $3 and m.Step_ID = 3 LIMIT 1`)
		if err != nil {
			return err
		}

		dbr.requestsList["SelectID.metrics.Parameter_idMS(4)"], err = db.Prepare(`SELECT m.ID 
		FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash = $4 
		WHERE m.Parameter_ID=$1 and m.OwnHash = $3 and m.Step_ID = 4 and 
		EXTRACT(MONTH FROM m.Date) = EXTRACT(MONTH FROM $2::TIMESTAMP) and 
		EXTRACT(YEAR FROM m.Date) = EXTRACT(YEAR FROM $2::TIMESTAMP) LIMIT 1`)
		if err != nil {
			return err
		}

		dbr.requestsList["SelectID.metrics.Parameter_idMS(5)"], err = db.Prepare(`SELECT m.ID 
		FROM metrics m  
		right join metrics_add_info mai on mai.metric_id = m.id and mai.hash = $4 
		WHERE m.Parameter_ID=$1 and m.OwnHash = $3 and m.Step_ID = 5 and 
		EXTRACT(YEAR FROM m.Date) = EXTRACT(YEAR FROM $2::TIMESTAMP) LIMIT 1`)
		if err != nil {
			return err
		}

		if dbr.requestsList["Check.metrics.DateStep_idParameter_idMS(1)"], err = db.Prepare(`SELECT count(ID)<>0 FROM metrics WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=1`); err != nil {
			return err
		}
		if dbr.requestsList["Check.metrics.DateStep_idParameter_idMS(2)"], err = db.Prepare(`SELECT count(ID)<>0 FROM metrics WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=2`); err != nil {
			return err
		}

		if dbr.requestsList["Check.metrics.DateStep_idParameter_idMS(3)"], err = db.Prepare(`SELECT count(ID)<>0 FROM metrics WHERE date(Date) = date($1) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=3`); err != nil {
			return err
		}

		if dbr.requestsList["Check.metrics.DateStep_idParameter_idMS(4)"], err = db.Prepare(`SELECT count(ID)<>0 FROM metrics WHERE EXTRACT(MONTH FROM Date) = EXTRACT(MONTH FROM $1::TIMESTAMP) and EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=4`); err != nil {
			return err
		}

		if dbr.requestsList["Check.metrics.DateStep_idParameter_idMS(5)"], err = db.Prepare(`SELECT count(ID)<>0 FROM metrics WHERE EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=5`); err != nil {
			return err
		}

		//SELECT

		if dbr.requestsList["Select.metrics.DateStep_idParameter_idMS(1)"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.DateStep_idParameter_idMS(2)"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=2`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.DateStep_idParameter_idMS(3)"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE date(Date) = date($1) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=3`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.DateStep_idParameter_idMS(4)"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE EXTRACT(MONTH FROM Date) = EXTRACT(MONTH FROM $1::TIMESTAMP) and EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=4`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.DateStep_idParameter_idMS(5)"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=5`); err != nil {
			return err
		}

		if dbr.requestsList["Select.metrics.JSONDateStep_idParameter_idMS(1)"], err = db.Prepare(`
					select row_to_json(row)::varchar from (
					SELECT ID, Value FROM metrics
					WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP)
					and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=1) row`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.JSONDateStep_idParameter_idMS(2)"], err = db.Prepare(`
					select row_to_json(row)::varchar from (
					SELECT ID, Value FROM metrics
					WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP)
					and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=2) row`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.JSONDateStep_idParameter_idMS(3)"], err = db.Prepare(`
					select row_to_json(row)::varchar from (
					SELECT ID, Value FROM metrics
					WHERE date(Date) = date($1) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=3) row`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.JSONDateStep_idParameter_idMS(4)"], err = db.Prepare(`
					select row_to_json(row)::varchar from (
					SELECT ID, Value FROM metrics
					WHERE EXTRACT(MONTH FROM Date) = EXTRACT(MONTH FROM $1::TIMESTAMP)
					and EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=4) row`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics.JSONDateStep_idParameter_idMS(5)"], err = db.Prepare(`
					select row_to_json(row)::varchar from (
					SELECT ID, Value FROM metrics
					WHERE EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=5) row`); err != nil {
			return err
		}

		//SelectID
		if dbr.requestsList["SelectID.metrics."], err = db.Prepare(`SELECT ID FROM metrics WHERE date(Date)::date = date($1)::date and Parameter_ID=$2 and OwnHash=$3`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics.DateStep_idParameter_idMS(1)"], err = db.Prepare(`SELECT ID FROM metrics WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=1`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics.DateStep_idParameter_idMS(2)"], err = db.Prepare(`SELECT ID FROM metrics WHERE date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=2`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics.DateStep_idParameter_idMS(3)"], err = db.Prepare(`SELECT ID FROM metrics WHERE date(Date) = date($1) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=3`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics.DateStep_idParameter_idMS(4)"], err = db.Prepare(`SELECT ID FROM metrics WHERE EXTRACT(MONTH FROM Date) = EXTRACT(MONTH FROM $1::TIMESTAMP) and EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=4`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics.DateStep_idParameter_idMS(5)"], err = db.Prepare(`SELECT ID FROM metrics WHERE EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and Parameter_ID=$2 and OwnHash=$3 and Step_ID=5`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_cashbox
	{
		if dbr.requestsList["Insert.metrics_cashbox."], err = db.Prepare(
			`INSERT INTO metrics_cashbox(metric_id, order_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_cashbox."], err = db.Prepare(
			`SELECT id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder 
				FROM metrics_cashbox`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_cashbox.Metric_id"], err = db.Prepare(
			`SELECT id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder 
				FROM metrics_cashbox WHERE metric_id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_cashbox.JSONMetric_idAction_time"], err = db.Prepare(
			`select row_to_json(row)::varchar from ( 
				select id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder from metrics_cashbox 
				WHERE metric_id=$1 and action_time=$2) row `); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_cashbox."], err = db.Prepare(
			`select row_to_json(row)::varchar from ( 
				select id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder from metrics_cashbox 
				WHERE metric_id=$1 and action_time=$2) row `); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_orders_info
	{
		if dbr.requestsList["Insert.metrics_orders_info."], err = db.Prepare(
			`INSERT INTO metrics_orders_info(metric_id, order_id, chain_hash, org_hash, point_hash, id_day_point, cashregister_id, count_elements, date_preorder_cook, side_order, type_delivery, type_payments, price, bonus, discount_id, discount_name, discount_percent, city, street, house, building, creator_hash, creator_role_hash, creator_time, duration_of_create, duration_of_select_element, cook_start_time, cook_end_time, collector_hash, collector_time, courier_hash, courier_start_time, courier_end_time, cancel_hash, cancel_time, cancellation_reason_id, cancellation_reason_note, crash_user_hash, crash_user_role_hash, compensation, type_compensation, type)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42)`); err != nil {
			return err
		}
		//		if dbr.requestsList["Select.metrics_orders_info."], err = db.Prepare(
		//			`SELECT id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder
		//				FROM metrics_orders_info`); err != nil {
		//			return err
		//		}
		if dbr.requestsList["Select.metrics_orders_info.Order_id"], err = db.Prepare(
			`SELECT id FROM metrics_orders_info WHERE order_id = $1`); err != nil {
			return err
		}
		//		if dbr.requestsList["Select.metrics_orders_info.JSONMetric_idAction_time"], err = db.Prepare(
		//			`select row_to_json(row)::varchar from (
		//				select id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder from metrics_orders_info
		//				WHERE metric_id=$1 and action_time=$2) row `); err != nil {
		//			return err
		//		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_orders_list_info
	{
		if dbr.requestsList["Insert.metrics_orders_list_info."], err = db.Prepare(
			`INSERT INTO metrics_orders_list_info(metric_id, order_id, id_item, id_parent_item, price_id, price_name, type_id, cooking_tracker, discount_id, discount_name, discount_percent, price, cook_hash, start_time, end_time, fail_id, fail_user_hash, fail_comments, real_foodcost, count, type_name)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_orders_list_info.IdItem_OrderId"], err = db.Prepare(
			`SELECT id FROM metrics_orders_list_info WHERE id_item = $1 and order_id = $2`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_add_info
	{
		if dbr.requestsList["Insert.metrics_add_info."], err = db.Prepare(`INSERT INTO metrics_add_info(metric_id, hash, name, count, units, price) VALUES ($1, $2, $3, $4, $5, $6)`); err != nil {
			return err
		} //Insert.metrics_add_info.Cook
		if dbr.requestsList["Insert.metrics_add_info.Point"], err = db.Prepare(`INSERT INTO metrics_add_info(metric_id, hash, name, type_id, type_name, count, units, price, price_id, status_id, real_foodcost) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`); err != nil {
			return err
		}
		if dbr.requestsList["Insert.metrics_add_info.Cook"], err = db.Prepare(`INSERT INTO metrics_add_info(metric_id, price_id, status_id) VALUES ($1, $2, $3)`); err != nil {
			return err
		}
		if dbr.requestsList["Update.metrics_add_info.CountPriceId"], err = db.Prepare(`UPDATE metrics_add_info SET count = $2, price = $3 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Update.metrics_add_info.AddCountPrice"], err = db.Prepare(`UPDATE metrics_add_info SET count = count + $2, price = price + $3 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_add_info."], err = db.Prepare(`SELECT id, metric_id, hash, name, units, price FROM metrics_add_info`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_add_info.Metric_id"], err = db.Prepare(`SELECT id, metric_id, hash, name, units, price FROM metrics_add_info WHERE metric_id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_add_info.JSONMetric_idHash"], err = db.Prepare(`select row_to_json(row)::varchar from ( 
		select id, metric_id, hash, name, count, units, price 
		from metrics_add_info ) row WHERE metric_id=$1 and hash=$2`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_add_info.JSONMetric_idPrice_id"], err = db.Prepare(`select row_to_json(row)::varchar from ( 
		select id, metric_id, hash, name, count, units, price, price_id 
		from metrics_add_info ) row WHERE metric_id=$1 and price_id=$2`); err != nil {
			return err
		}
		if dbr.requestsList["Check.metrics_add_info.Metric_idHash"], err = db.Prepare(`SELECT count(ID)<>0 FROM metrics_add_info WHERE metric_id=$1 and hash=$2`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_add_info.Metric_idHash"], err = db.Prepare(`SELECT ID FROM metrics_add_info WHERE metric_id=$1 and hash=$2`); err != nil {
			return err
		}

		if dbr.requestsList["Update.metrics_add_info.NumberOfLastRecord"], err = db.Prepare(`UPDATE metrics_add_info
		   SET count=count+$7
			 WHERE hash=$6 and
				metric_id=(select m.id from metrics m
				inner join metrics_parameters mp
					on mp.id=m.parameter_id
				inner join metrics_service_data msd
					on msd.service_table=mp.interface_id
				inner join metrics_service_table mst
					on mst.id=msd.service_table and mst.query=$1 and mst.tablename=$2 and mst.typeparameter=$3
				where m.ownhash = $4 and date(m.date)=date($5))`); err != nil {
			return err
		}
		/*
			UPDATE metrics_add_info
			   SET count=1
			 WHERE hash='895c5d440bdcc1c800953ff58657c1c8b73bc29ef0464095c4e503b3e4760df3' and
				metric_id=(select m.id from metrics m
				inner join metrics_parameters mp
					on mp.id=m.parameter_id
				inner join metrics_service_data msd
					on msd.service_table=mp.interface_id
				inner join metrics_service_table mst
					on mst.id=msd.service_table and mst.tablename='Rashod' and mst.query='Select' and mst.typeparameter='SkladDate'
				where m.ownhash = 'SkladMicroFive' and date(m.date)=date('2017-08-23'));
		*/

		/*
			Выгрузки на визуалку

		*/
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_add_array
	{
		if dbr.requestsList["Insert.metrics_add_array."], err = db.Prepare(`INSERT INTO metrics_add_array(metric_id, hash, order_id) VALUES ($1, $2, $3)`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_add_array."], err = db.Prepare(`SELECT id, metric_id, hash, order_id FROM metrics_add_array`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_add_array.Metric_id"], err = db.Prepare(`SELECT id, metric_id, hash, order_id FROM metrics_add_array WHERE metric_id=$1`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_user_work_info
	{
		dbr.requestsList["Insert.metrics_user_work_info."], err = db.Prepare(`INSERT INTO metrics_user_work_info (metric_id, rolehash, rolename, pointhash, pointname) VALUES ($1, $2, $3, $4, $5)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_user_work_info."], err = db.Prepare(`SELECT id, metric_id, rolehash, rolename, pointhash, pointname FROM metrics_user_work_info`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_user_work_info.Metric_id"], err = db.Prepare(`SELECT id, metric_id, rolehash, rolename, pointhash, pointname FROM metrics_user_work_info WHERE metric_id=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_dop_data
	{
		/*
			1 - иерархия организаций
			2 - роли организаций
		*/

		if dbr.requestsList["Insert.metrics_dop_data."], err = db.Prepare(`
			INSERT INTO metrics_dop_data (str1, str2, str3, value1, value2, value3, date, data_id) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`); err != nil {
			return err
		}
		/*
		   str1 character varying DEFAULT 'DEFAULT'::character varying,
		   str2 character varying DEFAULT 'DEFAULT'::character varying,
		   str3 character varying DEFAULT 'DEFAULT'::character varying,
		   value1 numeric DEFAULT (-1),
		   value2 numeric DEFAULT (-1),
		   value3 numeric DEFAULT (-1),
		   date timestamp without time zone DEFAULT '0001-01-01 00:00:00'::timestamp without time zone,
		   data_id numeric NOT NULL,
		*/
		if dbr.requestsList["Select.metrics_dop_data.Franchise_hierarchy"], err = db.Prepare(`
			SELECT id, str1, str2, str3
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE date <= $2 and data_id=1 ORDER BY date DESC LIMIT 1)
			AND str1=$1 and data_id=1
		`); err != nil {
			return err
		}

		if dbr.requestsList["Select.metrics_dop_data.All_Franchise_hierarchy"], err = db.Prepare(`
			SELECT id, str1, str2, str3
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE data_id=1 ORDER BY date DESC LIMIT 1)
			and data_id=1
		`); err != nil {
			return err
		}

		if dbr.requestsList["Select.metrics_dop_data.Role"], err = db.Prepare(`
			SELECT id, str1, str2
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE date <= $2 and data_id=2 ORDER BY date DESC LIMIT 1)
			AND str1=$1 and data_id=2
		`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_dop_data.All_Role"], err = db.Prepare(`
			SELECT id, str1, str2
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE data_id=2 ORDER BY date DESC LIMIT 1)
			and data_id=2
		`); err != nil {
			return err
		}

		if dbr.requestsList["Select.metrics_dop_data.Str1DateData"], err = db.Prepare(`
			SELECT id, str1, str2, str3, value1, value2, value3, date, data_id
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE date <= $2 and data_id=$3 ORDER BY date DESC LIMIT 1)
			AND str1=$1 and data_id=$3
		`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_dop_data.Str2DateData"], err = db.Prepare(`
			SELECT id, str1, str2, str3, value1, value2, value3, date, data_id
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE date <= $2 and data_id=$3 ORDER BY date DESC LIMIT 1)
			AND str2=$1 and data_id=$3
		`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_dop_data.Str3DateData"], err = db.Prepare(`
			SELECT id, str1, str2, str3, value1, value2, value3, date, data_id
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE date <= $2 and data_id=$3 ORDER BY date DESC LIMIT 1)
			AND str3=$1 and data_id=$3
		`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_dop_data.LoadHierarchy"], err = db.Prepare(`
			SELECT id, str1, str2, str3
			FROM metrics_dop_data
			WHERE date = (SELECT date FROM metrics_dop_data WHERE data_id=1 AND date <= localtimestamp ORDER BY date DESC LIMIT 1)
			and str2=$1 and data_id=1
		`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_parameters
	{
		dbr.requestsList["Insert.metrics_parameters."], err = db.Prepare(`INSERT INTO metrics_parameters (Interface_ID, Type_Mod_ID,  Own_ID, Min_Step_ID) VALUES ($1, $2, $3, $4)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_parameters.Id"], err = db.Prepare(`UPDATE metrics_parameters SET Interface_ID=$2, Type_Mod_ID=$3, Own_ID=$4, Min_Step_ID=$5 WHERE ID=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_parameters."], err = db.Prepare(`SELECT ID, Interface_ID, Type_Mod_ID, Own_ID, Min_Step_ID FROM metrics_parameters`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_parameters.Id"], err = db.Prepare(`SELECT ID, Interface_ID, Type_Mod_ID, Own_ID, Min_Step_ID FROM metrics_parameters WHERE ID=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_parameters.Own_id"], err = db.Prepare(`SELECT ID, Interface_ID, Type_Mod_ID, Own_ID, Min_Step_ID FROM metrics_parameters WHERE Own_ID=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_link_step -- Общие данные
	{
		dbr.requestsList["Select.metrics_link_step."], err = db.Prepare(
			`SELECT MP.Min_Step_ID, 
			MSTEP.ID, MSTEP.Name, MSTEP.Value, EXTRACT(EPOCH FROM MSTEP.Value::INTERVAL)/60 as minuts, 
			MSTEPT.ID, MSTEPT.Name,
			MP.ID, MP.Interface_ID, MP.Type_Mod_ID, MP.Own_ID, 
			MSD.ID, MSD.Service_Table, MSD.End_Date, MSD.End_ID,  
			MST.ID, MST.Query, MST.TableName, MST.TypeParameter, MST.Service_ID, MST.Activ,   
			MS.ID, MS.Name, MS.IP   
			FROM metrics_parameters MP   
			INNER JOIN metrics_step MSTEP ON MP.Min_Step_ID=MSTEP.ID   
			INNER JOIN metrics_service_data MSD ON MP.Interface_ID=MSD.ID  
			INNER JOIN metrics_service_table MST ON MSD.Service_Table=MST.ID 
			INNER JOIN metrics_service MS ON MST.Service_ID=MS.ID
			INNER JOIN metrics_step_type MSTEPT ON MSTEPT.ID=MP.step_type_id
			WHERE MST.Activ = true
			ORDER BY MS.Name, minuts, MST.TypeParameter`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_type
	{
		dbr.requestsList["Insert.metrics_type."], err = db.Prepare(`INSERT INTO metrics_type (parent_id, name) VALUES ($1, $2)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_type.Id"], err = db.Prepare(`UPDATE metrics_type SET parent_id=$2, name=$3 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_type."], err = db.Prepare(`SELECT id, parent_id, name FROM metrics_type`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_type.Parent_id"], err = db.Prepare(`SELECT id, parent_id, name FROM metrics_type WHERE parent_id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Delete.metrics_type.Id"], err = db.Prepare(`DELETE FROM metrics_type WHERE ID=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_mod
	{
		dbr.requestsList["Insert.metrics_mod."], err = db.Prepare(`INSERT INTO metrics_mod (parent_id, name) VALUES ($1, $2)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_mod.Id"], err = db.Prepare(`UPDATE metrics_mod SET parent_id=$2, name=$3 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_mod."], err = db.Prepare(`SELECT id, parent_id, name FROM metrics_mod ORDER BY parent_id, name ASC`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_mod.Parent_id"], err = db.Prepare(`SELECT id, parent_id, name FROM metrics_mod WHERE parent_id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Delete.metrics_mod.Id"], err = db.Prepare(`DELETE FROM metrics_mod WHERE ID=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_link_type_and_mod
	{
		dbr.requestsList["Insert.metrics_link_type_and_mod."], err = db.Prepare(`INSERT INTO metrics_link_type_and_mod (type_id, mod_id, info) VALUES ($1, $2, $3)`)
		if err != nil {
			return err
		}

		dbr.requestsList["Update.metrics_link_type_and_mod.Id"], err = db.Prepare(`UPDATE metrics_link_type_and_mod SET type_id=$2, mod_id=$3, info=$4 WHERE id=$1`)
		if err != nil {
			return err
		}

		dbr.requestsList["Select.metrics_link_type_and_mod."], err = db.Prepare(`SELECT id, type_id, mod_id, info FROM metrics_link_type_and_mod`)
		if err != nil {
			return err
		}

		/*Модификации*/
		if dbr.requestsList["Select.metrics_link_type_and_mod_or_names."], err = db.Prepare(
			`SELECT mltam.id, mltam.type_id, mt.name, mltam.mod_id, mm.name, mltam.info FROM metrics_link_type_and_mod mltam 
			left join metrics_type mt on mt.id=mltam.type_id 
			left join metrics_mod mm on mm.id=mltam.mod_id 
		ORDER BY mltam.id`); err != nil {
			return err
		} /*ORDER BY mt.name, mm.name*/
		dbr.requestsList["Delete.metrics_link_type_and_mod.Id"], err = db.Prepare(`DELETE FROM metrics_link_type_and_mod WHERE Id=$1`)
		if err != nil {
			return err
		}

		/*
				dbr.requestsList["Update.metrics_link_type_and_mod.Type_idMod_id"], err = db.Prepare(`UPDATE metrics_link_type_and_mod SET type_id=$3, mod_id=$4 WHERE type_id=$1, mod_id=$2`)
				if err != nil {
					return err
				}

				dbr.requestsList["Delete.metrics_link_type_and_mod.Type_id"], err = db.Prepare(`DELETE FROM metrics_link_type_and_mod WHERE type_id=$1`)
				if err != nil {
					return err
				}

				dbr.requestsList["Delete.metrics_link_type_and_mod.Mod_id"], err = db.Prepare(`DELETE FROM metrics_link_type_and_mod WHERE mod_id=$1`)
				if err != nil {
					return err
				}

				dbr.requestsList["Delete.metrics_link_type_and_mod.Type_idMod_id"], err = db.Prepare(`DELETE FROM metrics_link_type_and_mod WHERE type_id=$1 and mod_id=$1`)
				if err != nil {
					return err
				}

			dbr.requestsList["Select.metrics_link_type_and_mod."], err = db.Prepare(`select mm.id, mm.parent_id, mm.name FROM metrics_link_type_and_mod mltam left join metrics_mod mm on mm.id=mod_id WHERE mltam.type_id = $1`)
			if err != nil {
				return err
			}
		*/
		//	dbr.requestsList["Select.metrics_link_type_and_mod.Type_id"], err = db.Prepare(`SELECT type_id, mod_id FROM metrics_link_type_and_mod where type_id = $1`)
		//	if err != nil {
		//		return err
		//	}

		//	dbr.requestsList["Select.metrics_link_type_and_mod.Mod_id"], err = db.Prepare(`SELECT type_id, mod_id FROM metrics_link_type_and_mod where mod_id = $1`)
		//	if err != nil {
		//		return err
		//	}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_own
	{
		dbr.requestsList["Insert.metrics_own."], err = db.Prepare(`INSERT INTO metrics_own (name)  VALUES ($1)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_own.Id"], err = db.Prepare(`UPDATE metrics_own SET name=$2 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_own."], err = db.Prepare(`SELECT id, name FROM metrics_own`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_own.Id"], err = db.Prepare(`SELECT id, name FROM metrics_own WHERE id=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_service_data
	{
		dbr.requestsList["Insert.metrics_service_data."], err = db.Prepare(`INSERT INTO metrics_service_data (service_table, end_date, end_id) VALUES ($1, $2, $3)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_service_data.Id"], err = db.Prepare(`UPDATE metrics_service_data SET service_table = $2, end_date = $3, end_id = $4 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_service_data.Id-end_date"], err = db.Prepare(`UPDATE metrics_service_data SET end_date = $2 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_service_data.Id-end_id"], err = db.Prepare(`UPDATE metrics_service_data SET end_id = $2 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_service_data.Id-end_dateId-end_id"], err = db.Prepare(`UPDATE metrics_service_data SET end_date = date($2), end_id = $3 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_service_data."], err = db.Prepare(`SELECT id, service_table, end_date, end_id FROM metrics_service_data`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_service_data.Id"], err = db.Prepare(`SELECT id, service_table, end_date, end_id FROM metrics_service_data WHERE id=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_service_table
	{
		dbr.requestsList["Insert.metrics_service_table."], err = db.Prepare(`INSERT INTO metrics_service_table(Query, TableName, TypeParameter, Service_ID) VALUES ($1, $2, $3, $4)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_service_table.Id"], err = db.Prepare(`UPDATE metrics_service_table SET Query = $2, TableName = $3, TypeParameter = $4, Service_ID = $5 WHERE ID=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_service_table."], err = db.Prepare(`SELECT ID, Query, TableName, TypeParameter, Service_ID FROM metrics_service_table`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_service_table.Id"], err = db.Prepare(`SELECT ID, Query, TableName, TypeParameter, Service_ID FROM metrics_service_table WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_service_table.Service_id"], err = db.Prepare(`SELECT ID, Query, TableName, TypeParameter, Service_ID FROM metrics_service_table WHERE Service_ID=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_service
	{
		dbr.requestsList["Insert.metrics_service."], err = db.Prepare(`INSERT INTO metrics_service(name, ip) VALUES ($1, $2)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_service.Id"], err = db.Prepare(`UPDATE metrics_service SET name = $2, ip = $3 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_service."], err = db.Prepare(`SELECT id, name, ip FROM metrics_service`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_service.Id"], err = db.Prepare(`SELECT id, name, ip FROM metrics_service WHERE id=$1`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_step
	{
		if dbr.requestsList["Insert.metrics_step."], err = db.Prepare(`INSERT INTO metrics_step(name, value) VALUES ($1, $2)`); err != nil {
			return err
		}
		if dbr.requestsList["Update.metrics_step.Id"], err = db.Prepare(`UPDATE metrics_step SET name = $2, value = $3 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_step."], err = db.Prepare(`SELECT id, name, value, EXTRACT(EPOCH FROM value::INTERVAL)/60 as minuts FROM metrics_step ORDER BY minuts ASC`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_step.Id"], err = db.Prepare(`SELECT id, name, value, EXTRACT(EPOCH FROM value::INTERVAL)/60 as minuts FROM metrics_step WHERE id=$1`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_cook_time
	{
		if dbr.requestsList["Insert.metrics_cook_time."], err = db.Prepare(`INSERT INTO metrics_cook_time(userhash, price_id, date, mintime, midtime, maxtime) VALUES ($1, $2, $3, $4, $5, $6)`); err != nil {
			return err
		}
		if dbr.requestsList["Update.metrics_cook_time.Id"], err = db.Prepare(`UPDATE metrics_cook_time SET userhash=$2, price_id=$3, date=$4, mintime=$5, midtime=$6, maxtime=$7 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_cook_time."], err = db.Prepare(`SELECT id, userhash, price_id, date, mintime, midtime, maxtime FROM metrics_cook_time`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_cook_count
	{
		if dbr.requestsList["Insert.metrics_cook_count."], err = db.Prepare(`INSERT INTO metrics_cook_count(userhash, parametr_id, step_id, date, made, summade, fail, sumfail, remake, sumremake, slow, sumslow) VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0, 0, 0, 0) RETURNING id`); err != nil {
			return err
		}

		if dbr.requestsList["Update.metrics_cook_count.Id"], err = db.Prepare(`UPDATE metrics_cook_count SET userhash=$2, step_id=$3, date=$4, made=$5, summade=$6, fail=$7, sumfail=$8, remake=$9, sumremake=$10, slow=$11, sumslow=$12 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["UpdateMade.metrics_cook_count.Id"], err = db.Prepare(`UPDATE metrics_cook_count SET made=made+$2, summade=summade+$3 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["UpdateFail.metrics_cook_count.Id"], err = db.Prepare(`UPDATE metrics_cook_count SET fail=fail+$2, sumfail=sumfail+$3 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["UpdateRemake.metrics_cook_count.Id"], err = db.Prepare(`UPDATE metrics_cook_count SET remake=remake+$2, sumremake=sumremake+$3 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["UpdateSlow.metrics_cook_count.Id"], err = db.Prepare(`UPDATE metrics_cook_count SET slow=slow+$2, sumslow=sumslow+$3 WHERE id=$1`); err != nil {
			return err
		}

		/*С параметром*/
		if dbr.requestsList["Select.metrics_cook_count."], err = db.Prepare(`SELECT id, userhash, parametr_id, step_id, date, made, summade, fail, sumfail, remake, sumremake, slow, sumslow FROM metrics_cook_count`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.date|parametr|userhash|step_id(1)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and parametr_id =$2 and userhash=$3 and step_id=1`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.date|parametr|userhash|step_id(2)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and parametr_id =$2 and userhash=$3 and step_id=2`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.date|parametr|userhash|step_id(3)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where date(Date) = date($1) and parametr_id =$2 and userhash=$3 and step_id=3`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.date|parametr|userhash|step_id(4)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where EXTRACT(MONTH FROM Date) = EXTRACT(MONTH FROM $1::TIMESTAMP) and EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and parametr_id =$2 and userhash=$3 and step_id=4`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.date|parametr|userhash|step_id(5)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and parametr_id =$2 and userhash=$3 and step_id=5`); err != nil {
			return err
		}

		/*Без параметра*/
		if dbr.requestsList["SelectID.metrics_cook_count.userhash|step_id(1)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and EXTRACT(MINUTE FROM Date) = EXTRACT(MINUTE FROM $1::TIMESTAMP) and userhash=$2 and step_id=1`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.userhash|step_id(2)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where date(Date) = date($1) and EXTRACT(HOUR FROM Date) = EXTRACT(HOUR FROM $1::TIMESTAMP) and userhash=$2 and step_id=2`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.userhash|step_id(3)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where date(Date) = date($1) and userhash=$2 and step_id=3`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.userhash|step_id(4)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where EXTRACT(MONTH FROM Date) = EXTRACT(MONTH FROM $1::TIMESTAMP) and EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and userhash=$2 and step_id=4`); err != nil {
			return err
		}
		if dbr.requestsList["SelectID.metrics_cook_count.userhash|step_id(5)"], err = db.Prepare(`SELECT id FROM metrics_cook_count where EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM $1::TIMESTAMP) and userhash=$2 and step_id=5`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_cook_prices
	{
		if dbr.requestsList["Insert.metrics_cook_prices."], err = db.Prepare(`INSERT INTO metrics_cook_prices(cook_count_id, price_id, status_id) VALUES ($1, $2, $3)`); err != nil {
			return err
		}
		if dbr.requestsList["Update.metrics_cook_prices.Id"], err = db.Prepare(`UPDATE metrics_cook_prices SET cook_count_id=$2, price_id=$3, status_id=$4 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_cook_prices."], err = db.Prepare(`SELECT id, cook_count_id, price_id, status_id FROM metrics_cook_prices`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_status
	{
		if dbr.requestsList["Insert.metrics_status."], err = db.Prepare(`INSERT INTO metrics_status(name, addinfo) VALUES ($1, $2)`); err != nil {
			return err
		}
		if dbr.requestsList["Update.metrics_status.Id"], err = db.Prepare(`UPDATE metrics_status SET name=$2, addinfo=$3 WHERE id=$1`); err != nil {
			return err
		}
		if dbr.requestsList["Select.metrics_status."], err = db.Prepare(`SELECT id, name, addinfo FROM metrics_status`); err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_casher_time
	{
		dbr.requestsList["Insert.metrics_casher_time."], err = db.Prepare(`INSERT INTO metrics_casher_time(order_id, casher_id, timefororder, countchecksprinted, midtimeselectmenu, status_id, sumcountchecksprinted) VALUES ($1, $2, $3, $4, $5, $6, $7)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_casher_time.Id"], err = db.Prepare(`UPDATE metrics_casher_time SET casher_id=$2, timefororder=$3, countchecksprinted=$4, midtimeselectmenu=$5, status_id=$6, sumcountchecksprinted=$7 WHERE order_id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_casher_time."], err = db.Prepare(`SELECT order_id, casher_id, timefororder, countchecksprinted, midtimeselectmenu, status_id, sumcountchecksprinted FROM metrics_casher_time`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_operator_time
	{
		dbr.requestsList["Insert.metrics_operator_time."], err = db.Prepare(`INSERT INTO metrics_operator_time(order_id, cell_id, answer, fillingorder, timedialogue) VALUES ($1, $2, $3, $4, $5)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_operator_time.Id"], err = db.Prepare(`UPDATE metrics_operator_time SET cell_id=$2, answer=$3, fillingorder=$4, timedialogue=$5 WHERE order_id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_operator_time."], err = db.Prepare(`SELECT order_id, cell_id, answer, fillingorder, timedialogue FROM metrics_operator_time`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_operator_cells_count
	{
		dbr.requestsList["Insert.metrics_operator_cells_count."], err = db.Prepare(`INSERT INTO metrics_operator_cells_count(metric_id, madecalls, acceptedcalls, failcalls, breakcalls) VALUES ($1, $2, $3, $4, $5)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_operator_cells_count.Id"], err = db.Prepare(`UPDATE metrics_operator_cells_count SET metric_id=$2, madecalls=$3, acceptedcalls=$4, failcalls=$5, breakcalls=$6 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_operator_cells_count."], err = db.Prepare(`SELECT id, metric_id, madecalls, acceptedcalls, failcalls, breakcalls FROM metrics_operator_cells_count`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_casher_count
	{
		dbr.requestsList["Insert.metrics_casher_count."], err = db.Prepare(`INSERT INTO metrics_casher_count(userhash, parametr_id, step_id, date, madeorders, summadeorders, madeitems, failorders, sumfailorders, canceledorders, sumcanceledorders) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_casher_count.Id"], err = db.Prepare(`UPDATE metrics_casher_count SET userhash=$2, step_id=$3, date=$4, madeorders=$5, summadeorders=$6, madeitems=$7, failorders=$8, sumfailorders=$9, canceledorders=$10, sumcanceledorders=$11 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_casher_count."], err = db.Prepare(`SELECT id, userhash, parametr_id, step_id, date, madeorders, summadeorders, madeitems, failorders, sumfailorders, canceledorders, sumcanceledorders FROM metrics_casher_count`)
		if err != nil {
			return err
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_link_step_coc
	{
		dbr.requestsList["Insert.metrics_link_step_coc."], err = db.Prepare(`INSERT INTO metrics_link_step_coc(step_id) VALUES ($1)`)
		if err != nil {
			return err
		}
		dbr.requestsList["Update.metrics_link_step_coc.Id"], err = db.Prepare(`UPDATE metrics_link_step_coc SET step_id=$2 WHERE id=$1`)
		if err != nil {
			return err
		}
		dbr.requestsList["Select.metrics_link_step_coc."], err = db.Prepare(`SELECT id, step_id FROM metrics_link_step_coc`)
		if err != nil {
			return err
		}
	}
	return err
}

func (dbr *dbRequests) CloseRequests() error {

	//	dbr.rlock.Lock()
	//	defer dbr.rlock.Unlock()
	for _, request := range dbr.requestsList {
		if err := request.Close(); err != nil {
			return err
		}

	}
	return nil
}

func (dbr *dbRequests) ExecTransact(requestName string, values ...interface{}) error {

	//	dbr.rlock.RLock()
	//	defer dbr.rlock.RUnlock()
	_, ok := dbr.requestsList[requestName]
	if !ok {
		println(requestName)
		return errors.New("Missmatch request!")
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Stmt(dbr.requestsList[requestName]).Exec(values...)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (dbr *dbRequests) QueryRow(requestName string, values ...interface{}) (*sql.Row, error) {

	//	dbr.rlock.RLock()
	//	defer dbr.rlock.RUnlock()
	_, ok := dbr.requestsList[requestName]
	if !ok {
		println(requestName)
		return nil, errors.New("Missmatch request!")
	}

	row := dbr.requestsList[requestName].QueryRow(values...)

	return row, nil
}

func (dbr *dbRequests) Query(requestName string, values ...interface{}) (*sql.Rows, error) {

	//	dbr.rlock.RLock()
	//	defer dbr.rlock.RUnlock()
	_, ok := dbr.requestsList[requestName]
	if !ok {
		println(requestName)
		return nil, errors.New("Missmatch request!")
	}

	rows, err := dbr.requestsList[requestName].Query(values...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func init() {
	var err error
	GlobMapUsing = make(map[string]bool)

	db, err = sql.Open("postgres", "postgres://"+config.Config.Postgre_user+":"+config.Config.Postgre_password+"@"+config.Config.Postgre_host+"/"+config.Config.Postgre_database+"?sslmode="+config.Config.Postgre_ssl)
	if err != nil {
		log.Panic("Postgresql writer not found!:", err)
	}

	if err = db.Ping(); err != nil {
		log.Panic("Postgresql not reply!:", err)
	}

	if err = Requests.initRequests(); err != nil {
		log.Panic("Postgresql request init error:", err)
	}

	log.Println("Запросы к Postgresql инициализированы")
	println("Запросы к Postgresql инициализированы")
}
