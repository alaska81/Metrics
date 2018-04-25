package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"MetricsNew/config"

	_ "github.com/lib/pq"
)

var db *sql.DB
var Requests dbRequests

type dbRequests struct {
	requestsList map[string]*sql.Stmt
}

func (dbr *dbRequests) initRequests() error {

	dbr.requestsList = make(map[string]*sql.Stmt)
	var err error
	fmt.Println("Begin init requests")

	////////////////////////////////////////////////////////////////////////
	/////////////////////////////   METRICS   //////////////////////////////
	////////////////////////////////////////////////////////////////////////

	{
		////////////////////////
		/* ОТВЕТЫ ДЛЯ АДМИНКИ */
		////////////////////////

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxNewByInterval"], err = db.Prepare(`
			SELECT mc.cashregister, mc.action_time, mc.userhash, coalesce(mc.user_name, ''), mc.info, mc.type_payments, mc.cash, mc.date_preorder, mc.order_id <> 0 as is_orders, NOT mcs.end_time IS NULL as is_close
			FROM metrics m
			INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
			LEFT JOIN metrics_cashbox_shift mcs ON mcs.cashregister = mc.cashregister
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				mc.cashregister in (SELECT cashregister FROM metrics_cashbox WHERE action_time >= (date($2) || ' 06:00:00')::timestamp and action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
			ORDER BY mc.cashregister, mc.action_time
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportSaleNewByInterval"], err = db.Prepare(`
			SELECT moli.price_name, moli.type_id, coalesce(moli.type_name, '') as type_name, 
				SUM(CASE WHEN date(moi.creator_time) < date('2018-03-22') THEN ceil(moli.price - moli.price * moli.discount_percent / 100) ELSE moli.price_with_discount END) as price_with_discount, 
				moli.price_id, sum(moli.count) as count, sum(moli.real_foodcost) as real_foodcost, --(moli.code_consist = 2) as is_modifier,
				coalesce(moi.division, '') as division
			FROM metrics m 
			inner join metrics_orders_info moi on m.id = moi.metric_id
			inner join metrics_orders_list_info moli on moi.order_id = moli.order_id AND (moli.id_parent_item = 0 OR moli.code_consist = 2)
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= (date($2) || ' 06:00:00')::timestamp AND moi.creator_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				OR (moi.date_preorder_cook >= (date($2) || ' 06:00:00')::timestamp AND moi.date_preorder_cook < (date($3) || ' 06:00:00')::timestamp + interval '1 day'))
				AND moi.type_delivery <> 4
				AND moli.over_status_id not in (15, 16)
				AND date(moi.cancel_time) = date('0001-01-01')
			GROUP BY moli.price_name, moli.type_id, moli.type_name, moli.price_id, moi.division --, is_modifier
			ORDER BY moli.price_name
		`)
		// date(localtimestamp) < date('2018-03-01')
		// CASE WHEN date(moi.creator_time) < date('2018-03-21') THEN moli.id_parent_item = 0 ELSE TRUE END
		// ceil, round, floor
		//sum(ceil(moli.price - moli.price * moli.discount_percent / 100))
		// type_delivery = type	- тип заказа(1-"Навынос",2-"Доставка",3-"Ресторан",4-"Довоз",5;"Предзаказ"(не используется))
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportSaleNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxPrepayByInterval"], err = db.Prepare(`
			WITH s as (
				SELECT mc.order_id, (SELECT sum(cash) FROM metrics_cashbox mm WHERE mm.info LIKE '%'||mc.order_id||'%' AND date(mm.action_time) <> date(mm.date_preorder)) as sum_cash
				FROM metrics m
				INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					(mc.date_preorder >= (date($2) || ' 06:00:00')::timestamp and mc.date_preorder <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					AND date(mc.action_time) <> date(mc.date_preorder)
					AND mc.order_id <> 0
				GROUP BY mc.order_id
				ORDER BY mc.order_id
			)
			SELECT coalesce(sum(sum_cash), 0) FROM s
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxPrepayByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxPostpayByInterval"], err = db.Prepare(`
			WITH s as (
				SELECT mc.order_id, (SELECT sum(cash) FROM metrics_cashbox mm WHERE mm.info LIKE '%'||mc.order_id||'%' AND date(mm.action_time) <> date(mm.date_preorder) AND date(mm.date_preorder) <> date('0001-01-01')) as sum_cash
				FROM metrics m
				INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					mc.action_time >= (date($2) || ' 06:00:00')::timestamp and mc.action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day'
					AND date(mc.action_time) <> date(mc.date_preorder) AND date(mc.date_preorder) <> date('0001-01-01')
					AND mc.order_id <> 0
				GROUP BY mc.order_id
				ORDER BY mc.order_id
			)
			SELECT coalesce(sum(sum_cash), 0) FROM s
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxPostpayByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxReturnByInterval"], err = db.Prepare(`
			WITH o as (
				SELECT order_id
				FROM metrics m
				inner join metrics_cashbox mc on mc.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND 
					cashregister in (SELECT cashregister FROM metrics_cashbox WHERE action_time >= (date($2) || ' 06:00:00')::timestamp and action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					AND cash > 0
					AND order_id <> 0
			)
			
			SELECT coalesce(abs(sum(cash)), 0)
			FROM o
			INNER JOIN metrics_cashbox mc ON mc.info LIKE '%'||o.order_id||'%'
			WHERE cashregister in (SELECT cashregister FROM metrics_cashbox WHERE action_time >= (date($2) || ' 06:00:00')::timestamp and action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
			AND mc.cash < 0	AND mc.order_id = 0	
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxReturnByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportSummOnTypePayments"], err = db.Prepare(`
			WITH cbox as (
				SELECT mc.*, mc.order_id <> 0 as is_orders
				FROM metrics m 
				INNER JOIN metrics_cashbox mc on m.id = mc.metric_id 
					AND (mc.action_time >= (date($2) || ' 06:00:00')::timestamp and mc.action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					--AND mc.cash > 0
				WHERE (m.ownhash = $1 or $1 = 'all')
			)
			SELECT cb.type_payments, count(cb.id), coalesce(SUM(cb.cash), 0), cb.is_orders
			FROM cbox cb 

			GROUP BY cb.type_payments, cb.is_orders
			`)
		//			SELECT mc.type_payments, count(mc.id), coalesce(sum(mc.cash), 0)
		//			FROM metrics m
		//			inner join metrics_cashbox mc on m.id = mc.metric_id
		//			WHERE (m.ownhash = $1 or $1 = 'all') AND
		//				(mc.action_time >= (date($2) || ' 06:00:00')::timestamp and mc.action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
		//				AND mc.cash > 0
		//			GROUP BY mc.type_payments

		//			SELECT coalesce(sum(mc.cash), 0)
		//			FROM metrics m
		//			inner join metrics_orders_info moi on m.id = moi.metric_id
		//			inner join metrics_cashbox mc on moi.order_id = mc.order_id
		//			WHERE m.ownhash = $1 AND
		//				((date(moi.date_preorder_cook) = date('0001-01-01') AND date(moi.creator_time) >= date($2) AND date(moi.creator_time) <= date($3))
		//				OR (moi.date_preorder_cook >= $2 AND moi.date_preorder_cook < $3))
		//				AND moi.type_delivery <> 4
		//				AND mc.type_payments = $4
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportSummOnTypePayments: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxPointlByInterval"], err = db.Prepare(`
			WITH cbox as (
				SELECT m.ownhash, mc.type_payments, mc.order_id, mc.cash, mc.order_id <> 0 as is_orders
				FROM metrics m 
				INNER JOIN metrics_cashbox mc on m.id = mc.metric_id 
					AND (mc.action_time >= (date($2) || ' 06:00:00')::timestamp and mc.action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				WHERE (m.ownhash = $1 or $1 = 'all')
			)
			
			SELECT ownhash as point_hash, (SELECT own_name FROM metrics_hash_name WHERE ownhash = own_hash) as point_name, type_payments, count(order_id), coalesce(SUM(cash), 0) as summ, is_orders
			FROM cbox
			GROUP BY ownhash, type_payments, is_orders
			ORDER BY ownhash
			`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxPointlByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCouriersNewByInterval"], err = db.Prepare(`
			WITH p as (
				SELECT UNNEST(userhashes_arr) as user_arr	
				FROM metrics_plan
				WHERE 
				plan_date >= date($2) AND plan_date <= date($3) + interval '1 day' - interval '1 millisecond'
			),
			plan as (
				SELECT UNNEST(string_to_array(user_arr, ',')) as user_hash
				FROM p
			)

			SELECT coalesce((SELECT own_name FROM metrics_hash_name WHERE moi.courier_hash = own_hash), '-') as user_name, moi.courier_hash, count(moi.order_id), array_agg(moi.order_id), 
				extract(epoch from (AVG(moi.courier_end_time - moi.courier_start_time))) as avg_time, 
				COUNT(NULLIF((extract(epoch from (moi.courier_end_time - moi.courier_start_time))-1 > $4 * 60), false)) as count_overtime, 
				round(100 / (extract(epoch from(SUM(moi.courier_end_time - moi.courier_start_time))) / (count(moi.order_id) * $4 * 60))) as speed,
				(CASE WHEN (SELECT COUNT(p.user_hash) FROM plan p WHERE p.user_hash = moi.courier_hash) > 0 THEN (SELECT COUNT(p.user_hash) FROM plan p WHERE p.user_hash = moi.courier_hash) / 2 - 1 ELSE 0 END) as work_time
			FROM metrics m 
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			WHERE (m.ownhash = $1 or $1 = 'all') AND 
				(moi.courier_start_time >= (date($2) || ' 06:00:00')::timestamp and moi.courier_start_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(moi.courier_end_time) <> date('0001-01-01')
			GROUP BY user_name, moi.courier_hash
			ORDER BY count(moi.order_id) DESC
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCouriersNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCouriersAddrByInterval"], err = db.Prepare(`
			SELECT coalesce((SELECT own_name FROM metrics_hash_name WHERE moi.courier_hash = own_hash), '-') as user_name, moi.courier_hash, moi.city, moi.street, moi.house, moi.building, sum(moli.price-(moli.price*moli.discount_percent/100)) as price, extract(epoch from (moi.courier_end_time - moi.courier_start_time)) as time_delivery, extract(epoch from (moi.courier_start_time - moi.collector_time)) as time_taken
			FROM metrics m 
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			INNER JOIN metrics_orders_list_info moli on moi.order_id = moli.order_id AND moli.set = false
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				(moi.courier_start_time >= (date($2) || ' 06:00:00')::timestamp and moi.courier_start_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(moi.courier_end_time) <> date('0001-01-01')
			GROUP BY user_name, moi.courier_hash, moi.city, moi.street, moi.building, moi.house, time_delivery, time_taken
			ORDER BY user_name
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCouriersAddrByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportTimeDeliveryByInterval"], err = db.Prepare(`
			SELECT coalesce(avg(extract(epoch from (moi.courier_end_time - moi.creator_time))), 0) as time_delivery
			FROM metrics m 
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.courier_hash
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				(moi.courier_start_time >= (date($2) || ' 06:00:00')::timestamp and moi.courier_start_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(moi.date_preorder_cook) = date('0001-01-01')
				AND date(moi.courier_end_time) <> date('0001-01-01')
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportTimeDeliveryByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportOperatorsNewByInterval"], err = db.Prepare(`
			WITH p as (
				SELECT UNNEST(userhashes_arr) as user_arr	
				FROM metrics_plan
				WHERE 
				plan_date >= date($2) AND plan_date <= date($3) + interval '1 day' - interval '1 millisecond'
			),
			plan as (
				SELECT UNNEST(string_to_array(user_arr, ',')) as user_hash
				FROM p
			)

			SELECT coalesce((SELECT own_name FROM metrics_hash_name WHERE moi.creator_hash = own_hash), '-') as user_name, moi.creator_hash, count(moi.order_id),
			(CASE WHEN (SELECT COUNT(p.user_hash) FROM plan p WHERE p.user_hash = moi.creator_hash) > 0 THEN (SELECT COUNT(p.user_hash) FROM plan p WHERE p.user_hash = moi.creator_hash) / 2 - 1 ELSE 0 END) as work_time
			FROM metrics m 
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			WHERE (m.ownhash = $1 or $1 = 'all') and 
				(moi.creator_time >= (date($2) || ' 06:00:00')::timestamp and moi.creator_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND moi.division <> '*'
			GROUP BY user_name, moi.creator_hash
			ORDER BY count(moi.order_id) DESC
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportOperatorsNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCancelOrdersNewByInterval"], err = db.Prepare(`
			SELECT moi.order_id, moi.creator_time, moi.cancel_time, mhn.own_name, moi.cancel_hash, moi.cancellation_reason_id, cancellation_reason_note
			FROM metrics m
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.cancel_hash
			WHERE (m.ownhash = $1 or $1 = 'all') and
				(moi.cancel_time >= (date($2) || ' 06:00:00')::timestamp and moi.cancel_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
			ORDER BY moi.order_id, moi.creator_time, moi.cancel_time, mhn.own_name, moi.cancel_hash, moi.cancellation_reason_id, cancellation_reason_note
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCancelOrdersNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportOrdersNewByInterval"], err = db.Prepare(`
			SELECT moi.order_id, moi.creator_time, moi.cancel_time, mhn.own_name, moi.cancel_hash, moi.cancellation_reason_id, cancellation_reason_note
			FROM metrics m
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.cancel_hash
			WHERE (m.ownhash = $1 or $1 = 'all') and
				(moi.cancel_time >= (date($2) || ' 06:00:00')::timestamp and moi.cancel_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
			ORDER BY moi.order_id, moi.creator_time, moi.cancel_time, mhn.own_name, moi.cancel_hash, moi.cancellation_reason_id, cancellation_reason_note
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportOrdersNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportOrdersOnTime"], err = db.Prepare(`
			WITH timeparts as (
				SELECT hours
				FROM generate_series (date($2)::timestamp, date($3)::timestamp + interval '1 day' - interval '1 millisecond', interval '1 hour') as dh(hours)
			),
			orders as (
				SELECT *, (case when date(moi.date_preorder_cook) = date('0001-01-01') then moi.creator_time else moi.date_preorder_cook end) as order_time
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
				--LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.cancel_hash
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp AND moi.creator_time <= date($3)::timestamp + interval '1 day')
				OR (moi.date_preorder_cook >= date($2)::timestamp AND moi.date_preorder_cook <= date($3)::timestamp + interval '1 day'))
			)
			
			SELECT tp.hours::date as dates, tp.hours::time as times, COUNT(o.order_id), COUNT(NULLIF(date(o.date_preorder_cook) = date('0001-01-01'), TRUE)) as preorders, COUNT(NULLIF(o.type = 1, FALSE)) as delivery, COUNT(NULLIF(o.type = 2, FALSE)) as takeout, COUNT(NULLIF(o.compensation = TRUE, FALSE)) as compensation, SUM(CASE WHEN NULLIF(o.compensation = TRUE, FALSE) THEN o.price_with_discount ELSE 0 END) as sum_cashback, 
			coalesce(SUM(o.price_with_discount), 0) as sum_orders, coalesce(AVG(o.price_with_discount), 0) as avg_orders,
			COUNT(NULLIF(date(o.cancel_time) <> date('0001-01-01'), FALSE)) as cancel, SUM(CASE WHEN NULLIF(date(o.cancel_time) <> date('0001-01-01'), FALSE) THEN o.price_with_discount ELSE 0 END) as sum_cancel

			FROM timeparts tp
			LEFT JOIN orders o ON (o.order_time::date = tp.hours::date AND o.order_time::time >= tp.hours::time AND o.order_time::time < tp.hours::time + interval '1 hour')
			GROUP BY dates, times
			ORDER BY dates, times
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportOrdersOnTime: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportOrdersOnDay"], err = db.Prepare(`
			WITH timeparts as (
				SELECT days
				FROM generate_series (date($2)::timestamp, date($3)::timestamp + interval '1 day' - interval '1 millisecond', interval '1 day') as dh(days)
			),
			orders as (
				SELECT *, (case when date(moi.date_preorder_cook) = date('0001-01-01') then moi.creator_time else moi.date_preorder_cook end) as order_time
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
				--LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.cancel_hash
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp AND moi.creator_time <= date($3)::timestamp + interval '1 day')
				OR (moi.date_preorder_cook >= date($2)::timestamp AND moi.date_preorder_cook <= date($3)::timestamp + interval '1 day'))
			)
			
			SELECT tp.days::date as dates, tp.days::time as times, COUNT(o.order_id), COUNT(NULLIF(date(o.date_preorder_cook) = date('0001-01-01'), TRUE)) as preorders, COUNT(NULLIF(o.type = 1, FALSE)) as delivery, COUNT(NULLIF(o.type = 2, FALSE)) as takeout, COUNT(NULLIF(o.compensation = TRUE, FALSE)) as compensation, SUM(CASE WHEN NULLIF(o.compensation = TRUE, FALSE) THEN o.price_with_discount ELSE 0 END) as sum_cashback, 
			coalesce(SUM(o.price_with_discount), 0) as sum_orders, coalesce(AVG(o.price_with_discount), 0) as avg_orders,
			COUNT(NULLIF(date(o.cancel_time) <> date('0001-01-01'), FALSE)) as cancel, SUM(CASE WHEN NULLIF(date(o.cancel_time) <> date('0001-01-01'), FALSE) THEN o.price_with_discount ELSE 0 END) as sum_cancel

			FROM timeparts tp
			LEFT JOIN orders o ON o.order_time::date = tp.days::date
			GROUP BY dates, times
			ORDER BY dates, times
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportOrdersOnDay: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportOrdersByInterval"], err = db.Prepare(`
			WITH orders as (
				SELECT moi.order_id, (NULLIF(date(moi.date_preorder_cook) = date('0001-01-01'), TRUE)) as preorders, (NULLIF(moi.type_delivery = 1, FALSE)) as delivery, (NULLIF(moi.type_delivery = 2, FALSE)) as takeout, (NULLIF(moi.compensation = TRUE, FALSE)) as count_compensation
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp AND moi.creator_time <= date($3)::timestamp + interval '1 day')
				OR (moi.date_preorder_cook >= date($2)::timestamp AND moi.date_preorder_cook <= date($3)::timestamp + interval '1 day'))
				GROUP BY moi.order_id, moi.date_preorder_cook, moi.type_delivery, moi.compensation
			)
			
			SELECT COUNT(order_id) as count_orders, COUNT(preorders) as count_preorders, COUNT(delivery) as count_delivery, COUNT(takeout) as count_takeout, COUNT(count_compensation) as count_compensation
			FROM orders
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportOrdersByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportPredictCouriersOnTime"], err = db.Prepare(`
			WITH timeparts as (
				SELECT hours
				FROM generate_series (date($2)::timestamp, date($3)::timestamp + interval '1 day' - interval '1 millisecond', interval '1 hour') as dh(hours)
			),
			orders as (
				SELECT *
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
				WHERE (m.ownhash = $1 or $1 = 'all') AND
				(
					   ((moi.collector_time >= date($2)::timestamp - interval '7 day' AND moi.collector_time <= date($3)::timestamp - interval '6 day'))
					OR ((moi.collector_time >= date($2)::timestamp - interval '14 day' AND moi.collector_time <= date($3)::timestamp - interval '13 day'))
					OR ((moi.collector_time >= date($2)::timestamp - interval '28 day' AND moi.collector_time <= date($3)::timestamp - interval '27 day'))
				)
				AND moi.type_delivery = 2
			)
			
			SELECT tp.hours::date as dates, tp.hours::time as times, ceil(COUNT(o.order_id)/3::float)
			FROM timeparts tp
			LEFT JOIN orders o ON ((o.collector_time::date = tp.hours::date - interval '7 day' OR o.collector_time::date = tp.hours::date - interval '14 day' OR o.collector_time::date = tp.hours::date - interval '28 day') AND o.collector_time::time >= tp.hours::time AND o.collector_time::time < tp.hours::time + interval '1 hour')
			GROUP BY dates, times
			ORDER BY dates, times
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportPredictCouriersOnTime: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportPredictCollectorOnTime"], err = db.Prepare(`
			WITH timeparts as (
				SELECT hours
				FROM generate_series (date($2)::timestamp, date($3)::timestamp + interval '1 day' - interval '1 millisecond', interval '1 hour') as dh(hours)
			),
			orders as (
				SELECT *
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
				WHERE (m.ownhash = $1 or $1 = 'all') AND
				(
					((moi.collector_time >= date($2)::timestamp - interval '7 day' AND moi.collector_time <= date($3)::timestamp - interval '6 day'))
				 OR ((moi.collector_time >= date($2)::timestamp - interval '14 day' AND moi.collector_time <= date($3)::timestamp - interval '13 day'))
				 OR ((moi.collector_time >= date($2)::timestamp - interval '28 day' AND moi.collector_time <= date($3)::timestamp - interval '27 day'))
			 )
			)
			
			SELECT tp.hours::date as dates, tp.hours::time as times, ceil(COUNT(o.order_id)/3::float)
			FROM timeparts tp
			LEFT JOIN orders o ON ((o.collector_time::date = tp.hours::date - interval '7 day' OR o.collector_time::date = tp.hours::date - interval '14 day' OR o.collector_time::date = tp.hours::date - interval '28 day') AND o.collector_time::time >= tp.hours::time AND o.collector_time::time < tp.hours::time + interval '1 hour')
			GROUP BY dates, times
			ORDER BY dates, times
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportPredictCollectorOnTime: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportAvgTimeRelayOnTime"], err = db.Prepare(`
			WITH timeparts as (
				SELECT hours
				FROM generate_series (date($2)::timestamp, date($3)::timestamp + interval '1 day' - interval '1 millisecond', interval '1 hour') as dh(hours)
			),
			orders as (
				--SELECT *, (case when date(moi.date_preorder_cook) = date('0001-01-01') then moi.creator_time else moi.date_preorder_cook end) as order_time
				SELECT *, moi.collector_time as order_time
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
				WHERE (m.ownhash = $1 or $1 = 'all') AND
				--(
				--	((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp AND moi.creator_time <= date($3)::timestamp + interval '1 day')
				--	OR (moi.date_preorder_cook >= date($2)::timestamp AND moi.date_preorder_cook <= date($3)::timestamp + interval '1 day'))
				--)
					(moi.collector_time >= date($2)::timestamp AND moi.collector_time < date($3)::timestamp + interval '1 day')
			)
			
			SELECT tp.hours::date as dates, tp.hours::time as times, 
				coalesce(AVG(extract(epoch from (case when date(o.courier_start_time) <> date('0001-01-01') then o.courier_end_time - o.courier_start_time end))), 0) as avg_time_courier, 
				coalesce(AVG(extract(epoch from (case when date(o.courier_start_time) <> date('0001-01-01') then o.courier_end_time - o.collector_time end))), 0) as avg_time_transfer_customer,
				coalesce(AVG(extract(epoch from (case when date(o.courier_start_time) <> date('0001-01-01') then o.courier_start_time - o.collector_time end))), 0) as avg_time_transfer_courier,
				coalesce(AVG(extract(epoch from (case when date(o.courier_start_time) = date('0001-01-01') then o.courier_end_time - o.collector_time end))), 0) as avg_time_transfer_outer
			FROM timeparts tp
			LEFT JOIN orders o ON (o.order_time::date = tp.hours::date AND o.order_time::time >= tp.hours::time AND o.order_time::time < tp.hours::time + interval '1 hour' AND date(o.courier_end_time) <> date('0001-01-01'))
			GROUP BY dates, times
			ORDER BY dates, times
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportAvgTimeRelayOnTime: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportWorkloadOnTime"], err = db.Prepare(`
			WITH orders as (
				SELECT *, (case when moli.cooking_tracker = 1 then 1 else 2 end) as cooking_type
				FROM metrics m
				INNER JOIN metrics_orders_list_info moli ON moli.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
				(moli.start_time >= date($2)::timestamp AND moli.start_time <= date($3)::timestamp + interval '1 day')
				AND moli.cooking_tracker <> 0
				AND moli.set = false
			),
			mplan as (
				SELECT point_hash, role_hash, plan_date, count_cook, ((row_number() OVER (PARTITION by id) - 1) * interval '30 minute') AS timeparts, 
					(case when role_hash in ('8746fffb4f2e033aabefa8103e7e4f4d183f0098f1e6513a718c0dcff60be6c2048faaefc6477973c321c8f7c52c96d078c99b188ac2a11a221fb97fa957ccd3') then 1 else 
						(case when role_hash in ('b6b8c237446b537594a2e1fc44d1d522b0ac62ef3e157e940eb39db9c45deefe151ee05a292e8366127c26901efca3882670d1c53ba11c1169c3c53a71b686c2') then 2 else 3 end) end) as cooking_type
				FROM (SELECT id, point_hash, role_hash, plan_date, unnest(counts_arr) AS count_cook FROM metrics_plan WHERE plan_date >= date($2) AND plan_date <= date($3) + interval '1 day' - interval '1 millisecond') t
				WHERE (point_hash = $1 or $1 = 'all') --AND
					--role_hash in ('8746fffb4f2e033aabefa8103e7e4f4d183f0098f1e6513a718c0dcff60be6c2048faaefc6477973c321c8f7c52c96d078c99b188ac2a11a221fb97fa957ccd3','b6b8c237446b537594a2e1fc44d1d522b0ac62ef3e157e940eb39db9c45deefe151ee05a292e8366127c26901efca3882670d1c53ba11c1169c3c53a71b686c2')
				ORDER BY plan_date, timeparts
			)
			
			SELECT mp.cooking_type, mp.plan_date::date as dates, mp.timeparts::time as times, mp.point_hash, coalesce(mhn_point.own_name,'-') as point_name, sum(mp.count_cook) as count_cook,
				--(SELECT COUNT(o.id_item) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) as count_items,
				--array(SELECT (o.time_cook) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) as aa,
				(case when sum(mp.count_cook) > 0 then 
					ceil((SELECT coalesce(SUM(o.time_cook::numeric), 0) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) / (sum(mp.count_cook) * 30 * 60) * 100) 
					else ceil((SELECT coalesce(SUM(o.time_cook::numeric), 0) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) / 1800 * 100) 
				end) as workload
			FROM mplan mp
			--LEFT JOIN metrics_hash_name mhn_role ON mhn_role.own_hash = mp.role_hash
			LEFT JOIN metrics_hash_name mhn_point ON mhn_point.own_hash = mp.point_hash
			WHERE mp.cooking_type in (1, 2)
			GROUP BY mp.point_hash, mhn_point.own_name, dates, times, mp.plan_date, mp.timeparts, mp.cooking_type
			ORDER BY mp.cooking_type, dates, times, point_hash
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportWorkloadOnTime: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCookByInterval"], err = db.Prepare(`
			WITH p as (
				SELECT UNNEST(userhashes_arr) as user_arr	
				FROM metrics_plan
				WHERE (point_hash = $1 or $1 = 'all') AND
					plan_date >= date($2) AND plan_date <= date($3) + interval '1 day' - interval '1 millisecond'
			),
			plan as (
				SELECT UNNEST(string_to_array(user_arr, ',')) as user_hash
				FROM p
			)

			SELECT coalesce((SELECT own_name FROM metrics_hash_name WHERE moli.cook_hash = own_hash), '-') as cook_name, (SELECT own_name FROM metrics_hash_name WHERE moli.cook_role = own_hash) as cook_role, count(moli.id) as count_orders, 
				extract(epoch from (AVG(moli.end_time - moli.start_time))) as avg_time,
				COUNT(NULLIF((extract(epoch from (moli.end_time - moli.start_time))-1 > (case when cooking_tracker = 1 then time_cook else time_cook + time_fry end)), false)) as count_overtime, 
				(case when cooking_tracker = 1 then 1 else 2 end) as tracker, 
				round(100 / (extract(epoch from(SUM(moli.end_time - moli.start_time))) / SUM(case when cooking_tracker = 1 then time_cook else time_cook + time_fry end))) as speed,
				extract(epoch from (SUM(moli.end_time - moli.start_time))) as sum_time,
				(CASE WHEN (SELECT COUNT(p.user_hash) FROM plan p WHERE p.user_hash = moli.cook_hash) > 0 THEN (SELECT COUNT(p.user_hash) FROM plan p WHERE p.user_hash = moli.cook_hash) / 2 - 1 ELSE 0 END) as work_time
			FROM metrics m 
			INNER JOIN metrics_orders_list_info moli ON moli.metric_id = m.id
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				(moli.start_time >= (date($2) || ' 06:00:00')::timestamp and moli.start_time < (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(moli.end_time) <> date('0001-01-01')
				AND set = false
			GROUP BY tracker, cook_role, cook_name, moli.cook_hash
			ORDER BY cook_role, cook_name, tracker		
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCookByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportLastUpdateBySTId"], err = db.Prepare(`
			SELECT MIN(update_at)
			FROM metrics_parameters
			WHERE service_table_id = ANY((string_to_array($1, ',')::int[]))
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportLastUpdateBySTId: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportPersonalByInterval"], err = db.Prepare(`
			WITH p as (
				SELECT id, point_hash, role_hash, plan_date, persons[rn] AS persons, (rn - 1) * interval '30 minute' AS timeparts
				FROM  (
					SELECT id, point_hash, role_hash, plan_date, (userhashes_arr) as persons, generate_subscripts(userhashes_arr, 1) AS rn
					FROM metrics_plan 
					WHERE (point_hash = $1 or $1 = 'all') AND plan_date >= date($2) AND plan_date <= date($3) + interval '1 day' - interval '1 millisecond'
				) x
			),
			tp as (
				SELECT point_hash, role_hash, plan_date, unnest(string_to_array(persons, ',')) as user_hash, timeparts--, ((row_number() OVER (PARTITION by id) - 1) * interval '30 minute') AS timeparts
				FROM p
				ORDER BY plan_date, timeparts
			),
			tparr as (
				SELECT point_hash, role_hash, plan_date, user_hash, array_agg(timeparts) as timeparts_arr
				FROM tp
				GROUP BY point_hash, role_hash, plan_date, user_hash
			)

			SELECT plan_date, point_hash, (SELECT own_name FROM metrics_hash_name WHERE point_hash = own_hash) as point_name, role_hash, (SELECT own_name FROM metrics_hash_name WHERE role_hash = own_hash) as role_name, user_hash, coalesce((SELECT own_name FROM metrics_hash_name WHERE user_hash = own_hash), '-') as user_name, 
				array(SELECT (CASE WHEN minutes::time = ANY(timeparts_arr) THEN minutes::time::character varying ELSE '-' END) FROM generate_series ('0001-01-01 00:00:00'::timestamp, ('0001-01-01 00:00:00'::timestamp + interval '1 day' - interval '1 millisecond'), interval '30 minutes') as dh(minutes)),
				(case when role_hash in ('8746fffb4f2e033aabefa8103e7e4f4d183f0098f1e6513a718c0dcff60be6c2048faaefc6477973c321c8f7c52c96d078c99b188ac2a11a221fb97fa957ccd3', '954158042d3677233f3851d72c0d8c8574e63468d1b643c171a7348afa47b752ca0c6dff014ec8d535a2899413160ab0ce1d64a3740244fe7d57aff7f31a7b5d') then 1 else 
				(case when role_hash in ('b6b8c237446b537594a2e1fc44d1d522b0ac62ef3e157e940eb39db9c45deefe151ee05a292e8366127c26901efca3882670d1c53ba11c1169c3c53a71b686c2', 'b55ed851e362ea01dbce0664c8c362119bfc42e8293824db0618c0327486476bc152ede5d84a15907f8793b9b36d27aa848e844e993b588a3ab61f2b30f8be7f') then 2 else 3 end) end) as cooking_type
			FROM tparr
			GROUP BY cooking_type, point_hash, role_hash, plan_date, user_hash, timeparts_arr
			ORDER BY plan_date, point_hash, cooking_type, role_name, user_name
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportPersonalByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.orders.ReportCookByHashDateNum"], err = db.Prepare(`
			WITH ol as (
				SELECT *
				FROM metrics_orders_list_info
				WHERE cook_hash = $1
					AND date(start_time) >= date($2)
					AND cooking_tracker = 1 AND set = false 
				ORDER BY start_time DESC
			)

			SELECT count(id) as count_elements, 
				--(SELECT round(100 / (extract(epoch from(SUM(ids.end_time - ids.start_time))) / SUM(case when cooking_tracker = 1 then time_cook else time_cook + time_fry end))) FROM (SELECT * FROM ol LIMIT 100) as ids) as speed
				(SELECT round(100 / (extract(epoch from(SUM(ids.end_time - ids.start_time))) / SUM(time_cook))) FROM (SELECT * FROM ol LIMIT $3) as ids) as speed
			FROM ol oll
			GROUP BY oll.cook_hash
		`)
		if err != nil {
			return fmt.Errorf("Select.orders.ReportCookByHashDateNum: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportLaborCostOnCook"], err = db.Prepare(`
			WITH p as (
				SELECT point_hash, role_hash, plan_date, UNNEST(userhashes_arr) as user_arr	
				FROM metrics_plan
				WHERE (point_hash = $1 or $1 = 'all') AND
					plan_date >= date($2) AND plan_date < date($3) + interval '1 day' 
			),
			plan as (
				SELECT point_hash, role_hash, plan_date, UNNEST(string_to_array(user_arr, ',')) as user_hash
				FROM p
				WHERE role_hash IN ('8746fffb4f2e033aabefa8103e7e4f4d183f0098f1e6513a718c0dcff60be6c2048faaefc6477973c321c8f7c52c96d078c99b188ac2a11a221fb97fa957ccd3','954158042d3677233f3851d72c0d8c8574e63468d1b643c171a7348afa47b752ca0c6dff014ec8d535a2899413160ab0ce1d64a3740244fe7d57aff7f31a7b5d','b6b8c237446b537594a2e1fc44d1d522b0ac62ef3e157e940eb39db9c45deefe151ee05a292e8366127c26901efca3882670d1c53ba11c1169c3c53a71b686c2', 'b55ed851e362ea01dbce0664c8c362119bfc42e8293824db0618c0327486476bc152ede5d84a15907f8793b9b36d27aa848e844e993b588a3ab61f2b30f8be7f')
			),
			orders_list as (
				SELECT *
				FROM metrics m 
				INNER JOIN metrics_orders_list_info moli ON moli.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					(moli.start_time >= (date($2) || ' 06:00:00')::timestamp and moli.start_time < (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					AND date(moli.end_time) <> date('0001-01-01')
					AND set = false
			),
			revenue as (
				SELECT SUM(cash) as sum_cash
				FROM metrics m
				INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					(action_time >= (date($2) || ' 06:00:00')::timestamp AND action_time < (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					AND order_id > 0 AND type_payments < 3
			)

			SELECT p.point_hash, (SELECT own_name FROM metrics_hash_name WHERE p.point_hash = own_hash) as point_name, p.role_hash, (SELECT own_name FROM metrics_hash_name WHERE p.role_hash = own_hash) as role_name, p.user_hash, coalesce((SELECT own_name FROM metrics_hash_name WHERE p.user_hash = own_hash), '-') as user_name, --p.plan_date,
				COUNT(p.user_hash) / 2 - 1 as work_time,
				coalesce((SELECT hour_rate FROM metrics_users muu WHERE p.user_hash = muu.user_hash AND p.role_hash = muu.role_hash ORDER BY muu.update_time DESC LIMIT 1), 0) as hour_rate,
				(SELECT COUNT(orders_list.cook_hash) FROM orders_list WHERE p.user_hash = orders_list.cook_hash) count_list,
				coalesce((SELECT count_rate FROM metrics_users muu WHERE p.user_hash = muu.user_hash AND p.role_hash = muu.role_hash ORDER BY muu.update_time DESC LIMIT 1), 0) as count_rate,
				(SELECT sum_cash FROM revenue) as revenue
			FROM plan p
			GROUP BY p.point_hash, p.role_hash, p.user_hash
			ORDER BY point_name, role_name, user_name
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportLaborCostOnCook: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportLaborCostOnCollector"], err = db.Prepare(`
			WITH p as (
				SELECT point_hash, role_hash, plan_date, UNNEST(userhashes_arr) as user_arr	
				FROM metrics_plan
				WHERE (point_hash = $1 or $1 = 'all') AND
					plan_date >= date($2) AND plan_date < date($3) + interval '1 day' 
			),
			plan as (
				SELECT point_hash, role_hash, plan_date, UNNEST(string_to_array(user_arr, ',')) as user_hash
				FROM p
				WHERE role_hash IN ('dcfb7d4d43418b73fba6be0d51ce988e1a84dacda379e3ba3e1f3bef932d4c92c074009d331af45875dabc4fcf6e161925b93d1e67336f13540dfe4af063b556', 'c779bc4d156d8df8d97b1eee86c825b01d6eb7bcb480cb07e15bf516f1d9fc32d815cdc2084e02715a16a15a7197fd6a929cd3a4de2046fd64bdaa6586ac5ed2')
			),
			orders as (
				SELECT *
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp AND moi.creator_time <= date($3)::timestamp + interval '1 day')
					OR (moi.date_preorder_cook >= date($2)::timestamp AND moi.date_preorder_cook <= date($3)::timestamp + interval '1 day'))
			),
			revenue as (
				SELECT SUM(cash) as sum_cash
				FROM metrics m
				INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					(action_time >= (date($2) || ' 06:00:00')::timestamp AND action_time < (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					AND order_id > 0 AND type_payments < 3
			)

			SELECT p.point_hash, (SELECT own_name FROM metrics_hash_name WHERE p.point_hash = own_hash) as point_name, p.role_hash, (SELECT own_name FROM metrics_hash_name WHERE p.role_hash = own_hash) as role_name, p.user_hash, coalesce((SELECT own_name FROM metrics_hash_name WHERE p.user_hash = own_hash), '-') as user_name,
				COUNT(p.user_hash) / 2 - 1 as work_time, 
				coalesce((SELECT hour_rate FROM metrics_users muu WHERE p.user_hash = muu.user_hash AND p.role_hash = muu.role_hash ORDER BY muu.update_time DESC LIMIT 1), 0) as hour_rate,
				(SELECT COUNT(orders.collector_hash) FROM orders WHERE p.user_hash = orders.collector_hash) as count_orders,
				coalesce((SELECT count_rate FROM metrics_users muu WHERE p.user_hash = muu.user_hash AND p.role_hash = muu.role_hash ORDER BY muu.update_time DESC LIMIT 1), 0) as count_rate,
				(SELECT sum_cash FROM revenue) as revenue
			FROM plan p
			GROUP BY p.point_hash, p.role_hash, p.user_hash
			ORDER BY point_name, role_name, user_name
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportLaborCostOnCollector: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportBonusesOnDay"], err = db.Prepare(`
			WITH timeparts as (
				SELECT days
				FROM generate_series (date($2)::timestamp, date($3)::timestamp + interval '1 day' - interval '1 millisecond', interval '1 day') as dh(days)
			),
			tb as (
				SELECT type_bonus
				FROM generate_series(1, 5, 1) as type_bonus
			),
			bonus as (
				SELECT *
				FROM metrics m 
				INNER JOIN metrics_bonuses mb ON mb.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					mb.action_time >= date($2)::timestamp AND mb.action_time <= date($3)::timestamp + interval '1 day'
					AND type_bonus > 0
			),
			cashbox as (
				SELECT *
				FROM metrics m
				INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					(action_time >= (date($2) || ' 06:00:00')::timestamp AND action_time < (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					AND order_id > 0
			)
			
			SELECT tp.days::date as dates, tp.days::time as times, tb.type_bonus, coalesce(SUM(transaction_bonus), 0) as sum_bonus, COUNT(bonus_id) as count_bonus, COUNT(DISTINCT phone) as count_bonus,
			coalesce((SELECT SUM(cash) FROM cashbox WHERE action_time::date = tp.days::date), 0) as sum_price
			FROM timeparts tp
			INNER JOIN tb ON type_bonus IN (SELECT type_bonus FROM tb)
			LEFT JOIN bonus ON action_time::date = tp.days::date AND tb.type_bonus = bonus.type_bonus
			GROUP BY dates, times, tb.type_bonus, tp.days
			ORDER BY dates, times
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportBonusesOnDay: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportBonusesOnTime"], err = db.Prepare(`
			WITH timeparts as (
				SELECT hours
				FROM generate_series (date($2)::timestamp, date($3)::timestamp + interval '1 day' - interval '1 millisecond', interval '1 hour') as dh(hours)
			),
			tb as (
				SELECT type_bonus
				FROM generate_series(1, 5, 1) as type_bonus
			),
			bonus as (
				SELECT *
				FROM metrics m 
				INNER JOIN metrics_bonuses mb ON mb.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					mb.action_time >= date($2)::timestamp AND mb.action_time <= date($3)::timestamp + interval '1 day'
					AND type_bonus > 0
			),
			cashbox as (
				SELECT *
				FROM metrics m
				INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
				WHERE (m.ownhash = $1 or $1 = 'all') AND
					(action_time >= (date($2) || ' 06:00:00')::timestamp AND action_time < (date($3) || ' 06:00:00')::timestamp + interval '1 day')
					AND order_id > 0
			)
			
			SELECT tp.hours::date as dates, tp.hours::time as times, tb.type_bonus, coalesce(SUM(transaction_bonus), 0) as sum_bonus, COUNT(bonus_id) as count_bonus, COUNT(DISTINCT phone) as count_bonus,
			coalesce((SELECT SUM(cash) FROM cashbox WHERE action_time::date = tp.hours::date AND action_time::time >= tp.hours::time AND action_time::time < tp.hours::time + interval '1 hour'), 0) as sum_price
			FROM timeparts tp
			INNER JOIN tb ON type_bonus IN (SELECT type_bonus FROM tb)
			LEFT JOIN bonus ON (action_time::date = tp.hours::date AND action_time::time >= tp.hours::time AND action_time::time < tp.hours::time + interval '1 hour' AND tb.type_bonus = bonus.type_bonus)
			GROUP BY dates, times, tb.type_bonus, tp.hours
			ORDER BY dates, times
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportBonusesOnTime: %v", err)
		}

		///////////////////////
		/* Конец ДЛЯ АДМИНКИ */
		///////////////////////

		dbr.requestsList["Insert.metrics."], err = db.Prepare(`INSERT INTO metrics(OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID) VALUES ($1, $2, date($3), $4, $5, $6) RETURNING ID`)
		if err != nil {
			return fmt.Errorf("Insert.metrics.: %v", err)
		}
		//		dbr.requestsList["Update.metrics.Id"], err = db.Prepare(`UPDATE metrics SET OwnHash=$2, OwnName=$3, Date=$4, Value=$5, Step_ID=$6, Parameter_ID=$7 WHERE ID=$1`)
		//		if err != nil {
		//			return err
		//		}
		//		dbr.requestsList["Select.metrics.Id"], err = db.Prepare(`SELECT ID, OwnHash, OwnName, Date, Value, Step_ID, Parameter_ID FROM metrics WHERE ID=$1`)
		//		if err != nil {
		//			return fmt.Errorf("Select.metrics.Id: %v", err)
		//		}

		//SelectID
		if dbr.requestsList["SelectID.metrics."], err = db.Prepare(`SELECT ID FROM metrics WHERE date(Date)::date = date($1)::date and Parameter_ID=$2 and OwnHash=$3`); err != nil {
			return fmt.Errorf("SelectID.metrics.: %v", err)
		}

	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_cashbox
	{
		if dbr.requestsList["Select.metrics_cashbox.Order_Id"], err = db.Prepare(
			`SELECT id FROM metrics_cashbox WHERE order_id=$1 AND action_time=$2`); err != nil {
			return fmt.Errorf("Select.metrics_cashbox.Order_Id.: %v", err)
		}
		// if dbr.requestsList["Select.metrics_cashbox."], err = db.Prepare(
		// 	`SELECT id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder
		// 		FROM metrics_cashbox`); err != nil {
		// 	return fmt.Errorf("Select.metrics_cashbox.: %v", err)
		// }
		// if dbr.requestsList["Select.metrics_cashbox.Metric_id"], err = db.Prepare(
		// 	`SELECT id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder
		// 		FROM metrics_cashbox WHERE metric_id=$1`); err != nil {
		// 	return fmt.Errorf("Select.metrics_cashbox.Metric_id.: %v", err)
		// }
		if dbr.requestsList["Insert.metrics_cashbox."], err = db.Prepare(
			`INSERT INTO metrics_cashbox(metric_id, order_id, cashregister, action_time, userhash, user_name, info, type_payments, cash, date_preorder) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`); err != nil {
			return fmt.Errorf("Insert.metrics_cashbox.: %v", err)
		}
		if dbr.requestsList["Update.metrics_cashbox."], err = db.Prepare(
			`UPDATE metrics_cashbox SET metric_id=$3, cashregister=$4, userhash=$5, user_name=$6, info=$7, type_payments=$8, cash=$9, date_preorder=$10
			WHERE order_id=$1 AND action_time=$2`); err != nil {
			return fmt.Errorf("Update.metrics_cashbox.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_orders_info
	{
		if dbr.requestsList["Select.metrics_orders_info.Order_id"], err = db.Prepare(
			`SELECT id FROM metrics_orders_info WHERE order_id = $1`); err != nil {
			return fmt.Errorf("Select.metrics_orders_info.Order_id: %v", err)
		}
		if dbr.requestsList["Insert.metrics_orders_info."], err = db.Prepare(
			`INSERT INTO metrics_orders_info(metric_id, order_id, chain_hash, org_hash, point_hash, id_day_point, cashregister_id, count_elements, date_preorder_cook, side_order, type_delivery, type_payments, price, bonus, discount_id, discount_name, discount_percent, city, street, house, building, creator_hash, creator_role_hash, creator_time, duration_of_create, duration_of_select_element, cook_start_time, cook_end_time, collector_hash, collector_time, courier_hash, courier_start_time, courier_end_time, cancel_hash, cancel_time, cancellation_reason_id, cancellation_reason_note, crash_user_hash, crash_user_role_hash, compensation, type_compensation, type, customer_phone, price_with_discount, division)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45)`); err != nil {
			return fmt.Errorf("Insert.metrics_orders_info.: %v", err)
		}
		if dbr.requestsList["Update.metrics_orders_info."], err = db.Prepare(
			`UPDATE metrics_orders_info SET metric_id=$2, chain_hash=$3, org_hash=$4, point_hash=$5, id_day_point=$6, cashregister_id=$7, count_elements=$8, date_preorder_cook=$9, side_order=$10, type_delivery=$11, type_payments=$12, price=$13, bonus=$14, discount_id=$15, discount_name=$16, discount_percent=$17, city=$18, street=$19, house=$20, building=$21, creator_hash=$22, creator_role_hash=$23, creator_time=$24, duration_of_create=$25, duration_of_select_element=$26, cook_start_time=$27, cook_end_time=$28, collector_hash=$29, collector_time=$30, courier_hash=$31, courier_start_time=$32, courier_end_time=$33, cancel_hash=$34, cancel_time=$35, cancellation_reason_id=$36, cancellation_reason_note=$37, crash_user_hash=$38, crash_user_role_hash=$39, compensation=$40, type_compensation=$41, type=$42, customer_phone=$43, price_with_discount=$44, division=$45
			WHERE order_id=$1`); err != nil {
			return fmt.Errorf("Update.metrics_orders_info.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_orders_list_info
	{
		if dbr.requestsList["Select.metrics_orders_list_info.IdItem_OrderId"], err = db.Prepare(
			`SELECT id FROM metrics_orders_list_info 
			WHERE id_item = $1 and order_id = $2`); err != nil {
			return fmt.Errorf("Select.metrics_orders_list_info.IdItem_OrderId: %v", err)
		}
		if dbr.requestsList["Insert.metrics_orders_list_info."], err = db.Prepare(
			`INSERT INTO metrics_orders_list_info(metric_id, order_id, id_item, id_parent_item, price_id, price_name, type_id, cooking_tracker, discount_id, discount_name, discount_percent, price, cook_hash, start_time, end_time, fail_id, fail_user_hash, fail_comments, real_foodcost, count, type_name, over_status_id, time_cook, time_fry, set, cook_role, code_consist, price_with_discount)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)`); err != nil {
			return fmt.Errorf("Insert.metrics_orders_list_info.: %v", err)
		}
		if dbr.requestsList["Update.metrics_orders_list_info."], err = db.Prepare(
			`UPDATE metrics_orders_list_info SET metric_id=$3, id_parent_item=$4, price_id=$5, price_name=$6, type_id=$7, cooking_tracker=$8, discount_id=$9, discount_name=$10, discount_percent=$11, price=$12, cook_hash=$13, start_time=$14, end_time=$15, fail_id=$16, fail_user_hash=$17, fail_comments=$18, real_foodcost=$19, count=$20, type_name=$21, over_status_id=$22, time_cook=$23, time_fry=$24, set=$25, cook_role=$26, code_consist=$27, price_with_discount=$28 
			WHERE id_item=$1 AND order_id=$2`); err != nil {
			return fmt.Errorf("Update.metrics_orders_list_info.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_hash_name
	{
		if dbr.requestsList["Select.metrics_hash_name.Hash"], err = db.Prepare(
			`SELECT id FROM metrics_hash_name WHERE own_hash = $1`); err != nil {
			return fmt.Errorf("Select.metrics_hash_name.Hash: %v", err)
		}
		if dbr.requestsList["Insert.metrics_hash_name."], err = db.Prepare(
			`INSERT INTO metrics_hash_name(metric_id, own_hash, own_name, created_time)
			VALUES ($1, $2, $3, $4)`); err != nil {
			return fmt.Errorf("Insert.metrics_hash_name.: %v", err)
		}
		if dbr.requestsList["Update.metrics_hash_name."], err = db.Prepare(
			`UPDATE metrics_hash_name SET metric_id=$2, own_name=$3, created_time=$4
			WHERE own_hash = $1`); err != nil {
			return fmt.Errorf("Update.metrics_hash_name.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_plan
	{
		if dbr.requestsList["Select.metrics_plan.Data_Point_Role"], err = db.Prepare(
			`SELECT id FROM metrics_plan WHERE plan_date = $1 AND point_hash = $2 AND role_hash = $3`); err != nil {
			return fmt.Errorf("Select.metrics_plan.Data_Point: %v", err)
		}
		if dbr.requestsList["Insert.metrics_plan."], err = db.Prepare(
			`INSERT INTO metrics_plan(metric_id, plan_date, point_hash, role_hash, counts_arr, userhashes_arr)
			VALUES ($1, $2, $3, $4, $5, $6)`); err != nil {
			return fmt.Errorf("Insert.metrics_plan.: %v", err)
		}
		if dbr.requestsList["Update.metrics_plan."], err = db.Prepare(
			`UPDATE metrics_plan SET metric_id=$4, counts_arr=$5, userhashes_arr=$6
			WHERE plan_date = $1 AND point_hash = $2 AND role_hash = $3`); err != nil {
			return fmt.Errorf("Update.metrics_plan.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_events
	{
		if dbr.requestsList["Select.metrics_events.OrderID_UserHash_TypeEvent_TimeEvent"], err = db.Prepare(
			`SELECT id FROM metrics_events WHERE order_id = $1 AND user_hash = $2 AND type_event = $3 AND time_event = $4`); err != nil {
			return fmt.Errorf("Select.metrics_events.OrderID_UserHash_TypeEvent_TimeEvent: %v", err)
		}
		if dbr.requestsList["Insert.metrics_events."], err = db.Prepare(
			`INSERT INTO metrics_events(metric_id, order_id, user_hash, user_role, type_event, time_event, duration_event, description, point_hash)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`); err != nil {
			return fmt.Errorf("Insert.metrics_events.: %v", err)
		}
		if dbr.requestsList["Update.metrics_events."], err = db.Prepare(
			`UPDATE metrics_events SET metric_id=$5, user_role=$6, duration_event=$7, description=$8, point_hash=$9
			WHERE order_id = $1 AND user_hash = $2 AND type_event = $3 AND time_event = $4`); err != nil {
			return fmt.Errorf("Update.metrics_events.: %v", err)
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_bonuses
	{
		if dbr.requestsList["Select.metrics_bonuses.BonusID"], err = db.Prepare(
			`SELECT id FROM metrics_bonuses WHERE bonus_id = $1`); err != nil {
			return fmt.Errorf("Select.metrics_bonuses.BonusID: %v", err)
		}
		if dbr.requestsList["Insert.metrics_bonuses."], err = db.Prepare(
			`INSERT INTO metrics_bonuses(metric_id, bonus_id, phone, transaction_bonus, type_bonus, note, action_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`); err != nil {
			return fmt.Errorf("Insert.metrics_bonuses.: %v", err)
		}
		if dbr.requestsList["Update.metrics_bonuses."], err = db.Prepare(
			`UPDATE metrics_bonuses SET metric_id=$2, phone=$3, transaction_bonus=$4, type_bonus=$5, note=$6, action_time=$7
			WHERE bonus_id = $1`); err != nil {
			return fmt.Errorf("Update.metrics_bonuses.: %v", err)
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_users
	{
		if dbr.requestsList["Select.metrics_users.UserHash_UpdatedTime"], err = db.Prepare(
			`SELECT id FROM metrics_users WHERE user_hash = $1 AND update_time = $2`); err != nil {
			return fmt.Errorf("Select.metrics_users.UserHash_UpdatedTime: %v", err)
		}
		if dbr.requestsList["Insert.metrics_users."], err = db.Prepare(
			`INSERT INTO metrics_users(metric_id, user_hash, "UID", password, last_name, first_name, second_name, role_hash, point_hash, phone, inn, hour_rate, count_rate, "VPN_number", "VPN_password", language, level, level_change_time, check_plan, create_time, delete_time, update_time, full_name)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`); err != nil {
			return fmt.Errorf("Insert.metrics_users.: %v", err)
		}
		if dbr.requestsList["Update.metrics_users."], err = db.Prepare(
			`UPDATE metrics_users SET metric_id=$3, "UID"=$4, password=$5, last_name=$6, first_name=$7, second_name=$8, role_hash=$9, point_hash=$10, phone=$11, inn=$12, hour_rate=$13, count_rate=$14, "VPN_number"=$15, "VPN_password"=$16, language=$17, level=$18, level_change_time=$19, check_plan=$20, create_time=$21, delete_time=$22, full_name=$23
			WHERE user_hash = $1 AND update_time = $2`); err != nil {
			return fmt.Errorf("Update.metrics_users.: %v", err)
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_cashbox_shift
	{
		if dbr.requestsList["Select.metrics_cashbox_shift.CashRegister"], err = db.Prepare(
			`SELECT id FROM metrics_cashbox_shift WHERE cashregister = $1`); err != nil {
			return fmt.Errorf("Select.metrics_cashbox_shift.CashRegister: %v", err)
		}
		if dbr.requestsList["Insert.metrics_cashbox_shift."], err = db.Prepare(
			`INSERT INTO metrics_cashbox_shift(metric_id, cashregister, user_hash, point_hash, begin_time, end_time)
			VALUES ($1, $2, $3, $4, $5, $6)`); err != nil {
			return fmt.Errorf("Insert.metrics_cashbox_shift.: %v", err)
		}
		if dbr.requestsList["Update.metrics_cashbox_shift."], err = db.Prepare(
			`UPDATE metrics_cashbox_shift SET metric_id=$2, user_hash=$3, point_hash=$4, begin_time=$5, end_time=$6
			WHERE cashregister = $1`); err != nil {
			return fmt.Errorf("Update.metrics_cashbox_shift.: %v", err)
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_sklad
	{
		if dbr.requestsList["Select.metrics_sklad.SkladListID"], err = db.Prepare(
			`SELECT id FROM metrics_sklad WHERE sklad_list_id = $1`); err != nil {
			return fmt.Errorf("Select.metrics_sklad.SkladListID: %v", err)
		}
		if dbr.requestsList["Insert.metrics_sklad."], err = db.Prepare(
			`INSERT INTO metrics_sklad(metric_id, sklad_list_id, order_id, point_hash, sklad_hash, price_id, product_hash, product_name, count, type_units, action_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`); err != nil {
			return fmt.Errorf("Insert.metrics_sklad.: %v", err)
		}
		if dbr.requestsList["Update.metrics_sklad."], err = db.Prepare(
			`UPDATE metrics_sklad SET metric_id=$2, order_id=$3, point_hash=$4, sklad_hash=$5, price_id=$6, product_hash=$7, product_name=$8, count=$9, type_units=$10, action_time=$11
			WHERE sklad_list_id = $1`); err != nil {
			return fmt.Errorf("Update.metrics_sklad.: %v", err)
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_parameters
	{
		dbr.requestsList["Update.metrics_parameters.PendingDateAndId"], err = db.Prepare(`UPDATE metrics_parameters SET pending_date=$2, pending_id=$3, update_at=$4 WHERE service_table_id=$1`)
		if err != nil {
			return fmt.Errorf("Update.metrics_parameters.PendingDateAndId: %v", err)
		}
		dbr.requestsList["Select.metrics_parameters.ReportLastUpdateBySTId"], err = db.Prepare(`SELECT update_at FROM metrics_parameters WHERE service_table_id=$1`)
		if err != nil {
			return fmt.Errorf("Select.metrics_parameters.ReportLastUpdateBySTId: %v", err)
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
			MP.ID, MP.service_table_id, MP.timeout, MP.Own_ID, MP.pending_date, MP.pending_id, MP.protocol_version, MP.update_allow, MP.update_at,
			MST.ID, MST.Query, MST.TableName, MST.TypeParameter, MST.Service_ID, MST.Activ,   
			MS.ID, MS.Name, MS.IP   
			FROM metrics_parameters MP   
			INNER JOIN metrics_step MSTEP ON MP.Min_Step_ID=MSTEP.ID   
			--INNER JOIN metrics_service_data MSD ON MP.Interface_ID=MSD.ID  
			INNER JOIN metrics_service_table MST ON MP.service_table_id=MST.ID 
			INNER JOIN metrics_service MS ON MST.Service_ID=MS.ID
			INNER JOIN metrics_step_type MSTEPT ON MSTEPT.ID=MP.step_type_id
			WHERE MST.Activ = true
			ORDER BY MS.Name, minuts, MST.TypeParameter`)
		if err != nil {
			return fmt.Errorf("Select.metrics_link_step.: %v", err)
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_own
	{
		dbr.requestsList["Insert.metrics_own."], err = db.Prepare(`INSERT INTO metrics_own (name)  VALUES ($1)`)
		if err != nil {
			return fmt.Errorf("Insert.metrics_own.: %v", err)
		}
		dbr.requestsList["Update.metrics_own.Id"], err = db.Prepare(`UPDATE metrics_own SET name=$2 WHERE id=$1`)
		if err != nil {
			return fmt.Errorf("Update.metrics_own.Id: %v", err)
		}
		dbr.requestsList["Select.metrics_own."], err = db.Prepare(`SELECT id, name FROM metrics_own`)
		if err != nil {
			return fmt.Errorf("Select.metrics_own.: %v", err)
		}
		dbr.requestsList["Select.metrics_own.Id"], err = db.Prepare(`SELECT id, name FROM metrics_own WHERE id=$1`)
		if err != nil {
			return fmt.Errorf("Select.metrics_own.Id: %v", err)
		}
	}

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_service_table
	{
		dbr.requestsList["Insert.metrics_service_table."], err = db.Prepare(`INSERT INTO metrics_service_table(Query, TableName, TypeParameter, Service_ID) VALUES ($1, $2, $3, $4)`)
		if err != nil {
			return fmt.Errorf("Insert.metrics_service_table.: %v", err)
		}
		dbr.requestsList["Update.metrics_service_table.Id"], err = db.Prepare(`UPDATE metrics_service_table SET Query = $2, TableName = $3, TypeParameter = $4, Service_ID = $5 WHERE ID=$1`)
		if err != nil {
			return fmt.Errorf("Update.metrics_service_table.Id: %v", err)
		}
		dbr.requestsList["Select.metrics_service_table."], err = db.Prepare(`SELECT ID, Query, TableName, TypeParameter, Service_ID FROM metrics_service_table`)
		if err != nil {
			return fmt.Errorf("Select.metrics_service_table.: %v", err)
		}
		dbr.requestsList["Select.metrics_service_table.Id"], err = db.Prepare(`SELECT ID, Query, TableName, TypeParameter, Service_ID FROM metrics_service_table WHERE id=$1`)
		if err != nil {
			return fmt.Errorf("Select.metrics_service_table.Id: %v", err)
		}
		dbr.requestsList["Select.metrics_service_table.Service_id"], err = db.Prepare(`SELECT ID, Query, TableName, TypeParameter, Service_ID FROM metrics_service_table WHERE Service_ID=$1`)
		if err != nil {
			return fmt.Errorf("Select.metrics_service_table.Service_id: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_service
	{
		dbr.requestsList["Insert.metrics_service."], err = db.Prepare(`INSERT INTO metrics_service(name, ip) VALUES ($1, $2)`)
		if err != nil {
			return fmt.Errorf("Insert.metrics_service.: %v", err)
		}
		dbr.requestsList["Update.metrics_service.Id"], err = db.Prepare(`UPDATE metrics_service SET name = $2, ip = $3 WHERE id=$1`)
		if err != nil {
			return fmt.Errorf("Update.metrics_service.Id: %v", err)
		}
		dbr.requestsList["Select.metrics_service."], err = db.Prepare(`SELECT id, name, ip FROM metrics_service`)
		if err != nil {
			return fmt.Errorf("Select.metrics_service.: %v", err)
		}
		dbr.requestsList["Select.metrics_service.Id"], err = db.Prepare(`SELECT id, name, ip FROM metrics_service WHERE id=$1`)
		if err != nil {
			return fmt.Errorf("Select.metrics_service.Id: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_step
	{
		if dbr.requestsList["Insert.metrics_step."], err = db.Prepare(`INSERT INTO metrics_step(name, value) VALUES ($1, $2)`); err != nil {
			return fmt.Errorf("Insert.metrics_step.: %v", err)
		}
		if dbr.requestsList["Update.metrics_step.Id"], err = db.Prepare(`UPDATE metrics_step SET name = $2, value = $3 WHERE id=$1`); err != nil {
			return fmt.Errorf("Update.metrics_step.Id: %v", err)
		}
		if dbr.requestsList["Select.metrics_step."], err = db.Prepare(`SELECT id, name, value, EXTRACT(EPOCH FROM value::INTERVAL)/60 as minuts FROM metrics_step ORDER BY minuts ASC`); err != nil {
			return fmt.Errorf("Select.metrics_step.: %v", err)
		}
		if dbr.requestsList["Select.metrics_step.Id"], err = db.Prepare(`SELECT id, name, value, EXTRACT(EPOCH FROM value::INTERVAL)/60 as minuts FROM metrics_step WHERE id=$1`); err != nil {
			return fmt.Errorf("Select.metrics_step.Id: %v", err)
		}
	}

	fmt.Println("Requests init success")
	return nil
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
		log.Panic("Postgresql initRequests error:", err)
	}

	log.Println("Запросы к Postgresql инициализированы")
	println("Запросы к Postgresql инициализированы")
}
