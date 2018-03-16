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
		dbr.requestsList["Select.metrics.ReportSaleNewByInterval"], err = db.Prepare(`
			SELECT moli.price_name, moli.type_id, 
			coalesce(moli.type_name, ''), 
			sum(ceil(moli.price-moli.price*moli.discount_percent/100)), moli.price_id, sum(moli.count), sum(moli.real_foodcost),
			(moli.id_parent_item <> 0) as is_modifier
			FROM metrics m 
			inner join metrics_orders_info moi on m.id = moi.metric_id
			inner join metrics_orders_list_info moli on moi.order_id = moli.order_id AND CASE WHEN date(moi.creator_time) < date('2018-03-14') THEN moli.id_parent_item = 0 ELSE TRUE END
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= (date($2) || ' 06:00:00')::timestamp AND moi.creator_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				OR (moi.date_preorder_cook >= (date($2) || ' 06:00:00')::timestamp AND moi.date_preorder_cook < (date($3) || ' 06:00:00')::timestamp + interval '1 day'))
				AND moi.type <> 4
				AND moli.over_status_id not in (15, 16)
				AND date(moi.cancel_time) = date('0001-01-01')
			GROUP BY moli.price_name, moli.type_id, moli.type_name, moli.price_id, is_modifier
			ORDER BY moli.price_name
			`)
		// date(localtimestamp) <= date('2018-03-15') - real_foodcost считался с учетом модификаторов, теперь модификаторы отдельно
		// ceil, round, floor
		// type	- тип заказа(1-"Навынос",2-"Доставка",3-"Ресторан",4-"Довоз",5;"Предзаказ"(не используется))
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportSaleNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxPrepayByInterval"], err = db.Prepare(`
			SELECT coalesce(sum(cash), 0)
			FROM metrics m
			INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
			INNER JOIN metrics_orders_info moi ON moi.order_id = mc.order_id
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				(moi.date_preorder_cook >= (date($2) || ' 06:00:00')::timestamp and moi.date_preorder_cook <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(mc.action_time) <> date(moi.date_preorder_cook)
				AND mc.order_id <> 0
			`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxPrepayByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxPostpayByInterval"], err = db.Prepare(`
			SELECT coalesce(sum(cash), 0)
			FROM metrics m
			INNER JOIN metrics_cashbox mc ON mc.metric_id = m.id
			INNER JOIN metrics_orders_info moi ON moi.order_id = mc.order_id
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				mc.action_time >= (date($2) || ' 06:00:00')::timestamp and mc.action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day'
				AND date(mc.action_time) <> date(moi.date_preorder_cook ) AND date(moi.date_preorder_cook ) <> date('0001-01-01')
				AND mc.order_id <> 0
			`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxPostpayByInterval: %v", err)
		}

		/////////
		//		dbr.requestsList["Select.metrics.ReportCashboxReturnByInterval"], err = db.Prepare(`
		//			SELECT abs(sum(cash))
		//			FROM metrics m
		//			inner join metrics_cashbox mc on mc.metric_id = m.id
		//			WHERE (m.ownhash = $1 or $1 = 'all') AND
		//				cashregister in (SELECT cashregister FROM metrics_cashbox WHERE action_time >= (date($2) || ' 06:00:00')::timestamp and action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
		//		`)
		//		if err != nil {
		//			return fmt.Errorf("Select.metrics.ReportCashboxReturnByInterval: %v", err)
		//		}

		/////////
		dbr.requestsList["Select.metrics.ReportCashboxNewByInterval"], err = db.Prepare(`
			SELECT cashregister, action_time, userhash, coalesce(user_name, ''), info, type_payments, cash, date_preorder, order_id <> 0 as is_orders
			FROM metrics m
			inner join metrics_cashbox mc on mc.metric_id = m.id
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				cashregister in (SELECT cashregister FROM metrics_cashbox WHERE action_time >= (date($2) || ' 06:00:00')::timestamp and action_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
			ORDER BY cashregister, action_time
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCashboxNewByInterval: %v", err)
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
		//				AND moi.type <> 4
		//				AND mc.type_payments = $4
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportSummOnTypePayments: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCouriersNewByInterval"], err = db.Prepare(`
			SELECT coalesce(mhn.own_name,'-'), moi.courier_hash, count(moi.order_id), array_agg(moi.order_id), extract(epoch from (AVG(moi.courier_end_time - moi.courier_start_time))) as avg_time, COUNT(NULLIF((extract(epoch from (moi.courier_end_time - moi.courier_start_time))-1 > 15 * 60), false)) as count_overtime
			FROM metrics m 
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.courier_hash
			WHERE (m.ownhash = $1 or $1 = 'all') AND 
				(moi.courier_start_time >= (date($2) || ' 06:00:00')::timestamp and moi.courier_start_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(moi.courier_end_time) <> date('0001-01-01')
			GROUP BY mhn.own_name, moi.courier_hash
			ORDER BY count(moi.order_id) DESC
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCouriersNewByInterval: %v", err)
		}

		/////////
		dbr.requestsList["Select.metrics.ReportCouriersAddrByInterval"], err = db.Prepare(`
			SELECT coalesce(mhn.own_name,'-'), moi.courier_hash, moi.city, moi.street, moi.house, moi.building, sum(moli.price-(moli.price*moli.discount_percent/100)) as price, extract(epoch from (moi.courier_end_time - moi.courier_start_time)) as time_delivery, extract(epoch from (moi.courier_start_time - moi.collector_time)) as time_taken
			FROM metrics m 
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			INNER JOIN metrics_orders_list_info moli on moi.order_id = moli.order_id AND moli.set = false
			LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.courier_hash
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				(moi.courier_start_time >= (date($2) || ' 06:00:00')::timestamp and moi.courier_start_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(moi.courier_end_time) <> date('0001-01-01')
			GROUP BY mhn.own_name, moi.courier_hash, moi.city, moi.street, moi.building, moi.house, time_delivery, time_taken
			ORDER BY mhn.own_name
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
			SELECT coalesce(mhn.own_name,'-'), moi.creator_hash, count(moi.order_id)
			FROM metrics m 
			INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
			LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moi.creator_hash
			WHERE (m.ownhash = $1 or $1 = 'all') and 
				(moi.creator_time >= (date($2) || ' 06:00:00')::timestamp and moi.creator_time <= (date($3) || ' 06:00:00')::timestamp + interval '1 day')
			GROUP BY mhn.own_name, moi.creator_hash
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
			
			SELECT tp.hours::date as dates, tp.hours::time as times, COUNT(o.order_id), COUNT(NULLIF(date(o.date_preorder_cook) = date('0001-01-01'), TRUE)) as preorders, COUNT(NULLIF(o.type = 1, FALSE)) as delivery, COUNT(NULLIF(o.type = 2, FALSE)) as takeout
			FROM timeparts tp
			LEFT JOIN orders o ON (o.order_time::date = tp.hours::date AND o.order_time::time >= tp.hours::time AND o.order_time::time < tp.hours::time + interval '1 hour')
			GROUP BY dates, times
			ORDER BY dates, times
		`)
		//--SUM((date(o.date_preorder_cook) = date('0001-01-01'))::int) as preorders,
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportOrdersOnTime: %v", err)
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
				AND moi.type = 2
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
				SELECT order_id, (case when date(moi.date_preorder_cook) = date('0001-01-01') then moi.creator_time else moi.date_preorder_cook end) as order_time
				FROM metrics m
				INNER JOIN metrics_orders_info moi ON moi.metric_id = m.id 
				WHERE (m.ownhash = $1 or $1 = 'all') AND
				(
					((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp - interval '7 day' AND moi.creator_time <= date($3)::timestamp - interval '6 day')
					OR (moi.date_preorder_cook >= date($2)::timestamp - interval '7 day' AND moi.date_preorder_cook <= date($3)::timestamp - interval '6 day'))
				 OR ((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp - interval '14 day' AND moi.creator_time <= date($3)::timestamp - interval '13 day')
					OR (moi.date_preorder_cook >= date($2)::timestamp - interval '14 day' AND moi.date_preorder_cook <= date($3)::timestamp - interval '13 day'))
				 OR ((date(moi.date_preorder_cook) = date('0001-01-01') AND moi.creator_time >= date($2)::timestamp - interval '1 month' AND moi.creator_time <= date($3)::timestamp - interval '1 month' + interval '1 day')
					OR (moi.date_preorder_cook >= date($2)::timestamp - interval '28 day' AND moi.date_preorder_cook <= date($3)::timestamp - interval '27 day'))
				)
			)
			
			SELECT tp.hours::date as dates, tp.hours::time as times, ceil(COUNT(o.order_id)/3::float)
			FROM timeparts tp
			LEFT JOIN orders o ON ((o.order_time::date = tp.hours::date - interval '7 day' OR o.order_time::date = tp.hours::date - interval '14 day' OR o.order_time::date = tp.hours::date - interval '28 day') AND o.order_time::time >= tp.hours::time AND o.order_time::time < tp.hours::time + interval '1 hour')
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
				FROM (SELECT id, point_hash, role_hash, plan_date, unnest(cook_numbers) AS count_cook FROM metrics_plan WHERE plan_date >= date($2) AND plan_date <= date($3) + interval '1 day' - interval '1 millisecond') t
				WHERE (point_hash = $1 or $1 = 'all') --AND
					--role_hash in ('8746fffb4f2e033aabefa8103e7e4f4d183f0098f1e6513a718c0dcff60be6c2048faaefc6477973c321c8f7c52c96d078c99b188ac2a11a221fb97fa957ccd3','b6b8c237446b537594a2e1fc44d1d522b0ac62ef3e157e940eb39db9c45deefe151ee05a292e8366127c26901efca3882670d1c53ba11c1169c3c53a71b686c2')
				ORDER BY plan_date, timeparts
			)
			
			SELECT mp.cooking_type, mp.plan_date::date as dates, mp.timeparts::time as times, mp.point_hash, coalesce(mhn_point.own_name,'-') as point_name, sum(mp.count_cook) as count_cook,
				--(SELECT COUNT(o.id_item) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) as count_items,
				--array(SELECT (o.time_cook) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) as aa,
				(case when sum(mp.count_cook) > 0 then 
					ceil((SELECT coalesce(SUM(o.time_cook), 0) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) / (sum(mp.count_cook) * 30 * 60) * 100) 
					else ceil((SELECT coalesce(SUM(o.time_cook), 0) FROM orders o WHERE date(o.start_time) = date(mp.plan_date) AND o.start_time::time >= mp.timeparts::time AND o.start_time::time < mp.timeparts::time + interval '30 minute' AND o.ownhash = mp.point_hash AND o.cooking_type = mp.cooking_type) / 1800 * 100) 
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
			SELECT coalesce(mhn.own_name, '-') as cook_name, coalesce(mhnr.own_name, '-') as cook_role, count(moli.id) as count_orders, extract(epoch from (AVG(moli.end_time - moli.start_time))) as avg_time, COUNT(NULLIF((extract(epoch from (moli.end_time - moli.start_time))-1 > (case when cooking_tracker = 1 then time_cook else time_cook + time_fry end)), false)) as count_overtime, (case when cooking_tracker = 1 then 1 else 2 end) as ct
			FROM metrics m 
			INNER JOIN metrics_orders_list_info moli ON moli.metric_id = m.id
			LEFT JOIN metrics_hash_name mhn ON mhn.own_hash = moli.cook_hash
			LEFT JOIN metrics_hash_name mhnr ON mhnr.own_hash = moli.cook_role
			WHERE (m.ownhash = $1 or $1 = 'all') AND
				(moli.start_time >= (date($2) || ' 06:00:00')::timestamp and moli.start_time < (date($3) || ' 06:00:00')::timestamp + interval '1 day')
				AND date(moli.end_time) <> date('0001-01-01')
				AND set = false
			GROUP BY mhnr.own_name, mhn.own_name, ct
			ORDER BY ct, coalesce(mhnr.own_name, '-'), coalesce(mhn.own_name, '-')
		`)
		if err != nil {
			return fmt.Errorf("Select.metrics.ReportCookByInterval: %v", err)
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
		if dbr.requestsList["Insert.metrics_cashbox."], err = db.Prepare(
			`INSERT INTO metrics_cashbox(metric_id, order_id, cashregister, action_time, userhash, user_name, info, type_payments, cash, date_preorder) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`); err != nil {
			return fmt.Errorf("Insert.metrics_cashbox.: %v", err)
		}
		if dbr.requestsList["Select.metrics_cashbox.Order_Id"], err = db.Prepare(
			`SELECT id FROM metrics_cashbox WHERE order_id=$1 AND action_time=$2`); err != nil {
			return fmt.Errorf("Select.metrics_cashbox.Order_Id.: %v", err)
		}
		if dbr.requestsList["Select.metrics_cashbox."], err = db.Prepare(
			`SELECT id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder 
				FROM metrics_cashbox`); err != nil {
			return fmt.Errorf("Select.metrics_cashbox.: %v", err)
		}
		if dbr.requestsList["Select.metrics_cashbox.Metric_id"], err = db.Prepare(
			`SELECT id, metric_id, cashregister, action_time, userhash, info, type_payments, cash, date_preorder 
				FROM metrics_cashbox WHERE metric_id=$1`); err != nil {
			return fmt.Errorf("Select.metrics_cashbox.Metric_id.: %v", err)
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
		if dbr.requestsList["Insert.metrics_orders_info."], err = db.Prepare(
			`INSERT INTO metrics_orders_info(metric_id, order_id, chain_hash, org_hash, point_hash, id_day_point, cashregister_id, count_elements, date_preorder_cook, side_order, type_delivery, type_payments, price, bonus, discount_id, discount_name, discount_percent, city, street, house, building, creator_hash, creator_role_hash, creator_time, duration_of_create, duration_of_select_element, cook_start_time, cook_end_time, collector_hash, collector_time, courier_hash, courier_start_time, courier_end_time, cancel_hash, cancel_time, cancellation_reason_id, cancellation_reason_note, crash_user_hash, crash_user_role_hash, compensation, type_compensation, type, customer_phone)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43)`); err != nil {
			return fmt.Errorf("Insert.metrics_orders_info.: %v", err)
		}
		if dbr.requestsList["Select.metrics_orders_info.Order_id"], err = db.Prepare(
			`SELECT id FROM metrics_orders_info WHERE order_id = $1`); err != nil {
			return fmt.Errorf("Select.metrics_orders_info.Order_id: %v", err)
		}
		if dbr.requestsList["Update.metrics_orders_info."], err = db.Prepare(
			`UPDATE metrics_orders_info SET metric_id=$2, chain_hash=$3, org_hash=$4, point_hash=$5, id_day_point=$6, cashregister_id=$7, count_elements=$8, date_preorder_cook=$9, side_order=$10, type_delivery=$11, type_payments=$12, price=$13, bonus=$14, discount_id=$15, discount_name=$16, discount_percent=$17, city=$18, street=$19, house=$20, building=$21, creator_hash=$22, creator_role_hash=$23, creator_time=$24, duration_of_create=$25, duration_of_select_element=$26, cook_start_time=$27, cook_end_time=$28, collector_hash=$29, collector_time=$30, courier_hash=$31, courier_start_time=$32, courier_end_time=$33, cancel_hash=$34, cancel_time=$35, cancellation_reason_id=$36, cancellation_reason_note=$37, crash_user_hash=$38, crash_user_role_hash=$39, compensation=$40, type_compensation=$41, type=$42, customer_phone=$43 
			WHERE order_id=$1`); err != nil {
			return fmt.Errorf("Update.metrics_orders_info.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_orders_list_info
	{
		if dbr.requestsList["Insert.metrics_orders_list_info."], err = db.Prepare(
			`INSERT INTO metrics_orders_list_info(metric_id, order_id, id_item, id_parent_item, price_id, price_name, type_id, cooking_tracker, discount_id, discount_name, discount_percent, price, cook_hash, start_time, end_time, fail_id, fail_user_hash, fail_comments, real_foodcost, count, type_name, over_status_id, time_cook, time_fry, set, cook_role)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)`); err != nil {
			return fmt.Errorf("Insert.metrics_orders_list_info.: %v", err)
		}
		if dbr.requestsList["Select.metrics_orders_list_info.IdItem_OrderId"], err = db.Prepare(
			`SELECT id FROM metrics_orders_list_info 
			WHERE id_item = $1 and order_id = $2`); err != nil {
			return fmt.Errorf("Select.metrics_orders_list_info.IdItem_OrderId: %v", err)
		}
		if dbr.requestsList["Update.metrics_orders_list_info."], err = db.Prepare(
			`UPDATE metrics_orders_list_info SET metric_id=$3, id_parent_item=$4, price_id=$5, price_name=$6, type_id=$7, cooking_tracker=$8, discount_id=$9, discount_name=$10, discount_percent=$11, price=$12, cook_hash=$13, start_time=$14, end_time=$15, fail_id=$16, fail_user_hash=$17, fail_comments=$18, real_foodcost=$19, count=$20, type_name=$21, over_status_id=$22, time_cook=$23, time_fry=$24, set=$25, cook_role=$26 
			WHERE id_item=$1 AND order_id=$2`); err != nil {
			return fmt.Errorf("Update.metrics_orders_list_info.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// metrics_hash_name
	{
		if dbr.requestsList["Insert.metrics_hash_name."], err = db.Prepare(
			`INSERT INTO metrics_hash_name(metric_id, own_hash, own_name, created_time)
			VALUES ($1, $2, $3, $4)`); err != nil {
			return fmt.Errorf("Insert.metrics_hash_name.: %v", err)
		}
		if dbr.requestsList["Select.metrics_hash_name.Hash"], err = db.Prepare(
			`SELECT id FROM metrics_hash_name WHERE own_hash = $1`); err != nil {
			return fmt.Errorf("Select.metrics_hash_name.Hash: %v", err)
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
		if dbr.requestsList["Insert.metrics_plan."], err = db.Prepare(
			`INSERT INTO metrics_plan(metric_id, plan_date, point_hash, role_hash, cook_numbers)
			VALUES ($1, $2, $3, $4, $5)`); err != nil {
			return fmt.Errorf("Insert.metrics_plan.: %v", err)
		}
		if dbr.requestsList["Select.metrics_plan.Data_Point_Role"], err = db.Prepare(
			`SELECT id FROM metrics_plan WHERE plan_date = $1 AND point_hash = $2 AND role_hash = $3`); err != nil {
			return fmt.Errorf("Select.metrics_plan.Data_Point: %v", err)
		}
		if dbr.requestsList["Update.metrics_plan."], err = db.Prepare(
			`UPDATE metrics_plan SET metric_id=$4, cook_numbers=$5
			WHERE plan_date = $1 AND point_hash = $2 AND role_hash = $3`); err != nil {
			return fmt.Errorf("Update.metrics_plan.: %v", err)
		}
	}
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////metrics_parameters
	{
		dbr.requestsList["Insert.metrics_parameters."], err = db.Prepare(`INSERT INTO metrics_parameters (service_table_id, Type_Mod_ID,  Own_ID, Min_Step_ID) VALUES ($1, $2, $3, $4)`)
		if err != nil {
			return fmt.Errorf("Insert.metrics_parameters.: %v", err)
		}
		dbr.requestsList["Update.metrics_parameters.Id"], err = db.Prepare(`UPDATE metrics_parameters SET service_table_id=$2, Type_Mod_ID=$3, Own_ID=$4, Min_Step_ID=$5 WHERE ID=$1`)
		if err != nil {
			return fmt.Errorf("Update.metrics_parameters.Id: %v", err)
		}
		dbr.requestsList["Select.metrics_parameters."], err = db.Prepare(`SELECT ID, service_table_id, Type_Mod_ID, Own_ID, Min_Step_ID FROM metrics_parameters`)
		if err != nil {
			return fmt.Errorf("Select.metrics_parameters.: %v", err)
		}
		dbr.requestsList["Select.metrics_parameters.Id"], err = db.Prepare(`SELECT ID, service_table_id, Type_Mod_ID, Own_ID, Min_Step_ID FROM metrics_parameters WHERE ID=$1`)
		if err != nil {
			return fmt.Errorf("Select.metrics_parameters.Id: %v", err)
		}
		dbr.requestsList["Select.metrics_parameters.Own_id"], err = db.Prepare(`SELECT ID, service_table_id, Type_Mod_ID, Own_ID, Min_Step_ID FROM metrics_parameters WHERE Own_ID=$1`)
		if err != nil {
			return fmt.Errorf("Select.metrics_parameters.Own_id: %v", err)
		}
		/////////////////
		dbr.requestsList["Update.metrics_parameters.PendingDateAndId"], err = db.Prepare(`UPDATE metrics_parameters SET pending_date=$2, pending_id=$3 WHERE service_table_id=$1`)
		if err != nil {
			return fmt.Errorf("Update.metrics_parameters.PendingDateAndId: %v", err)
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
			MP.ID, MP.service_table_id, MP.Type_Mod_ID, MP.Own_ID, MP.pending_date, MP.pending_id, MP.protocol_version, MP.update_allow,
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
