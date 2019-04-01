
---run this script in the transit-performance database
--USE transit_performance
--GO

--This procedure processes all the real-time events. It is executed by the process_rt_event trigger ON INSERT into the dbo.rt_event table.

IF OBJECT_ID('ProcessCurrentMetrics','P') IS NOT NULL
	DROP PROCEDURE dbo.ProcessCurrentMetrics
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ProcessCurrentMetrics

AS

BEGIN
	SET NOCOUNT ON;

	DECLARE @current_time DATETIME
	SET @current_time = GETDATE()

	DECLARE @current_time_last_hour DATETIME
	SET @current_time_last_hour = DATEADD(s,-3600,@current_time)

	--save current metrics for each route	

	IF OBJECT_ID('tempdb..#today_rt_current_metrics','U') IS NOT NULL
		DROP TABLE #today_rt_current_metrics

	CREATE TABLE #today_rt_current_metrics
	(
		route_id						VARCHAR(255)	NOT NULL
		,threshold_id					VARCHAR(255)	NOT NULL
		,threshold_name					VARCHAR(255)	NOT NULL
		,threshold_type					VARCHAR(255)	NOT NULL
		,metric_result_last_hour		FLOAT			NULL
		,metric_result_current_day		FLOAT			NULL
		,metric_result_trip_last_hour	FLOAT			NULL
		,metric_result_trip_current_day	FLOAT			NULL
	)

	IF OBJECT_ID('tempdb..#current_metrics_day_til_now','U') IS NOT NULL
		DROP TABLE #current_metrics_day_til_now
	--
	CREATE TABLE #current_metrics_day_til_now
	(
		route_id						VARCHAR(255)	NOT NULL
		,threshold_id					VARCHAR(255)	NOT NULL
		,threshold_name					VARCHAR(255)	NOT NULL
		,threshold_type					VARCHAR(255)	NOT NULL
		,metric_result_current_day		FLOAT			NULL
		,metric_result_trip_current_day	FLOAT			NULL
	)

	IF OBJECT_ID('tempdb..#current_metrics_last_hour','U') IS NOT NULL
		DROP TABLE #current_metrics_last_hour
	--
	CREATE TABLE #current_metrics_last_hour
	(
		route_id						VARCHAR(255)	NOT NULL
		,threshold_id					VARCHAR(255)	NOT NULL
		,threshold_name					VARCHAR(255)	NOT NULL
		,threshold_type					VARCHAR(255)	NOT NULL
		,metric_result_last_hour		FLOAT			NULL
		,metric_result_trip_last_hour	FLOAT			NULL
	)

	DECLARE @route_ids TABLE
	(
		route_id VARCHAR(255)
	)

	INSERT INTO @route_ids
		VALUES ('Red'),('Blue'),('Orange')
		,('Green-B'),('Green-C'),('Green-D'),('Green-E')
		,('CR-Fairmount'),('CR-Fitchburg'),('CR-Franklin'),('CR-Greenbush'),('CR-Haverhill'),('CR-Kingston'),('CR-Lowell'),('CR-Middleborough')
		,('CR-Needham'),('CR-Newburyport'),('CR-Providence'),('CR-Worcester')

	INSERT INTO #current_metrics_day_til_now
	(
		route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,metric_result_current_day
		,metric_result_trip_current_day
	)

		SELECT
			tw2.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1 - SUM(tw2.scheduled_threshold_numerator_pax) / NULLIF(SUM(tw2.denominator_pax),0) AS metric_result_current_day
			,NULL
		FROM	dbo.today_rt_wait_time_od_threshold_pax tw2
				,dbo.config_threshold ct
		WHERE
					ct.threshold_id = tw2.threshold_id
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					tw2.prev_route_id IN (SELECT route_id FROM @route_ids)
				)
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					tw2.route_id IN (SELECT route_id FROM @route_ids)
				)
		GROUP BY
			tw2.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id

		UNION

		SELECT
			tt2.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1 - SUM(tt2.scheduled_threshold_numerator_pax) / NULLIF(SUM(tt2.denominator_pax),0) AS metric_result_current_day
			,NULL AS metric_result_trip_current_day

		FROM	dbo.today_rt_travel_time_threshold_pax tt2
				,dbo.config_threshold ct
		WHERE
				ct.threshold_id = tt2.threshold_id
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					tt2.route_id IN (SELECT route_id FROM @route_ids)
				)
		GROUP BY
			tt2.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id

		UNION

		SELECT
			sa2.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1 - SUM(sa2.scheduled_threshold_numerator_pax) / NULLIF(SUM(sa2.denominator_pax),0) AS metric_result_current_day
			,NULL AS metric_result_trip_current_day

		FROM	dbo.today_rt_schedule_adherence_threshold_pax sa2
				,dbo.config_threshold ct
		WHERE
				ct.threshold_id = sa2.threshold_id
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					sa2.route_id IN (SELECT route_id FROM @route_ids)
				)
		GROUP BY
			sa2.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id
		UNION

		SELECT
			dtt.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,NULL AS metric_result_current_day
			,1 - SUM(scheduled_threshold_numerator_trip) / SUM(denominator_trip) AS metric_result_trip_current_day
		FROM	dbo.today_rt_headway_time_threshold_trip dtt
				,dbo.config_threshold ct
		WHERE
				ct.threshold_id = dtt.threshold_id
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					route_id IN (SELECT route_id FROM @route_ids)
				)
		GROUP BY
			dtt.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id

		ORDER BY
			route_id,threshold_id


	INSERT INTO #current_metrics_last_hour
	(
		route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,metric_result_last_hour
		,metric_result_trip_last_hour
	)

		SELECT
			tw1.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1 - SUM(tw1.scheduled_threshold_numerator_pax) / NULLIF(SUM(tw1.denominator_pax),0) AS metric_result_last_hour
			,NULL AS metric_result_trip_last_hour
		FROM	dbo.today_rt_wait_time_od_threshold_pax tw1
				,dbo.config_threshold ct
		WHERE
				ct.threshold_id = tw1.threshold_id
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					tw1.prev_route_id IN (SELECT route_id FROM @route_ids)
				)
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					tw1.route_id IN (SELECT route_id FROM @route_ids)
				)
			AND
				DATEADD(s,tw1.end_time_sec,tw1.service_date) >= @current_time_last_hour
			AND
				DATEADD(s,tw1.end_time_sec,tw1.service_date) <= @current_time
		GROUP BY
			tw1.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id

		UNION

		SELECT
			tt1.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,1 - SUM(tt1.scheduled_threshold_numerator_pax) / NULLIF(SUM(tt1.denominator_pax),0) AS metric_result_last_hour
			,NULL AS metric_result_trip_last_hour
		FROM	dbo.today_rt_travel_time_threshold_pax tt1
				,dbo.config_threshold ct
		WHERE
				ct.threshold_id = tt1.threshold_id
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					tt1.route_id IN (SELECT route_id FROM @route_ids)
				)
			AND
				DATEADD(s,tt1.end_time_sec,tt1.service_date) >= @current_time_last_hour
			AND
				DATEADD(s,tt1.end_time_sec,tt1.service_date) <= @current_time
		GROUP BY
			tt1.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id

		UNION

		SELECT
			t.route_id
			,t.threshold_id
			,t.threshold_name
			,t.threshold_type
			,1 - SUM(t.scheduled_threshold_numerator_pax) / NULLIF(SUM(t.denominator_pax),0) AS metric_result_last_hour
			,NULL AS metric_result_trip_last_hour
		FROM
		(
			SELECT
				sa1.route_id
				,ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,sa1.scheduled_threshold_numerator_pax AS scheduled_threshold_numerator_pax
				,sa1.denominator_pax AS denominator_pax
				,sa1.scheduled_threshold_numerator_trip AS scheduled_threshold_numerator_trip
				,sa1.denominator_trip AS denominator_trip
				,CASE
					WHEN sa1.stop_order_flag = 1 THEN DATEADD(s,sa1.actual_departure_time_sec,sa1.service_date)
					ELSE DATEADD(s,sa1.actual_arrival_time_sec,sa1.service_date)
				END AS end_date_time
			FROM	dbo.today_rt_schedule_adherence_threshold_pax sa1
					,dbo.config_threshold ct
			WHERE
					ct.threshold_id = sa1.threshold_id
				AND
					(
						(SELECT COUNT(route_id) FROM @route_ids) = 0
					OR
						sa1.route_id IN (SELECT route_id FROM @route_ids)
					)
		) t
		WHERE
			t.end_date_time >= @current_time_last_hour
			AND t.end_date_time <= @current_time
		GROUP BY
			t.route_id
			,t.threshold_id
			,t.threshold_name
			,t.threshold_type
			,t.threshold_id
		UNION
		SELECT
			dtt.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,NULL AS metric_result_last_hour
			,1 - SUM(scheduled_threshold_numerator_trip) / SUM(denominator_trip) AS metric_result_trip_last_hour
		FROM	dbo.today_rt_headway_time_threshold_trip dtt
				,dbo.config_threshold ct
		WHERE
				ct.threshold_id = dtt.threshold_id
			AND
				(
					(SELECT COUNT(route_id) FROM @route_ids) = 0
				OR
					route_id IN (SELECT route_id FROM @route_ids)
				)
			AND
				DATEADD(s,dtt.end_time_sec,dtt.service_date) >= @current_time_last_hour
			AND
				DATEADD(s,dtt.end_time_sec,dtt.service_date) <= @current_time
		GROUP BY
			dtt.route_id
			,ct.threshold_id
			,ct.threshold_name
			,ct.threshold_type
			,ct.threshold_id
		ORDER BY
			route_id,threshold_id


	INSERT INTO #today_rt_current_metrics
	(
		route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,metric_result_last_hour
		,metric_result_current_day
		,metric_result_trip_last_hour
		,metric_result_trip_current_day
	)

		SELECT
			c1.route_id
			,c1.threshold_id
			,c1.threshold_name
			,c1.threshold_type
			,c2.metric_result_last_hour
			,c1.metric_result_current_day
			,c2.metric_result_trip_last_hour
			,c1.metric_result_trip_current_day

		FROM #current_metrics_day_til_now c1
		LEFT JOIN #current_metrics_last_hour c2
		ON
				c1.route_id = c2.route_id
			AND 
				c1.threshold_id = c2.threshold_id

	BEGIN TRANSACTION

		DELETE FROM dbo.today_rt_current_metrics

		INSERT INTO dbo.today_rt_current_metrics
		(
			route_id
			,threshold_id
			,threshold_name
			,threshold_type
			,metric_result_last_hour
			,metric_result_current_day
			,metric_result_trip_last_hour
			,metric_result_trip_current_day
		)

			SELECT
				route_id
				,threshold_id
				,threshold_name
				,threshold_type
				,metric_result_last_hour
				,metric_result_current_day
				,metric_result_trip_last_hour
				,metric_result_trip_current_day

			FROM #today_rt_current_metrics
			ORDER BY
				route_id,
				threshold_id

	COMMIT TRANSACTION

END
GO