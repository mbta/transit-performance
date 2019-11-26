
---run this script in the transit-performance database
--USE transit_performance
--GO

DROP PROCEDURE dbo.getMetrics
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE dbo.getMetrics

--Script Version: Master - 1.1.0.0

--This stored procedure is called by the metrics API call.  It selects metrics for a particular route, direction, stop for the requested time period. 

	@from_stop_ids	str_val_type READONLY
	,@to_stop_ids	str_val_type READONLY
	,@route_ids		str_val_type READONLY
	,@direction_ids	int_val_type READONLY
	,@from_time		DATETIME
	,@to_time		DATETIME
	
AS

BEGIN
    SET NOCOUNT ON; 

	DECLARE @metricstemp AS TABLE
	(			
		threshold_id VARCHAR(255)
		,threshold_name VARCHAR(255)
		,threshold_type VARCHAR(255)
		,metric_result FLOAT
		,metric_result_trip FLOAT
	)

	IF
		(
			(DATEDIFF(s,@from_time,@to_time) <= 21600) 
		AND 
			((SELECT COUNT(str_val) FROM @route_ids WHERE str_val NOT IN ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C','Green-D','Green-E')) = 0)
		)
		
	BEGIN --if a timespan is less than 6 hours and routes are only subway/light rail, then do the processing, if not return empty set

		DECLARE @service_date DATE
		SET @service_date = dbo.fnConvertDateTimeToServiceDate(@from_time)

		DECLARE @today_service_date DATE
		SET @today_service_date = dbo.fnConvertDateTimeToServiceDate(GETDATE())

		IF @service_date <> @today_service_date
		BEGIN

			INSERT INTO @metricstemp
			SELECT 
				t.threshold_id
				,t.threshold_name
				,t.threshold_type
				,1 - SUM(scheduled_threshold_numerator_pax)/SUM(denominator_pax) AS metric_result
				,1 - SUM(scheduled_threshold_numerator_trip)/SUM(denominator_trip) AS metric_result_trip --added

				FROM (
		
							SELECT
								ct.threshold_id
								,ct.threshold_name
								,ct.threshold_type
								,scheduled_threshold_numerator_pax
								,denominator_pax
								,scheduled_threshold_numerator_trip --added
								,denominator_trip --added
								,DATEADD(s,hwt.end_time_sec,hwt.service_date) as end_date_time
		
							FROM
								dbo.historical_wait_time_od_threshold_pax hwt
								,dbo.config_threshold ct
							WHERE
									ct.threshold_id = hwt.threshold_id
								AND

									(
										(SELECT COUNT(str_val) FROM @from_stop_ids) = 0
									OR
										from_stop_id IN (SELECT str_val FROM @from_stop_ids)
									)	
								AND
									(
										(SELECT COUNT(str_val) FROM @to_stop_ids) = 0
									OR
										to_stop_id IN (SELECT str_val FROM @to_stop_ids)
									)	
								AND
									(
										(SELECT COUNT(int_val) FROM @direction_ids) = 0
									OR
										direction_id IN (SELECT int_val FROM @direction_ids)
									)
								AND
									(
										(SELECT COUNT(str_val) FROM @route_ids) = 0
									OR
										prev_route_id IN (SELECT str_val FROM @route_ids)
									)
								AND
									(
										(SELECT COUNT(str_val) FROM @route_ids) = 0
									OR
										route_id IN (SELECT str_val FROM @route_ids)
									)
								AND
									hwt.service_date = @service_date
								AND 
									route_id IN  ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C','Green-D','Green-E')
						
							) t
				WHERE 
						t.end_date_time >= @from_time
					AND
						t.end_date_time <= @to_time
				GROUP BY
					t.threshold_id
					,t.threshold_name
					,t.threshold_type
					,t.threshold_id

			UNION 

			SELECT 
				t.threshold_id
				,t.threshold_name
				,t.threshold_type
				,1 - SUM(scheduled_threshold_numerator_pax)/SUM(denominator_pax) AS metric_result --changed to scheduled
				,1 - SUM(scheduled_threshold_numerator_trip)/SUM(denominator_trip) AS metric_result_trip --changed to scheduled
				FROM (

						SELECT
							ct.threshold_id
							,ct.threshold_name
							,ct.threshold_type
							,scheduled_threshold_numerator_pax --changed to scheduled
							,denominator_pax
							,scheduled_threshold_numerator_trip --changed to scheduled
							,denominator_trip --added
							,DATEADD(s,hwt.end_time_sec,hwt.service_date) as end_date_time
		
						FROM
							dbo.historical_travel_time_threshold_pax  hwt
							,dbo.config_threshold ct
						WHERE
								ct.threshold_id = hwt.threshold_id
							AND
								(
									(SELECT COUNT(str_val) FROM @from_stop_ids) = 0
								OR
									from_stop_id IN (SELECT str_val FROM @from_stop_ids)
								)	
							AND
								(
									(SELECT COUNT(str_val) FROM @to_stop_ids) = 0
								OR
									to_stop_id IN (SELECT str_val FROM @to_stop_ids)
								)	
							AND
								(
									(SELECT COUNT(int_val) FROM @direction_ids) = 0
								OR
									direction_id IN (SELECT int_val FROM @direction_ids)
								)
							AND
								(
									(SELECT COUNT(str_val) FROM @route_ids) = 0
								OR
									route_id IN (SELECT str_val FROM @route_ids)
								)
							AND
									hwt.service_date = @service_date
							AND 
									route_id IN  ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C','Green-D','Green-E')
						) t
				WHERE 
						t.end_date_time >= @from_time
					AND
						t.end_date_time <= @to_time

			GROUP BY
				t.threshold_id
				,t.threshold_name
				,t.threshold_type
				,t.threshold_id

			UNION 

			SELECT 
				t.threshold_id
				,t.threshold_name
				,t.threshold_type
				,1 - SUM(scheduled_threshold_numerator_pax)/SUM(denominator_pax) AS metric_result --changed to scheduled
				,1 - SUM(scheduled_threshold_numerator_trip)/SUM(denominator_trip) AS metric_result_trip --changed to scheduled
				FROM (

						SELECT
							ct.threshold_id
							,ct.threshold_name
							,ct.threshold_type
							,scheduled_threshold_numerator_pax --changed to scheduled
							,denominator_pax
							,scheduled_threshold_numerator_trip --changed to scheduled
							,denominator_trip --added
							,CASE 
								WHEN hwt.stop_order_flag = 1 THEN DATEADD(s,hwt.actual_departure_time_sec,hwt.service_date) 
								ELSE  DATEADD(s,hwt.actual_arrival_time_sec,hwt.service_date) 
								END AS end_date_time 
		
						FROM
							dbo.historical_schedule_adherence_threshold_pax  hwt
							,dbo.config_threshold ct
						WHERE
								ct.threshold_id = hwt.threshold_id
							AND
								(
									(SELECT COUNT(str_val) FROM @to_stop_ids) = 0
								OR
									stop_id IN (SELECT str_val FROM @to_stop_ids)
								)	
							AND
								(
									(SELECT COUNT(int_val) FROM @direction_ids) = 0
								OR
									direction_id IN (SELECT int_val FROM @direction_ids)
								)
							AND
								(
									(SELECT COUNT(str_val) FROM @route_ids) = 0
								OR
									route_id IN (SELECT str_val FROM @route_ids)
								)
							AND
									hwt.service_date = @service_date
							AND 
									route_id IN  ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C','Green-D','Green-E')
						) t
				WHERE 
						t.end_date_time >= @from_time
					AND
						t.end_date_time <= @to_time

			GROUP BY
				t.threshold_id
				,t.threshold_name
				,t.threshold_type
				,t.threshold_id
			ORDER BY
				threshold_id

		END

		ELSE --if today 
		BEGIN

			INSERT INTO @metricstemp
				SELECT
				ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,1 - SUM(scheduled_threshold_numerator_pax)/SUM(denominator_pax) AS metric_result
				,1 - SUM(scheduled_threshold_numerator_trip)/SUM(denominator_trip) AS metric_result_trip --added
		
		
			FROM
				dbo.today_rt_wait_time_od_threshold_pax hwt
				,dbo.config_threshold ct
			WHERE
					ct.threshold_id = hwt.threshold_id
				AND
					(
						(SELECT COUNT(str_val) FROM @from_stop_ids) = 0
					OR
						from_stop_id IN (SELECT str_val FROM @from_stop_ids)
					)	
				AND
					(
						(SELECT COUNT(str_val) FROM @to_stop_ids) = 0
					OR
						to_stop_id IN (SELECT str_val FROM @to_stop_ids)
					)	
				AND
					(
						(SELECT COUNT(int_val) FROM @direction_ids) = 0
					OR
						direction_id IN (SELECT int_val FROM @direction_ids)
					)
				AND
					(
						(SELECT COUNT(str_val) FROM @route_ids) = 0
					OR
						prev_route_id IN (SELECT str_val FROM @route_ids)
					)
				AND
					(
						(SELECT COUNT(str_val) FROM @route_ids) = 0
					OR
						route_id IN (SELECT str_val FROM @route_ids)
					)
				AND
					DATEADD(s,hwt.end_time_sec,hwt.service_date) >= @from_time
				AND
					DATEADD(s,hwt.end_time_sec,hwt.service_date) <= @to_time
				AND 
					route_id IN  ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C','Green-D','Green-E')
			GROUP BY
				ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,ct.threshold_id

			UNION 

			SELECT
				ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,1 - SUM(scheduled_threshold_numerator_pax)/SUM(denominator_pax) AS metric_result --changed to scheduled
				,1 - SUM(scheduled_threshold_numerator_trip)/SUM(denominator_trip) AS metric_result_trip --changed to scheduled
		
			FROM
				dbo.today_rt_travel_time_threshold_pax hwt
				,dbo.config_threshold ct
			WHERE
					ct.threshold_id = hwt.threshold_id
			AND
					(
						(SELECT COUNT(str_val) FROM @from_stop_ids) = 0
					OR
						from_stop_id IN (SELECT str_val FROM @from_stop_ids)
					)	
				AND
					(
						(SELECT COUNT(str_val) FROM @to_stop_ids) = 0
					OR
						to_stop_id IN (SELECT str_val FROM @to_stop_ids)
					)	
				AND
					(
						(SELECT COUNT(int_val) FROM @direction_ids) = 0
					OR
						direction_id IN (SELECT int_val FROM @direction_ids)
					)
				AND
					(
						(SELECT COUNT(str_val) FROM @route_ids) = 0
					OR
						route_id IN (SELECT str_val FROM @route_ids)
					)

				AND
					DATEADD(s,hwt.end_time_sec,hwt.service_date) >= @from_time
				AND
					DATEADD(s,hwt.end_time_sec,hwt.service_date) <= @to_time
				AND 
					route_id IN  ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C','Green-D','Green-E')
			GROUP BY
				ct.threshold_id
				,ct.threshold_name
				,ct.threshold_type
				,ct.threshold_id

			UNION 

			SELECT
				t.threshold_id
				,t.threshold_name
				,t.threshold_type
				,1 - SUM(t.scheduled_threshold_numerator_pax)/SUM(t.denominator_pax) AS metric_result --changed to scheduled
				,1 - SUM(t.scheduled_threshold_numerator_trip)/SUM(t.denominator_trip) AS metric_result_trip --changed to scheduled
		
			FROM
				(
				SELECT
					ct.threshold_id AS threshold_id
					,ct.threshold_name AS threshold_name
					,ct.threshold_type AS threshold_type
					,hwt.scheduled_threshold_numerator_pax AS scheduled_threshold_numerator_pax--changed to scheduled
					,hwt.denominator_pax AS denominator_pax
					,hwt.scheduled_threshold_numerator_trip AS scheduled_threshold_numerator_trip --changed to scheduled
					,hwt.denominator_trip AS denominator_trip--added
					,CASE 
						WHEN hwt.stop_order_flag = 1 THEN DATEADD(s,hwt.actual_departure_time_sec,hwt.service_date) 
						ELSE  DATEADD(s,hwt.actual_arrival_time_sec,hwt.service_date) 
						END AS end_date_time 
				FROM
					dbo.today_rt_schedule_adherence_threshold_pax hwt
					,dbo.config_threshold ct
				WHERE
					ct.threshold_id = hwt.threshold_id
				AND
					(
						(SELECT COUNT(str_val) FROM @to_stop_ids) = 0
					OR
						stop_id IN (SELECT str_val FROM @to_stop_ids)
					)	
				AND
					(
						(SELECT COUNT(int_val) FROM @direction_ids) = 0
					OR
						direction_id IN (SELECT int_val FROM @direction_ids)
					)
				AND
					(
						(SELECT COUNT(str_val) FROM @route_ids) = 0
					OR
						route_id IN (SELECT str_val FROM @route_ids)
					)
				AND 
					route_id IN  ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C','Green-D','Green-E')
					) t
				WHERE
					t.end_date_time >= @from_time -- need to fix for schedule adherence arrival/departure -- subquery
				AND
					t.end_date_time <= @to_time -- need to fix for schedule adherence arrival/departure
			GROUP BY
				t.threshold_id
				,t.threshold_name
				,t.threshold_type
				,t.threshold_id


			ORDER BY
				threshold_id

		END
	END --if a timespan is less than 6 hours and routes are only subway/light rail, then do the processing, if not return empty set

	SELECT
		threshold_id
		,threshold_name
		,threshold_type
		,metric_result
		,metric_result_trip
	FROM @metricstemp

	
END








GO

