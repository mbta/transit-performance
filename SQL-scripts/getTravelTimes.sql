
---run this script in the transit-performance database
--USE transit_performance
--GO


--This Procedure is called by the traveltimes API call. It selects travel times for a particular from_stop and to_stop pair (and optionally route)
-- and time period.

IF OBJECT_ID('getTravelTimes') IS NOT NULL
	DROP PROCEDURE dbo.getTravelTimes
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE dbo.getTravelTimes
	@from_stop_id VARCHAR(255)
	,@to_stop_id VARCHAR(255)
	,@from_time DATETIME
	,@to_time DATETIME
	,@route_id VARCHAR(255)

AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @traveltimestemp AS TABLE
	(
		route_id					VARCHAR(255)
		,direction_id				INT
		,start_time					DATETIME
		,end_time					DATETIME
		,travel_time_sec			INT
		,benchmark_travel_time_sec	INT
		,threshold_flag_1			VARCHAR(255)
		,threshold_flag_2			VARCHAR(255)
		,threshold_flag_3			VARCHAR(255)
	)

	IF (DATEDIFF(D,@from_time,@to_time) <= 7) --only return results for 7-day span
	
	BEGIN --if a timespan is less than 7 days, then do the processing, if not return empty set

		INSERT INTO @traveltimestemp
			SELECT --selects travel times from today, if the from_time and to_time are today
				htt.route_id
				,htt.direction_id
				,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
				,DATEADD(s,htt.end_time_sec,htt.service_date) AS end_time
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,CASE
					WHEN th.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					WHEN th.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					WHEN th.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					ELSE NULL
				END AS threshold_flag_1
				,CASE
					WHEN th2.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					WHEN th2.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					WHEN th2.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					ELSE NULL
				END AS threshold_flag_2
				,CASE
					WHEN th3.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					WHEN th3.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					WHEN th3.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					ELSE NULL
				END AS threshold_flag_3
			FROM	dbo.today_rt_travel_time_disaggregate htt
					,dbo.config_threshold th
					,dbo.config_threshold_calculation thc
					,dbo.config_threshold th2
					,dbo.config_threshold_calculation thc2
					,dbo.config_threshold th3
					,dbo.config_threshold_calculation thc3
			WHERE
					from_stop_id = @from_stop_id
				AND 
					to_stop_id = @to_stop_id
				AND 
					(route_id = @route_id OR @route_id IS NULL)
				AND 
					DATEADD(s,end_time_sec,service_date) >= @from_time
				AND 
					DATEADD(s,end_time_sec,service_date) <= @to_time
				AND 
					th.threshold_id = thc.threshold_id
				AND 
					th.threshold_id = 'threshold_id_04'
				AND 
					th2.threshold_id = thc2.threshold_id
				AND 
					th2.threshold_id = 'threshold_id_05'
				AND 
					th3.threshold_id = thc3.threshold_id
				AND 
					th3.threshold_id = 'threshold_id_06'
				AND
					(htt.route_type = 0 OR htt.route_type = 1)
			GROUP BY
				htt.service_date
				,htt.from_stop_id
				,htt.to_stop_id
				,htt.direction_id
				,htt.route_id
				,htt.start_time_sec
				,htt.end_time_sec
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,th.min_max_equal
				,th2.min_max_equal
				,th3.min_max_equal

			UNION

			SELECT --selects travel times from days in the past, if the from_time and to_time are not today
				htt.route_id
				,htt.direction_id
				,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
				,DATEADD(s,htt.end_time_sec,htt.service_date) AS end_time
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,CASE
					WHEN th.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					WHEN th.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					WHEN th.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					ELSE NULL
				END AS threshold_flag_1
				,CASE
					WHEN th2.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					WHEN th2.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					WHEN th2.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					ELSE NULL
				END AS threshold_flag_2
				,CASE
					WHEN th3.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					WHEN th3.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					WHEN th3.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					ELSE NULL
				END AS threshold_flag_3
			FROM	dbo.historical_travel_time_disaggregate htt
					LEFT JOIN gtfs.routes r
						ON
							htt.route_id = r.route_id
					,dbo.config_threshold th
					,dbo.config_threshold_calculation thc
					,dbo.config_threshold th2
					,dbo.config_threshold_calculation thc2
					,dbo.config_threshold th3
					,dbo.config_threshold_calculation thc3
			WHERE
					from_stop_id = @from_stop_id
				AND 
					to_stop_id = @to_stop_id
				AND 
					(htt.route_id = @route_id OR @route_id IS NULL)
				AND 
					DATEADD(s,end_time_sec,service_date) >= @from_time
				AND 
					DATEADD(s,end_time_sec,service_date) <= @to_time
				AND 
					th.threshold_id = thc.threshold_id
				AND 
					th.threshold_id = 'threshold_id_04'
				AND 
					th2.threshold_id = thc2.threshold_id
				AND 
					th2.threshold_id = 'threshold_id_05'
				AND 
					th3.threshold_id = thc3.threshold_id
				AND 
					th3.threshold_id = 'threshold_id_06'
				AND 
					(ISNULL(htt.route_type,r.route_type) IN (0,1))

			GROUP BY
				htt.service_date
				,htt.from_stop_id
				,htt.to_stop_id
				,htt.direction_id
				,htt.route_id
				,htt.start_time_sec
				,htt.end_time_sec
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,th.min_max_equal
				,th2.min_max_equal
				,th3.min_max_equal

			UNION

			SELECT --selects travel times from today, if the from_time and to_time are today
				htt.route_id
				,htt.direction_id
				,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
				,DATEADD(s,htt.end_time_sec,htt.service_date) AS end_time
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,CASE
					WHEN th.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					WHEN th.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					WHEN th.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_04'
					ELSE NULL
				END AS threshold_flag_1
				,CASE
					WHEN th2.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					WHEN th2.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					WHEN th2.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_05'
					ELSE NULL
				END AS threshold_flag_2
				,CASE
					WHEN th3.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					WHEN th3.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					WHEN th3.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_06'
					ELSE NULL
				END AS threshold_flag_3
			FROM	dbo.today_rt_travel_time_disaggregate htt
					,dbo.config_threshold th
					,dbo.config_threshold_calculation thc
					,dbo.config_threshold th2
					,dbo.config_threshold_calculation thc2
					,dbo.config_threshold th3
					,dbo.config_threshold_calculation thc3
			WHERE
					from_stop_id = @from_stop_id
				AND 
					to_stop_id = @to_stop_id
				AND 
					(route_id = @route_id OR @route_id IS NULL)
				AND 
					DATEADD(s,end_time_sec,service_date) >= @from_time
				AND 
					DATEADD(s,end_time_sec,service_date) <= @to_time
				AND 
					th.threshold_id = thc.threshold_id
				AND 
					th.threshold_id = 'threshold_id_04'
				AND 
					th2.threshold_id = thc2.threshold_id
				AND 
					th2.threshold_id = 'threshold_id_05'
				AND 
					th3.threshold_id = thc3.threshold_id
				AND 
					th3.threshold_id = 'threshold_id_06'
				AND 
					htt.route_type = 2
			GROUP BY
				htt.service_date
				,htt.from_stop_id
				,htt.to_stop_id
				,htt.direction_id
				,htt.route_id
				,htt.start_time_sec
				,htt.end_time_sec
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,th.min_max_equal
				,th2.min_max_equal
				,th3.min_max_equal

			UNION
			--commuter rail thresholds for travel time

			SELECT --selects travel times from days in the past, if the from_time and to_time are not today
				htt.route_id
				,htt.direction_id
				,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
				,DATEADD(s,htt.end_time_sec,htt.service_date) AS end_time
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,CASE
					WHEN th.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_10'
					WHEN th.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_10'
					WHEN th.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to)
						THEN 'threshold_id_10'
					ELSE NULL
				END AS threshold_flag_1
				,CASE
					WHEN th2.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_11'
					WHEN th2.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to) 
						THEN 'threshold_id_11'
					WHEN th2.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to)
						THEN 'threshold_id_11'
					ELSE NULL
				END AS threshold_flag_2
				,CASE
					WHEN th3.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_12'
					WHEN th3.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_12'
					WHEN th3.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to)
						THEN 'threshold_id_12'
					ELSE NULL
				END AS threshold_flag_3
			FROM	dbo.historical_travel_time_disaggregate htt
					,dbo.config_threshold th
					,dbo.config_threshold_calculation thc
					,dbo.config_threshold th2
					,dbo.config_threshold_calculation thc2
					,dbo.config_threshold th3
					,dbo.config_threshold_calculation thc3
			WHERE
					from_stop_id = @from_stop_id
				AND 
					to_stop_id = @to_stop_id
				AND 
					(route_id = @route_id OR @route_id IS NULL)
				AND 
					DATEADD(s,end_time_sec,service_date) >= @from_time
				AND 
					DATEADD(s,end_time_sec,service_date) <= @to_time
				AND 
					th.threshold_id = thc.threshold_id
				AND 
					th.threshold_id = 'threshold_id_10'
				AND 
					th2.threshold_id = thc2.threshold_id
				AND 
					th2.threshold_id = 'threshold_id_11'
				AND	
					th3.threshold_id = thc3.threshold_id
				AND 
					th3.threshold_id = 'threshold_id_12'
				AND 
					htt.route_type = 2

			GROUP BY
				htt.service_date
				,htt.from_stop_id
				,htt.to_stop_id
				,htt.direction_id
				,htt.route_id
				,htt.start_time_sec
				,htt.end_time_sec
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,th.min_max_equal
				,th2.min_max_equal
				,th3.min_max_equal
			UNION

			SELECT --selects travel times from today, if the from_time and to_time are today
				htt.route_id
				,htt.direction_id
				,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
				,DATEADD(s,htt.end_time_sec,htt.service_date) AS end_time
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,CASE
					WHEN th.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to) THEN 'threshold_id_10'
					WHEN th.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to) THEN 'threshold_id_10'
					WHEN th.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc.multiply_by + thc.add_to) THEN 'threshold_id_10'
					ELSE NULL
				END AS threshold_flag_1
				,CASE
					WHEN th2.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to) THEN 'threshold_id_11'
					WHEN th2.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to) THEN 'threshold_id_11'
					WHEN th2.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc2.multiply_by + thc2.add_to) THEN 'threshold_id_11'
					ELSE NULL
				END AS threshold_flag_2
				,CASE
					WHEN th3.min_max_equal = 'min' AND htt.travel_time_sec > MIN(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to) THEN 'threshold_id_12'
					WHEN th3.min_max_equal = 'max' AND htt.travel_time_sec > MAX(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to) THEN 'threshold_id_12'
					WHEN th3.min_max_equal = 'equal' AND htt.travel_time_sec > AVG(htt.benchmark_travel_time_sec * thc3.multiply_by + thc3.add_to) THEN 'threshold_id_12'
					ELSE NULL
				END AS threshold_flag_3

			FROM	dbo.today_rt_travel_time_disaggregate htt
					,dbo.config_threshold th
					,dbo.config_threshold_calculation thc
					,dbo.config_threshold th2
					,dbo.config_threshold_calculation thc2
					,dbo.config_threshold th3
					,dbo.config_threshold_calculation thc3
			WHERE
					from_stop_id = @from_stop_id
				AND 
					to_stop_id = @to_stop_id
				AND 
					(route_id = @route_id OR @route_id IS NULL)
				AND 
					DATEADD(s,end_time_sec,service_date) >= @from_time
				AND 
					DATEADD(s,end_time_sec,service_date) <= @to_time
				AND 
					th.threshold_id = thc.threshold_id
				AND 
					th.threshold_id = 'threshold_id_10'
				AND 
					th2.threshold_id = thc2.threshold_id
				AND 
					th2.threshold_id = 'threshold_id_11'
				AND 
					th3.threshold_id = thc3.threshold_id
				AND 
					th3.threshold_id = 'threshold_id_12'
			GROUP BY
				htt.service_date
				,htt.from_stop_id
				,htt.to_stop_id
				,htt.direction_id
				,htt.route_id
				,htt.start_time_sec
				,htt.end_time_sec
				,htt.travel_time_sec
				,htt.benchmark_travel_time_sec
				,th.min_max_equal
				,th2.min_max_equal
				,th3.min_max_equal

	END--if a timespan is less than 7 days, then do the processing, if not return empty set

	SELECT
		t.route_id
		,direction_id
		,start_time
		,end_time
		,travel_time_sec
		,benchmark_travel_time_sec
		,threshold_flag_1
		,threshold_flag_2
		,threshold_flag_3
	FROM @traveltimestemp t
	JOIN gtfs.routes r
	ON
		t.route_id = r.route_id
	WHERE
		r.route_type <> 2 --do not return results for Commuter Rail
	ORDER BY end_time 

END









GO