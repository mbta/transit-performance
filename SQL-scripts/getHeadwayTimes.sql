
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('getHeadwayTimes') IS NOT NULL
DROP PROCEDURE dbo.getHeadwayTimes
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE dbo.getHeadwayTimes

--Script Version: Master - 1.1.0.0

--This Procedure is called by the headway API call. It selects headways  for a particular stop (and optionally to_stop or route + direction) and time period.

	@stop_id		VARCHAR(255)
	,@to_stop_id	VARCHAR(255) = NULL
	,@route_id		VARCHAR(255) = NULL
	,@direction_id	INT = NULL
	,@from_time		DATETIME
	,@to_time		DATETIME
	

AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @headwaytimestemp AS TABLE
	(
		route_id					VARCHAR(255)
		,prev_route_id				VARCHAR(255)
		,direction_id				INT
		,start_time					DATETIME
		,end_time					DATETIME
		,headway_time_sec			INT
		,benchmark_headway_time_sec	INT
		,threshold_flag_1			VARCHAR(255)
		,threshold_flag_2			VARCHAR(255)
		,threshold_flag_3			VARCHAR(255)
	)
	
	IF (DATEDIFF(D,@from_time,@to_time) <= 7) --only return results for 7-day span
	
	BEGIN --if a timespan is less than 7 days, then do the processing, if not return empty set

		DECLARE @service_date DATE
		SET @service_date = dbo.fnConvertDateTimeToServiceDate(@from_time)

		DECLARE @today_service_date DATE
		SET @today_service_date = dbo.fnConvertDateTimeToServiceDate(GETDATE())

		IF @service_date = @today_service_date
		BEGIN --if service_date is today then use today_rt tables

			IF @to_stop_id IS NULL --the to_stop is not defined so use sr (stop and route) tables instead of od (origin-destination)
			BEGIN

				IF @route_id IS NULL --the route is not defined so use headway_time_sr_all_disaggregate
				BEGIN
					
					INSERT INTO @headwaytimestemp
						
						SELECT --selects headways from today, if the from_time and to_time are today
							htt.route_id
							,htt.prev_route_id
							,htt.direction_id
							,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
							,DATEADD(s,htt.end_time_sec,service_date) AS end_time
							,htt.headway_time_sec
							,htt.benchmark_headway_time_sec
							,CASE
								WHEN th.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								ELSE NULL
							END AS threshold_flag_1
							,CASE
								WHEN th2.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								ELSE NULL
							END AS threshold_flag_2
							,CASE
								WHEN th3.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								ELSE NULL
							END AS threshold_flag_3
						FROM	dbo.today_rt_headway_time_sr_all_disaggregate htt
								,dbo.config_threshold th
								,dbo.config_threshold_calculation thc
								,dbo.config_threshold th2
								,dbo.config_threshold_calculation thc2
								,dbo.config_threshold th3
								,dbo.config_threshold_calculation thc3
						WHERE
								stop_id = @stop_id
							AND 
								(direction_id = @direction_id OR @direction_id IS NULL)
							AND 
								DATEADD(s,end_time_sec,service_date) >= @from_time
							AND 
								DATEADD(s,end_time_sec,service_date) <= @to_time
							AND 
								th.threshold_id = thc.threshold_id
							AND 
								th.threshold_id = 'threshold_id_01'
							AND 
								th2.threshold_id = thc2.threshold_id
							AND 
								th2.threshold_id = 'threshold_id_02'
							AND 
								th3.threshold_id = thc3.threshold_id
							AND 
								th3.threshold_id = 'threshold_id_03'
						GROUP BY
							route_id
							,prev_route_id
							,direction_id
							,service_date
							,start_time_sec
							,end_time_sec
							,headway_time_sec
							,benchmark_headway_time_sec
							,th.min_max_equal
							,th2.min_max_equal
							,th3.min_max_equal

				END
				
				ELSE
				--route_id is not null and use headway_time_sr_same
				BEGIN
					
					INSERT INTO @headwaytimestemp
						
						SELECT --selects headways from today, if the from_time and to_time are today
							htt.route_id
							,htt.prev_route_id
							,htt.direction_id
							,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
							,DATEADD(s,htt.end_time_sec,service_date) AS end_time
							,htt.headway_time_sec
							,htt.benchmark_headway_time_sec
							,CASE
								WHEN th.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								ELSE NULL
							END AS threshold_flag_1
							,CASE
								WHEN th2.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								ELSE NULL
							END AS threshold_flag_2
							,CASE
								WHEN th3.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								ELSE NULL
							END AS threshold_flag_3
						FROM	dbo.today_rt_headway_time_sr_same_disaggregate htt
								,dbo.config_threshold th
								,dbo.config_threshold_calculation thc
								,dbo.config_threshold th2
								,dbo.config_threshold_calculation thc2
								,dbo.config_threshold th3
								,dbo.config_threshold_calculation thc3
						WHERE
								route_id = @route_id
							AND 
								stop_id = @stop_id
							AND 
								(direction_id = @direction_id OR @direction_id IS NULL)
							AND 
								DATEADD(s,end_time_sec,service_date) >= @from_time
							AND 
								DATEADD(s,end_time_sec,service_date) <= @to_time
							AND 
								th.threshold_id = thc.threshold_id
							AND 
								th.threshold_id = 'threshold_id_01'
							AND 
								th2.threshold_id = thc2.threshold_id
							AND 
								th2.threshold_id = 'threshold_id_02'
							AND 
								th3.threshold_id = thc3.threshold_id
							AND 
								th3.threshold_id = 'threshold_id_03'
						GROUP BY
							route_id
							,prev_route_id
							,direction_id
							,service_date
							,start_time_sec
							,end_time_sec
							,headway_time_sec
							,benchmark_headway_time_sec
							,th.min_max_equal
							,th2.min_max_equal
							,th3.min_max_equal
				
				END
			
			END

			ELSE --to_stop_id is given, so use od
			BEGIN
				
				IF @route_id IS NULL --route_id cannot be given if stop_id is given
				BEGIN
					
					INSERT INTO @headwaytimestemp
						
						SELECT --selects headways from today, if the from_time and to_time are today
							htt.route_id
							,htt.prev_route_id
							,htt.direction_id
							,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
							,DATEADD(s,htt.end_time_sec,service_date) AS end_time
							,htt.headway_time_sec
							,htt.benchmark_headway_time_sec
							,CASE
								WHEN th.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								ELSE NULL
							END AS threshold_flag_1
							,CASE
								WHEN th2.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								ELSE NULL
							END AS threshold_flag_2
							,CASE
								WHEN th3.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								ELSE NULL
							END AS threshold_flag_3
						FROM	dbo.today_rt_headway_time_od_disaggregate htt
								,dbo.config_threshold th
								,dbo.config_threshold_calculation thc
								,dbo.config_threshold th2
								,dbo.config_threshold_calculation thc2
								,dbo.config_threshold th3
								,dbo.config_threshold_calculation thc3
						WHERE
								stop_id = @stop_id
							AND 
								to_stop_id = @to_stop_id
							AND 
								(direction_id = @direction_id OR @direction_id IS NULL)
							AND 
								DATEADD(s,end_time_sec,service_date) >= @from_time
							AND 
								DATEADD(s,end_time_sec,service_date) <= @to_time
							AND 
								th.threshold_id = thc.threshold_id
							AND 
								th.threshold_id = 'threshold_id_01'
							AND 
								th2.threshold_id = thc2.threshold_id
							AND 
								th2.threshold_id = 'threshold_id_02'
							AND 
								th3.threshold_id = thc3.threshold_id
							AND 
								th3.threshold_id = 'threshold_id_03'

						GROUP BY
							route_id
							,prev_route_id
							,direction_id
							,service_date
							,start_time_sec
							,end_time_sec
							,headway_time_sec
							,benchmark_headway_time_sec
							,th.min_max_equal
							,th2.min_max_equal
							,th3.min_max_equal

				END
			
			END
		
		END--if service date is today, then do processing, if not then look in historical data
		
		ELSE --service date is not today so use historical data
		BEGIN
			
			IF @to_stop_id IS NULL --the to_stop is not defined so use sr (stop and route) tables instead of od (origin-destination)
			BEGIN

				IF @route_id IS NULL --the route is not defined so use headway_time_sr_all_disaggregate
				BEGIN
					
					INSERT INTO @headwaytimestemp
						
						SELECT --selects headways from days in the past, if the from_time and to_time are not today
							htt.route_id
							,htt.prev_route_id
							,htt.direction_id
							,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
							,DATEADD(s,htt.end_time_sec,service_date) AS end_time
							,htt.headway_time_sec
							,htt.benchmark_headway_time_sec
							,CASE
								WHEN th.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								ELSE NULL
							END AS threshold_flag_1
							,CASE
								WHEN th2.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								ELSE NULL
							END AS threshold_flag_2
							,CASE
								WHEN th3.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								ELSE NULL
							END AS threshold_flag_3
						FROM	dbo.historical_headway_time_sr_all_disaggregate htt
								,dbo.config_threshold th
								,dbo.config_threshold_calculation thc
								,dbo.config_threshold th2
								,dbo.config_threshold_calculation thc2
								,dbo.config_threshold th3
								,dbo.config_threshold_calculation thc3
						WHERE
								stop_id = @stop_id
							AND 
								(direction_id = @direction_id OR @direction_id IS NULL)
							AND 
								DATEADD(s,end_time_sec,service_date) >= @from_time
							AND 
								DATEADD(s,end_time_sec,service_date) <= @to_time
							AND 
								th.threshold_id = thc.threshold_id
							AND 
								th.threshold_id = 'threshold_id_01'
							AND 
								th2.threshold_id = thc2.threshold_id
							AND 
								th2.threshold_id = 'threshold_id_02'
							AND 
								th3.threshold_id = thc3.threshold_id
							AND 
								th3.threshold_id = 'threshold_id_03'
						GROUP BY
							route_id
							,prev_route_id
							,direction_id
							,service_date
							,start_time_sec
							,end_time_sec
							,headway_time_sec
							,benchmark_headway_time_sec
							,th.min_max_equal
							,th2.min_max_equal
							,th3.min_max_equal

				END
				
				ELSE
				--route_id is not null and use headway_time_sr_same
				BEGIN
					
					INSERT INTO @headwaytimestemp
						
						SELECT --selects headways from days in the past, if the from_time and to_time are not today
							htt.route_id
							,htt.prev_route_id
							,htt.direction_id
							,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
							,DATEADD(s,htt.end_time_sec,service_date) AS end_time
							,htt.headway_time_sec
							,htt.benchmark_headway_time_sec
							,CASE
								WHEN th.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								ELSE NULL
							END AS threshold_flag_1
							,CASE
								WHEN th2.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								ELSE NULL
							END AS threshold_flag_2
							,CASE
								WHEN th3.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								ELSE NULL
							END AS threshold_flag_3
						FROM	dbo.historical_headway_time_sr_same_disaggregate htt
								,dbo.config_threshold th
								,dbo.config_threshold_calculation thc
								,dbo.config_threshold th2
								,dbo.config_threshold_calculation thc2
								,dbo.config_threshold th3
								,dbo.config_threshold_calculation thc3
						WHERE
								route_id = @route_id
							AND 
								stop_id = @stop_id
							AND 
								(direction_id = @direction_id OR @direction_id IS NULL)
							AND 
								DATEADD(s,end_time_sec,service_date) >= @from_time
							AND 
								DATEADD(s,end_time_sec,service_date) <= @to_time
							AND 
								th.threshold_id = thc.threshold_id
							AND 
								th.threshold_id = 'threshold_id_01'
							AND 
								th2.threshold_id = thc2.threshold_id
							AND 
								th2.threshold_id = 'threshold_id_02'
							AND 
								th3.threshold_id = thc3.threshold_id
							AND 
								th3.threshold_id = 'threshold_id_03'
						GROUP BY
							route_id
							,prev_route_id
							,direction_id
							,service_date
							,start_time_sec
							,end_time_sec
							,headway_time_sec
							,benchmark_headway_time_sec
							,th.min_max_equal
							,th2.min_max_equal
							,th3.min_max_equal
				
				END
			
			END
			
			ELSE --to_stop_id is given, so use od
			BEGIN
				
				IF @route_id IS NULL --route_id cannot be given if stop_id is given
				BEGIN
					
					INSERT INTO @headwaytimestemp
						
						SELECT --selects headways from days in the past, if the from_time and to_time are not today
							htt.route_id
							,htt.prev_route_id
							,htt.direction_id
							,DATEADD(s,htt.start_time_sec,htt.service_date) AS start_time
							,DATEADD(s,htt.end_time_sec,service_date) AS end_time
							,htt.headway_time_sec
							,htt.benchmark_headway_time_sec
							,CASE
								WHEN th.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								WHEN th.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc.multiply_by + thc.add_to)
									THEN 'threshold_id_01'
								ELSE NULL
							END AS threshold_flag_1
							,CASE
								WHEN th2.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								WHEN th2.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc2.multiply_by + thc2.add_to)
									THEN 'threshold_id_02'
								ELSE NULL
							END AS threshold_flag_2
							,CASE
								WHEN th3.min_max_equal = 'min' AND htt.headway_time_sec > MIN(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'max' AND htt.headway_time_sec > MAX(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								WHEN th3.min_max_equal = 'equal' AND htt.headway_time_sec > AVG(htt.benchmark_headway_time_sec * thc3.multiply_by + thc2.add_to)
									THEN 'threshold_id_03'
								ELSE NULL
							END AS threshold_flag_3
						FROM	dbo.historical_headway_time_od_disaggregate htt
								,dbo.config_threshold th
								,dbo.config_threshold_calculation thc
								,dbo.config_threshold th2
								,dbo.config_threshold_calculation thc2
								,dbo.config_threshold th3
								,dbo.config_threshold_calculation thc3
						WHERE
								stop_id = @stop_id
							AND 
								to_stop_id = @to_stop_id
							AND 
								(direction_id = @direction_id OR @direction_id IS NULL)
							AND 
								DATEADD(s,end_time_sec,service_date) >= @from_time
							AND 
								DATEADD(s,end_time_sec,service_date) <= @to_time
							AND 
								th.threshold_id = thc.threshold_id
							AND 
								th.threshold_id = 'threshold_id_01'
							AND 
								th2.threshold_id = thc2.threshold_id
							AND 
								th2.threshold_id = 'threshold_id_02'
							AND 
								th3.threshold_id = thc3.threshold_id
							AND 
								th3.threshold_id = 'threshold_id_03'
						GROUP BY
							route_id
							,prev_route_id
							,direction_id
							,service_date
							,start_time_sec
							,end_time_sec
							,headway_time_sec
							,benchmark_headway_time_sec
							,th.min_max_equal
							,th2.min_max_equal
							,th3.min_max_equal
				
				END
			
			END
		
		END--if service date is today, then do processing, if not return empty set
	
	END--if a timespan is less than 7 days, then do the processing, if not return empty set
	
	SELECT
		h.route_id
		,prev_route_id
		,direction_id
		,start_time
		,end_time
		,headway_time_sec
		,benchmark_headway_time_sec
		,threshold_flag_1
		,threshold_flag_2
		,threshold_flag_3
	FROM @headwaytimestemp h
	JOIN gtfs.routes r
	ON
		h.route_id = r.route_id
	WHERE
		r.route_type <> 2 --do not return results for Commuter Rail
	ORDER BY end_time

END








GO