

--Run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.ReviseHistoricalMetrics','P') IS NOT NULL
	DROP PROCEDURE dbo.ReviseHistoricalMetrics

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ReviseHistoricalMetrics

--Script Version: Master - 1.0.0.0

--This procedure revises metric results in the historical_metrics table

    @service_date 		DATE
	,@route_id			VARCHAR(255)
    ,@disruption_type	VARCHAR(255)
	,@start_time_sec	INT
	,@end_time_sec		INT	
	,@from_stop_order_0	INT
	,@to_stop_order_0 	INT
	,@from_stop_order_1	INT
	,@to_stop_order_1 	INT	
	,@ashmont_flag		BIT

AS


BEGIN
	SET NOCOUNT ON;


---------------------------------------------------------------------------------
	--CONFIGURE INPUTS
---------------------------------------------------------------------------------


--type of disruption (configures the ODs that are selected)	
DECLARE @disruption_type_process VARCHAR(255)
	SET @disruption_type_process = @disruption_type
			--'reset'	-- Undo revisions
			--'1' 		-- Line disruption (selects all ODs along the given route)
			--'2' 		-- Segment disruption (selects all ODs within segment, and ODs passing into or through segment)
			--'3' 		-- Stop disruption (selects all ODs pertaining to stop(s))
	
	
--service_date of disruption	
DECLARE @service_date_process DATE
	SET @service_date_process = @service_date

	
--start of disruption seconds after midnight 
DECLARE @start_time_sec_process INT
	SET @start_time_sec_process = @start_time_sec

	
--end of disruption seconds after midnight
DECLARE @end_time_sec_process INT
	SET @end_time_sec_process = @end_time_sec

	
 --route_id of disruption	
DECLARE @route_id_process VARCHAR(255)
	SET @route_id_process = @route_id

 --from_stop_order of disruption for direction_id = 0, as identified from gtfs.route_direction_stop
DECLARE @from_stop_order_process_0 INT
	SET @from_stop_order_process_0 = @from_stop_order_0	
	
 --to_stop_order of disruption for direction_id = 0, as identified from gtfs.route_direction_stop
DECLARE @to_stop_order_process_0 INT
	SET @to_stop_order_process_0 = @to_stop_order_0
	
 --from_stop_order of disruption for direction_id = 1, as identified from gtfs.route_direction_stop
DECLARE @from_stop_order_process_1 INT
	SET @from_stop_order_process_1 = @from_stop_order_1
		
 --to_stop_order of disruption for direction_id = 1, as identified from gtfs.route_direction_stop
DECLARE @to_stop_order_process_1 INT
	SET @to_stop_order_process_1 = @to_stop_order_1

	
-- Ashmont branch flag for disruption types 2 and 3. This flag is necessary bc stop sequencing in the gtfs.route_direction_stop places the Braintree branch sequentially between the Ashmont branch and the trunk
DECLARE @ashmont_flag_process BIT
	SET @ashmont_flag_process = @ashmont_flag  -- 1 if disruption affects Ashmont branch only (i.e. Braintree branch is unaffected and should not be revised), 0 otherwise		
	

--threshold_ids to revise
DECLARE @threshold_ids AS TABLE 
	(
	threshold_id	VARCHAR(255)
	)
INSERT INTO
	@threshold_ids
VALUES 
	('threshold_id_01')
	--,('threshold_id_02')
	--,('threshold_id_03')
	--,('threshold_id_04')
	--,('threshold_id_05')
	--,('threshold_id_06')	
	

DECLARE @day_of_the_week VARCHAR(255);
SET @day_of_the_week =
(
	SELECT
		DATENAME(dw,@service_date_process)
);

DECLARE @day_type_id VARCHAR(255);
SET @day_type_id =
(
	SELECT
		day_type_id
	FROM 
		dbo.config_day_type_dow
	WHERE
		day_of_the_week = @day_of_the_week
)


--------------------------------------------------------------------------------------------------------------------
IF 
(
	@disruption_type_process = 'reset'
	AND
	(
		SELECT 
			COUNT(*)
		FROM dbo.revise_historical_metrics_log
		WHERE 
			service_date = @service_date_process
			AND route_id = @route_id_process
			AND threshold_id IN (SELECT DISTINCT threshold_id FROM @threshold_ids)
	) 
	> 0
)

BEGIN 

UPDATE dbo.historical_metrics
	SET 
		metric_result = l.original_metric_result
		,numerator_pax = l.original_numerator_pax
		,denominator_pax = l.original_denominator_pax
	FROM dbo.historical_metrics hm
	RIGHT JOIN
	(
	SELECT
			service_date
			,route_id
			,threshold_id
			,time_period_type
			,original_metric_result
			,original_numerator_pax
			,original_denominator_pax
		FROM dbo.revise_historical_metrics_log 

		WHERE 
			service_date = @service_date_process
			AND route_id = @route_id_process
			AND threshold_id IN (SELECT DISTINCT threshold_id FROM @threshold_ids)
			AND revise_datetime = 
				(	SELECT 
						MIN(a.revise_datetime)
					FROM 
						dbo.revise_historical_metrics_log a
					WHERE
						a.service_date = @service_date_process
						AND a.route_id = @route_id_process
						AND a.threshold_id IN (SELECT DISTINCT threshold_id FROM @threshold_ids)
				)		
		) l
	
	ON
		hm.service_date = l.service_date
		AND hm.route_id = l.route_id
		AND hm.threshold_id = l.threshold_id
		AND hm.time_period_type = l.time_period_type

/*
	IF OBJECT_ID('dbo.revise_historical_metrics_log','U') IS NOT NULL
		DROP TABLE dbo.revise_historical_metrics_log

	CREATE TABLE dbo.revise_historical_metrics_log
	(
		revise_datetime				DATETIME2
		,service_date				VARCHAR(255)
		,route_id					VARCHAR(255)
		,disruption_type			VARCHAR(255)
		,start_time_sec				INT
		,end_time_sec				INT	
		,from_stop_order_0			INT
		,to_stop_order_0 			INT
		,from_stop_order_1			INT
		,to_stop_order_1 			INT	
		,ashmont_flag				BIT
		,threshold_id				VARCHAR(255)
		,time_period_type			VARCHAR(255)
		,original_metric_result		FLOAT
		,original_numerator_pax		FLOAT
		,original_denominator_pax	FLOAT
		,revised_metric_result		FLOAT
		,revised_numerator_pax		FLOAT
		,revised_denominator_pax	FLOAT
	)
*/
	
	INSERT INTO dbo.revise_historical_metrics_log
	(
		revise_datetime
		,service_date
		,route_id
		,disruption_type
		,threshold_id
		,time_period_type
		,original_metric_result
		,original_numerator_pax
		,original_denominator_pax
		,revised_metric_result
		,revised_numerator_pax
		,revised_denominator_pax
	)
		
	SELECT	
		SYSDATETIME()
		,service_date
		,route_id
		,CASE
			WHEN @disruption_type_process = '1' THEN 'disruption_type_01'
			WHEN @disruption_type_process = '2' THEN 'disruption_type_02'
			WHEN @disruption_type_process = '3' THEN 'disruption_type_03'
			ELSE @disruption_type_process
			END
		,threshold_id
		,time_period_type
		,metric_result
		,numerator_pax
		,denominator_pax
		,metric_result
		,numerator_pax
		,denominator_pax		
		
	FROM
		dbo.historical_metrics

	WHERE 
		service_date = @service_date_process
		AND route_id = @route_id_process
		AND threshold_id IN (SELECT DISTINCT threshold_id FROM @threshold_ids)

END
---------------------------------------------------------------------------------
	--Passenger Arrival Estimate by OD and Time Slice
---------------------------------------------------------------------------------
		

IF OBJECT_ID('tempdb..#passenger_arrival_estimate_slice','U') IS NOT NULL
	DROP TABLE #passenger_arrival_estimate_slice

CREATE TABLE #passenger_arrival_estimate_slice
(
	service_date				VARCHAR(255)
	,route_id					VARCHAR(255)
	,from_stop_id				VARCHAR(255)
	,to_stop_id					VARCHAR(255)
	,time_slice_id				VARCHAR(255)
	,time_slice_start_sec		INT
	,time_slice_end_sec			INT
	,passenger_arrival_rate 	FLOAT
	,passenger_arrival_estimate	FLOAT
)

--------------------------------------------------------------------------------------------------------------------
IF @disruption_type_process = '1'

BEGIN 
	
	INSERT INTO #passenger_arrival_estimate_slice
	(
		service_date
		,route_id
		,from_stop_id
		,to_stop_id
		,time_slice_id
		,time_slice_start_sec
		,time_slice_end_sec
		,passenger_arrival_rate
		,passenger_arrival_estimate
	)

	SELECT DISTINCT
		@service_date_process
		,rds1.route_id
		,rds1.stop_id AS from_stop_id
		,rds2.stop_id AS to_stop_id
		,par.time_slice_id
		,ts.time_slice_start_sec
		,ts.time_slice_end_sec
		,par.passenger_arrival_rate
		,CASE
			WHEN 
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70154_70156'
					,'70154_70158'
					,'70154_70200'
					,'70156_70158'
					,'70156_70200'
					,'70157_70155'
					,'70158_70200'
					,'70159_70155'
					,'70159_70157'
					,'70197_70155'
					,'70197_70157'
					,'70197_70159'
					,'70200_70201'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 4.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70152'
					,'70150_70154'
					,'70150_70156'
					,'70150_70158'
					,'70150_70200'
					,'70152_70154'
					,'70152_70156'
					,'70152_70158'
					,'70152_70200'
					,'70153_70151'
					,'70154_70201'
					,'70155_70151'
					,'70155_70153'
					,'70156_70201'
					,'70157_70151'
					,'70157_70153'
					,'70158_70201'
					,'70159_70151'
					,'70159_70153'
					,'70197_70151'
					,'70197_70153'
					,'70202_70155'
					,'70202_70157'
					,'70202_70159'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 3.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70201'
					,'70152_70201'
					,'70154_70203'
					,'70154_70205'
					,'70156_70203'
					,'70156_70205'
					,'70158_70203'
					,'70158_70205'
					,'70200_70203'
					,'70200_70205'
					,'70201_70203'
					,'70201_70205'
					,'70202_70151'
					,'70202_70153'
					,'70203_70205'
					,'70204_70157'
					,'70204_70159'
					,'70204_70202'
					,'70206_70155'
					,'70206_70159'
					,'70206_70202'
					,'70206_70204'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 2.0				
			ELSE par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec) 
		 END AS passenger_arrival_estimate
		
	FROM
		config_passenger_arrival_rate par
		
	LEFT JOIN config_time_slice ts
	ON	
		par.time_slice_id = ts.time_slice_id
		
	LEFT JOIN gtfs.route_direction_stop rds1
	ON
		par.from_stop_id = rds1.stop_id

	LEFT JOIN gtfs.route_direction_stop rds2
	ON
		par.to_stop_id = rds2.stop_id
		AND rds1.route_id = rds2.route_id
		AND rds1.direction_id = rds2.direction_id
		AND rds1.stop_order < rds2.stop_order
		
	WHERE	
		par.day_type_id = @day_type_id
		AND rds1.route_id = @route_id_process
		AND ts.time_slice_start_sec >= @start_time_sec_process
		AND ts.time_slice_start_sec < @end_time_sec_process
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 0 AND rds1.stop_order < 50)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 1 AND rds2.stop_order > 600)	
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 0 AND rds1.stop_order < 20)
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 1 AND rds2.stop_order > 630)	
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 0 AND rds1.stop_order < 40)
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 1 AND rds2.stop_order > 610)				

END

--------------------------------------------------------------------------------------------------------------------
IF 
	(@disruption_type_process = '2' AND @ashmont_flag = 0)
	
BEGIN		
	
	INSERT INTO #passenger_arrival_estimate_slice
	(
		service_date
		,route_id
		,from_stop_id
		,to_stop_id
		,time_slice_id
		,time_slice_start_sec
		,time_slice_end_sec
		,passenger_arrival_rate
		,passenger_arrival_estimate
	)

	SELECT DISTINCT
		@service_date_process
		,rds1.route_id
		,rds1.stop_id AS from_stop_id
		,rds2.stop_id AS to_stop_id
		,par.time_slice_id
		,ts.time_slice_start_sec
		,ts.time_slice_end_sec
		,par.passenger_arrival_rate
		,CASE
			WHEN 
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70154_70156'
					,'70154_70158'
					,'70154_70200'
					,'70156_70158'
					,'70156_70200'
					,'70157_70155'
					,'70158_70200'
					,'70159_70155'
					,'70159_70157'
					,'70197_70155'
					,'70197_70157'
					,'70197_70159'
					,'70200_70201'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 4.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70152'
					,'70150_70154'
					,'70150_70156'
					,'70150_70158'
					,'70150_70200'
					,'70152_70154'
					,'70152_70156'
					,'70152_70158'
					,'70152_70200'
					,'70153_70151'
					,'70154_70201'
					,'70155_70151'
					,'70155_70153'
					,'70156_70201'
					,'70157_70151'
					,'70157_70153'
					,'70158_70201'
					,'70159_70151'
					,'70159_70153'
					,'70197_70151'
					,'70197_70153'
					,'70202_70155'
					,'70202_70157'
					,'70202_70159'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 3.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70201'
					,'70152_70201'
					,'70154_70203'
					,'70154_70205'
					,'70156_70203'
					,'70156_70205'
					,'70158_70203'
					,'70158_70205'
					,'70200_70203'
					,'70200_70205'
					,'70201_70203'
					,'70201_70205'
					,'70202_70151'
					,'70202_70153'
					,'70203_70205'
					,'70204_70157'
					,'70204_70159'
					,'70204_70202'
					,'70206_70155'
					,'70206_70159'
					,'70206_70202'
					,'70206_70204'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 2.0				
			ELSE par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec) 
		 END AS passenger_arrival_estimate
		
	FROM
		config_passenger_arrival_rate par
		
	LEFT JOIN config_time_slice ts
	ON	
		par.time_slice_id = ts.time_slice_id
		
	LEFT JOIN gtfs.route_direction_stop rds1
	ON
		par.from_stop_id = rds1.stop_id

	LEFT JOIN gtfs.route_direction_stop rds2
	ON
		par.to_stop_id = rds2.stop_id
		AND rds1.route_id = rds2.route_id
		AND rds1.direction_id = rds2.direction_id
		AND rds1.stop_order < rds2.stop_order
	
	WHERE	
		par.day_type_id = @day_type_id
		AND rds1.route_id = @route_id_process
		AND ts.time_slice_start_sec >= @start_time_sec_process
		AND ts.time_slice_start_sec < @end_time_sec_process
		AND
			(
				(
					 --within the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --through the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order < @from_stop_order_0
					AND rds2.stop_order > @to_stop_order_0
				)
				OR
				(
					 --from the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds1.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --to the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds2.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --within the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --through the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order < @from_stop_order_1
					AND rds2.stop_order > @to_stop_order_1
				)
				OR
				(
					 --from the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds1.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --to the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds2.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)
			)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 0 AND rds1.stop_order < 50)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 1 AND rds2.stop_order > 600)	
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 0 AND rds1.stop_order < 20)
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 1 AND rds2.stop_order > 630)	
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 0 AND rds1.stop_order < 40)
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 1 AND rds2.stop_order > 610)		

END

--------------------------------------------------------------------------------------------------------------------
IF 
	(@disruption_type_process = '2' AND @ashmont_flag = 1)

BEGIN	
	
	INSERT INTO #passenger_arrival_estimate_slice
	(
		service_date
		,route_id
		,from_stop_id
		,to_stop_id
		,time_slice_id
		,time_slice_start_sec
		,time_slice_end_sec
		,passenger_arrival_rate
		,passenger_arrival_estimate
	)

	SELECT DISTINCT
		@service_date_process
		,rds1.route_id
		,rds1.stop_id AS from_stop_id
		,rds2.stop_id AS to_stop_id
		,par.time_slice_id
		,ts.time_slice_start_sec
		,ts.time_slice_end_sec
		,par.passenger_arrival_rate
		,CASE
			WHEN 
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70154_70156'
					,'70154_70158'
					,'70154_70200'
					,'70156_70158'
					,'70156_70200'
					,'70157_70155'
					,'70158_70200'
					,'70159_70155'
					,'70159_70157'
					,'70197_70155'
					,'70197_70157'
					,'70197_70159'
					,'70200_70201'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 4.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70152'
					,'70150_70154'
					,'70150_70156'
					,'70150_70158'
					,'70150_70200'
					,'70152_70154'
					,'70152_70156'
					,'70152_70158'
					,'70152_70200'
					,'70153_70151'
					,'70154_70201'
					,'70155_70151'
					,'70155_70153'
					,'70156_70201'
					,'70157_70151'
					,'70157_70153'
					,'70158_70201'
					,'70159_70151'
					,'70159_70153'
					,'70197_70151'
					,'70197_70153'
					,'70202_70155'
					,'70202_70157'
					,'70202_70159'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 3.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70201'
					,'70152_70201'
					,'70154_70203'
					,'70154_70205'
					,'70156_70203'
					,'70156_70205'
					,'70158_70203'
					,'70158_70205'
					,'70200_70203'
					,'70200_70205'
					,'70201_70203'
					,'70201_70205'
					,'70202_70151'
					,'70202_70153'
					,'70203_70205'
					,'70204_70157'
					,'70204_70159'
					,'70204_70202'
					,'70206_70155'
					,'70206_70159'
					,'70206_70202'
					,'70206_70204'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 2.0				
			ELSE par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec) 
		 END AS passenger_arrival_estimate
		
	FROM
		config_passenger_arrival_rate par
		
	LEFT JOIN config_time_slice ts
	ON	
		par.time_slice_id = ts.time_slice_id
		
	LEFT JOIN gtfs.route_direction_stop rds1
	ON
		par.from_stop_id = rds1.stop_id

	LEFT JOIN gtfs.route_direction_stop rds2
	ON
		par.to_stop_id = rds2.stop_id
		AND rds1.route_id = rds2.route_id
		AND rds1.direction_id = rds2.direction_id
		AND rds1.stop_order < rds2.stop_order
	
	WHERE	
		par.day_type_id = @day_type_id
		AND rds1.route_id = @route_id_process
		AND ts.time_slice_start_sec >= @start_time_sec_process
		AND ts.time_slice_start_sec < @end_time_sec_process
		AND
			(				
				(
					 --within the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --through the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order < @from_stop_order_0
					AND rds2.stop_order > @to_stop_order_0
				)
				OR
				(
					 --from the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds1.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --to the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds2.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --within the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --through the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order < @from_stop_order_1
					AND rds2.stop_order > @to_stop_order_1
				)
				OR
				(
					 --from the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds1.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --to the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds2.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)				
			)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 0 AND rds1.stop_order < 50)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 1 AND rds2.stop_order > 600)	
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 0 AND rds1.stop_order < 20)
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 1 AND rds2.stop_order > 630)	
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 0 AND rds1.stop_order < 40)
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 1 AND rds2.stop_order > 610)		

		AND NOT (rds1.route_id = 'Red' AND rds1.direction_id = 0 AND rds2.stop_order > 170)
		AND NOT (rds1.route_id = 'Red' AND rds1.direction_id = 1 AND rds1.stop_order < 50)				

END
		
--------------------------------------------------------------------------------------------------------------------
IF 
	(@disruption_type_process = '3' AND @ashmont_flag = 0)

BEGIN
	
	INSERT INTO #passenger_arrival_estimate_slice
	(
		service_date
		,route_id
		,from_stop_id
		,to_stop_id
		,time_slice_id
		,time_slice_start_sec
		,time_slice_end_sec
		,passenger_arrival_rate
		,passenger_arrival_estimate
	)

	SELECT DISTINCT
		@service_date_process
		,rds1.route_id
		,rds1.stop_id AS from_stop_id
		,rds2.stop_id AS to_stop_id
		,par.time_slice_id
		,ts.time_slice_start_sec
		,ts.time_slice_end_sec
		,par.passenger_arrival_rate
		,CASE
			WHEN 
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70154_70156'
					,'70154_70158'
					,'70154_70200'
					,'70156_70158'
					,'70156_70200'
					,'70157_70155'
					,'70158_70200'
					,'70159_70155'
					,'70159_70157'
					,'70197_70155'
					,'70197_70157'
					,'70197_70159'
					,'70200_70201'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 4.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70152'
					,'70150_70154'
					,'70150_70156'
					,'70150_70158'
					,'70150_70200'
					,'70152_70154'
					,'70152_70156'
					,'70152_70158'
					,'70152_70200'
					,'70153_70151'
					,'70154_70201'
					,'70155_70151'
					,'70155_70153'
					,'70156_70201'
					,'70157_70151'
					,'70157_70153'
					,'70158_70201'
					,'70159_70151'
					,'70159_70153'
					,'70197_70151'
					,'70197_70153'
					,'70202_70155'
					,'70202_70157'
					,'70202_70159'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 3.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70201'
					,'70152_70201'
					,'70154_70203'
					,'70154_70205'
					,'70156_70203'
					,'70156_70205'
					,'70158_70203'
					,'70158_70205'
					,'70200_70203'
					,'70200_70205'
					,'70201_70203'
					,'70201_70205'
					,'70202_70151'
					,'70202_70153'
					,'70203_70205'
					,'70204_70157'
					,'70204_70159'
					,'70204_70202'
					,'70206_70155'
					,'70206_70159'
					,'70206_70202'
					,'70206_70204'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 2.0				
			ELSE par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec) 
		 END AS passenger_arrival_estimate
		
	FROM
		config_passenger_arrival_rate par
		
	LEFT JOIN config_time_slice ts
	ON	
		par.time_slice_id = ts.time_slice_id
		
	LEFT JOIN gtfs.route_direction_stop rds1
	ON
		par.from_stop_id = rds1.stop_id

	LEFT JOIN gtfs.route_direction_stop rds2
	ON
		par.to_stop_id = rds2.stop_id
		AND rds1.route_id = rds2.route_id
		AND rds1.direction_id = rds2.direction_id
		AND rds1.stop_order < rds2.stop_order
			
	WHERE	
		par.day_type_id = @day_type_id
		AND rds1.route_id = @route_id_process
		AND ts.time_slice_start_sec >= @start_time_sec_process
		AND ts.time_slice_start_sec < @end_time_sec_process
		AND
			(
				(
					 --within the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --from the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds1.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --to the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds2.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --within the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --from the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds1.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --to the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds2.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)				
			)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 0 AND rds1.stop_order < 50)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 1 AND rds2.stop_order > 600)	
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 0 AND rds1.stop_order < 20)
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 1 AND rds2.stop_order > 630)	
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 0 AND rds1.stop_order < 40)
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 1 AND rds2.stop_order > 610)		
	
END
	
--------------------------------------------------------------------------------------------------------------------
IF
	(@disruption_type_process = '3' AND @ashmont_flag = 1)

BEGIN
	
	INSERT INTO #passenger_arrival_estimate_slice
	(
		service_date
		,route_id
		,from_stop_id
		,to_stop_id
		,time_slice_id
		,time_slice_start_sec
		,time_slice_end_sec
		,passenger_arrival_rate
		,passenger_arrival_estimate
	)

	SELECT DISTINCT
		@service_date_process
		,rds1.route_id
		,rds1.stop_id AS from_stop_id
		,rds2.stop_id AS to_stop_id
		,par.time_slice_id
		,ts.time_slice_start_sec
		,ts.time_slice_end_sec
		,par.passenger_arrival_rate
		,CASE
			WHEN 
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70154_70156'
					,'70154_70158'
					,'70154_70200'
					,'70156_70158'
					,'70156_70200'
					,'70157_70155'
					,'70158_70200'
					,'70159_70155'
					,'70159_70157'
					,'70197_70155'
					,'70197_70157'
					,'70197_70159'
					,'70200_70201'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 4.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70152'
					,'70150_70154'
					,'70150_70156'
					,'70150_70158'
					,'70150_70200'
					,'70152_70154'
					,'70152_70156'
					,'70152_70158'
					,'70152_70200'
					,'70153_70151'
					,'70154_70201'
					,'70155_70151'
					,'70155_70153'
					,'70156_70201'
					,'70157_70151'
					,'70157_70153'
					,'70158_70201'
					,'70159_70151'
					,'70159_70153'
					,'70197_70151'
					,'70197_70153'
					,'70202_70155'
					,'70202_70157'
					,'70202_70159'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 3.0
			WHEN
				CONCAT(rds1.stop_id, '_', rds2.stop_id) IN
					(
					'70150_70201'
					,'70152_70201'
					,'70154_70203'
					,'70154_70205'
					,'70156_70203'
					,'70156_70205'
					,'70158_70203'
					,'70158_70205'
					,'70200_70203'
					,'70200_70205'
					,'70201_70203'
					,'70201_70205'
					,'70202_70151'
					,'70202_70153'
					,'70203_70205'
					,'70204_70157'
					,'70204_70159'
					,'70204_70202'
					,'70206_70155'
					,'70206_70159'
					,'70206_70202'
					,'70206_70204'
					)
				THEN
					(par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec)) / 2.0				
			ELSE par.passenger_arrival_rate * (ts.time_slice_end_sec - ts.time_slice_start_sec) 
		 END AS passenger_arrival_estimate
		
	FROM
		config_passenger_arrival_rate par
		
	LEFT JOIN config_time_slice ts
	ON	
		par.time_slice_id = ts.time_slice_id
		
	LEFT JOIN gtfs.route_direction_stop rds1
	ON
		par.from_stop_id = rds1.stop_id

	LEFT JOIN gtfs.route_direction_stop rds2
	ON
		par.to_stop_id = rds2.stop_id
		AND rds1.route_id = rds2.route_id
		AND rds1.direction_id = rds2.direction_id
		AND rds1.stop_order < rds2.stop_order
			
	WHERE	
		par.day_type_id = @day_type_id
		AND rds1.route_id = @route_id_process
		AND ts.time_slice_start_sec >= @start_time_sec_process
		AND ts.time_slice_start_sec < @end_time_sec_process
		AND
			(
				(
					 --within the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --from the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds1.stop_order >= @from_stop_order_0
					AND rds1.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --to the segment, direction_id = 0
					rds1.direction_id = 0
					AND rds2.stop_order >= @from_stop_order_0
					AND rds2.stop_order <= @to_stop_order_0
				)
				OR
				(
					 --within the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --from the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds1.stop_order >= @from_stop_order_1
					AND rds1.stop_order <= @to_stop_order_1
				)
				OR
				(
					 --to the segment, direction_id = 1
					rds1.direction_id = 1
					AND rds2.stop_order >= @from_stop_order_1
					AND rds2.stop_order <= @to_stop_order_1
				)				
			)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 0 AND rds1.stop_order < 50)
		AND NOT (rds1.route_id = 'Green-B' AND rds1.direction_id = 1 AND rds2.stop_order > 600)	
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 0 AND rds1.stop_order < 20)
		AND NOT (rds1.route_id = 'Green-C' AND rds1.direction_id = 1 AND rds2.stop_order > 630)	
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 0 AND rds1.stop_order < 40)
		AND NOT (rds1.route_id = 'Green-D' AND rds1.direction_id = 1 AND rds2.stop_order > 610)		

		AND NOT (rds1.route_id = 'Red' AND rds1.direction_id = 0 AND rds2.stop_order > 170)
		AND NOT (rds1.route_id = 'Red' AND rds1.direction_id = 1 AND rds1.stop_order < 50)				

END

---------------------------------------------------------------------------------
	--Passenger Arrival Estimate by Route and Time Period
---------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#passenger_arrival_estimate_period','U') IS NOT NULL
		DROP TABLE #passenger_arrival_estimate_period

	CREATE TABLE #passenger_arrival_estimate_period
	(
		service_date				VARCHAR(255)
		,route_id					VARCHAR(255)
		,threshold_id				VARCHAR(255)
		,time_period_type			VARCHAR(255)
		,passenger_arrival_estimate	FLOAT
	)
	
	INSERT INTO #passenger_arrival_estimate_period
	(
		service_date
		,route_id
		,threshold_id
		,time_period_type
		,passenger_arrival_estimate
	)
	
	SELECT
		s.service_date
		,s.route_id
		,t.threshold_id
		,tp.time_period_type
		,sum(s.passenger_arrival_estimate)
		
	FROM
		#passenger_arrival_estimate_slice s
		
	CROSS JOIN @threshold_ids t	
				
	LEFT JOIN dbo.service_date sd
		ON 
			s.service_date = sd.service_date

	LEFT JOIN dbo.config_time_period tp
		ON
			sd.day_type_id = tp.day_type_id
			AND s.time_slice_start_sec >= tp.time_period_start_time_sec
			AND s.time_slice_end_sec <= tp.time_period_end_time_sec

	WHERE
		s.from_stop_id IS NOT NULL
		AND s.to_stop_id IS NOT NULL
		AND s.passenger_arrival_estimate IS NOT NULL
			
	GROUP BY
		s.service_date
		,s.route_id
		,t.threshold_id
		,tp.time_period_type					
			


---------------------------------------------------------------------------------
	--Existing Numerator and Denominator by OD and Time Slice
---------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#existing_num_denom_slice','U') IS NOT NULL
		DROP TABLE #existing_num_denom_slice

	CREATE TABLE #existing_num_denom_slice
	(
		service_date			VARCHAR(255)
		,route_id				VARCHAR(255)
		,threshold_id			VARCHAR(255)
		,from_stop_id			VARCHAR(255)
		,to_stop_id				VARCHAR(255)
		,time_slice_id			VARCHAR(255)
		,time_slice_start_sec	INT
		,time_slice_end_sec		INT
		,existing_numerator		FLOAT
		,existing_denominator	FLOAT
	)
	
	INSERT INTO #existing_num_denom_slice
	(
		service_date
		,route_id
		,threshold_id
		,from_stop_id
		,to_stop_id
		,time_slice_id
		,time_slice_start_sec
		,time_slice_end_sec
		,existing_numerator
		,existing_denominator
	)
			
	SELECT
		hwt.service_date
		,hwt.route_id
		,hwt.threshold_id
		,hwt.from_stop_id
		,hwt.to_stop_id
		,ts.time_slice_id
		,ts.time_slice_start_sec
		,ts.time_slice_end_sec
		,SUM(hwt.scheduled_threshold_numerator_pax) as existing_numerator
		,SUM(hwt.denominator_pax) as existing_denominator
		
	FROM
		historical_wait_time_od_threshold_pax hwt

	LEFT JOIN dbo.config_time_slice ts
		ON
			hwt.start_time_sec >= ts.time_slice_start_sec
			AND hwt.end_time_sec < ts.time_slice_end_sec
		
	WHERE
		hwt.service_date = @service_date_process
		AND hwt.route_id = @route_id_process
		AND hwt.threshold_id IN (SELECT threshold_id FROM @threshold_ids)
		AND hwt.start_time_sec >= @start_time_sec_process
		AND hwt.end_time_sec < @end_time_sec_process
		AND CONCAT(hwt.from_stop_id, '_', hwt.to_stop_id) IN (SELECT CONCAT(from_stop_id, '_', to_stop_id) FROM #passenger_arrival_estimate_slice )
	
	GROUP BY	
		hwt.service_date
		,hwt.route_id
		,hwt.threshold_id
		,hwt.from_stop_id
		,hwt.to_stop_id
		,ts.time_slice_id
		,ts.time_slice_start_sec
		,ts.time_slice_end_sec			
			

			
---------------------------------------------------------------------------------
	--Existing Numerator and Denominator by Route and Time Period
---------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#existing_num_denom_period','U') IS NOT NULL
		DROP TABLE #existing_num_denom_period

	CREATE TABLE #existing_num_denom_period
	(
		service_date				VARCHAR(255)
		,route_id					VARCHAR(255)
		,threshold_id				VARCHAR(255)
		,time_period_type			VARCHAR(255)
		,existing_numerator			FLOAT
		,existing_denominator		FLOAT
	)
				
	INSERT INTO #existing_num_denom_period
	(
		service_date
		,route_id
		,threshold_id
		,time_period_type
		,existing_numerator
		,existing_denominator
	)
			
	SELECT
		s.service_date
		,s.route_id
		,s.threshold_id
		,tp.time_period_type
		,SUM(s.existing_numerator) AS existing_numerator
		,SUM(s.existing_denominator) AS existing_denominator
		
	FROM
		#existing_num_denom_slice s		
			
	LEFT JOIN dbo.service_date sd
		ON 
			s.service_date = sd.service_date

	LEFT JOIN dbo.config_time_period tp
		ON
			sd.day_type_id = tp.day_type_id
			AND s.time_slice_start_sec >= tp.time_period_start_time_sec
			AND s.time_slice_end_sec <= tp.time_period_end_time_sec			
			
	GROUP BY
		s.service_date
		,s.route_id
		,s.threshold_id
		,tp.time_period_type


---------------------------------------------------------------------------------
	--Procedure if a previous revision was performed
---------------------------------------------------------------------------------		
		
		
	IF 
	(
		SELECT 
			COUNT(*)
		FROM dbo.revise_historical_metrics_log
		WHERE 
			service_date IN (SELECT DISTINCT service_date FROM #passenger_arrival_estimate_period)
			AND route_id IN (SELECT DISTINCT route_id FROM #passenger_arrival_estimate_period)
			AND threshold_id IN (SELECT DISTINCT threshold_id FROM #passenger_arrival_estimate_period)
	) 
	> 0

BEGIN

	UPDATE dbo.historical_metrics
		SET 
			metric_result = l.original_metric_result
			,numerator_pax = l.original_numerator_pax
			,denominator_pax = l.original_denominator_pax
		FROM dbo.historical_metrics hm
		
		LEFT JOIN dbo.revise_historical_metrics_log l
		ON
			hm.service_date = l.service_date
			AND hm.route_id = l.route_id
			AND hm.threshold_id = l.threshold_id
			AND hm.time_period_type = l.time_period_type

		WHERE 
			hm.service_date IN (SELECT DISTINCT service_date FROM #passenger_arrival_estimate_period)
			AND hm.route_id IN (SELECT DISTINCT route_id FROM #passenger_arrival_estimate_period)
			AND hm.threshold_id IN (SELECT DISTINCT threshold_id FROM #passenger_arrival_estimate_period)
			AND l.revise_datetime = 
				(	SELECT 
						MIN(a.revise_datetime)
					FROM 
						dbo.revise_historical_metrics_log a
					WHERE
						a.service_date IN (SELECT DISTINCT service_date FROM #passenger_arrival_estimate_period)
						AND a.route_id IN (SELECT DISTINCT route_id FROM #passenger_arrival_estimate_period)
						AND a.threshold_id IN (SELECT DISTINCT threshold_id FROM #passenger_arrival_estimate_period)					
				)
			
	
	--Write to revision log

	INSERT INTO dbo.revise_historical_metrics_log
	(
		revise_datetime
		,service_date
		,route_id
		,disruption_type
		,start_time_sec
		,end_time_sec
		,from_stop_order_0
		,to_stop_order_0
		,from_stop_order_1
		,to_stop_order_1
		,ashmont_flag
		,threshold_id
		,time_period_type
		,original_metric_result
		,original_numerator_pax
		,original_denominator_pax
		,revised_metric_result
		,revised_numerator_pax
		,revised_denominator_pax
	)
		
	SELECT	
		SYSDATETIME()
		,@service_date
		,@route_id
		,CASE
			WHEN @disruption_type_process = '1' THEN 'disruption_type_01'
			WHEN @disruption_type_process = '2' THEN 'disruption_type_02'
			WHEN @disruption_type_process = '3' THEN 'disruption_type_03'
			ELSE @disruption_type_process
			END
		,CASE
			WHEN @disruption_type = 'reset' THEN NULL
			ELSE @start_time_sec
			END AS start_time_sec
		,CASE
			WHEN @disruption_type = 'reset' THEN NULL
			ELSE @end_time_sec
			END AS end_time_sec
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @from_stop_order_0
			ELSE NULL
			END AS from_stop_order_0
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @to_stop_order_0
			ELSE NULL
			END AS to_stop_order_0
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @from_stop_order_1
			ELSE NULL
			END AS from_stop_order_1
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @to_stop_order_1
			ELSE NULL
			END AS to_stop_order_1
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @ashmont_flag
			ELSE NULL
			END AS ashmont_flag
		,paep.threshold_id
		,paep.time_period_type
		,hm.metric_result
		,hm.numerator_pax
		,hm.denominator_pax
		,1 - ((hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate)/(hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate)) AS revised_metric_result
		,hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate AS revised_numerator_pax
		,hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate AS revised_denominator_pax
		
		
	FROM
		#passenger_arrival_estimate_period paep
		
	LEFT JOIN #existing_num_denom_period endp
		ON
			endp.service_date = paep.service_date
			AND endp.route_id = paep.route_id
			AND endp.threshold_id = paep.threshold_id
			AND endp.time_period_type = paep.time_period_type
				
	LEFT JOIN dbo.historical_metrics hm
		ON
			hm.service_date = paep.service_date
			AND hm.route_id = paep.route_id
			AND hm.threshold_id = paep.threshold_id
			AND hm.time_period_type = paep.time_period_type		


	--Revise historical_metrics

	UPDATE dbo.historical_metrics
		SET 
			metric_result = 1 - ((hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate)/(hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate))
			,numerator_pax = hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate
			,denominator_pax = hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate
		FROM dbo.historical_metrics hm
		
		LEFT JOIN #passenger_arrival_estimate_period paep
			ON
				hm.service_date = paep.service_date
				AND hm.route_id = paep.route_id
				AND hm.threshold_id = paep.threshold_id
				AND hm.time_period_type = paep.time_period_type	
		
		LEFT JOIN #existing_num_denom_period endp
			ON
				endp.service_date = paep.service_date
				AND endp.route_id = paep.route_id
				AND endp.threshold_id = paep.threshold_id
				AND endp.time_period_type = paep.time_period_type		

		WHERE
			hm.service_date = paep.service_date
			AND hm.route_id = paep.route_id
			AND hm.threshold_id = paep.threshold_id
			AND hm.time_period_type = paep.time_period_type				
			
END			

---------------------------------------------------------------------------------
	--Procedure if no previous revision was performed
---------------------------------------------------------------------------------		
		
		
	IF 
	(
		SELECT 
			COUNT(*)
		FROM dbo.revise_historical_metrics_log
		WHERE 
			service_date IN (SELECT DISTINCT service_date FROM #passenger_arrival_estimate_period)
			AND route_id IN (SELECT DISTINCT route_id FROM #passenger_arrival_estimate_period)
			AND threshold_id IN (SELECT DISTINCT threshold_id FROM #passenger_arrival_estimate_period)
	) 
	= 0
		
BEGIN

	--Write to revision log


	INSERT INTO dbo.revise_historical_metrics_log
	(
		revise_datetime
		,service_date
		,route_id
		,disruption_type
		,start_time_sec
		,end_time_sec
		,from_stop_order_0
		,to_stop_order_0
		,from_stop_order_1
		,to_stop_order_1
		,ashmont_flag
		,threshold_id
		,time_period_type
		,original_metric_result
		,original_numerator_pax
		,original_denominator_pax
		,revised_metric_result
		,revised_numerator_pax
		,revised_denominator_pax
	)
		
	SELECT	
		SYSDATETIME()
		,@service_date
		,@route_id
		,CASE
			WHEN @disruption_type_process = '1' THEN 'disruption_type_01'
			WHEN @disruption_type_process = '2' THEN 'disruption_type_02'
			WHEN @disruption_type_process = '3' THEN 'disruption_type_03'
			ELSE @disruption_type_process
			END
		,CASE
			WHEN @disruption_type = 'reset' THEN NULL
			ELSE @start_time_sec
			END AS start_time_sec
		,CASE
			WHEN @disruption_type = 'reset' THEN NULL
			ELSE @end_time_sec
			END AS end_time_sec
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @from_stop_order_0
			ELSE NULL
			END AS from_stop_order_0
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @to_stop_order_0
			ELSE NULL
			END AS to_stop_order_0
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @from_stop_order_1
			ELSE NULL
			END AS from_stop_order_1
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @to_stop_order_1
			ELSE NULL
			END AS to_stop_order_1
		,CASE	
			WHEN @disruption_type IN ('2', '3') THEN @ashmont_flag
			ELSE NULL
			END AS ashmont_flag
		,paep.threshold_id
		,paep.time_period_type
		,hm.metric_result
		,hm.numerator_pax
		,hm.denominator_pax
		,1 - ((hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate)/(hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate)) AS revised_metric_result
		,hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate AS revised_numerator_pax
		,hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate AS revised_denominator_pax
		
		
	FROM
		#passenger_arrival_estimate_period paep
		
	LEFT JOIN #existing_num_denom_period endp
		ON
			endp.service_date = paep.service_date
			AND endp.route_id = paep.route_id
			AND endp.threshold_id = paep.threshold_id
			AND endp.time_period_type = paep.time_period_type
				
	LEFT JOIN dbo.historical_metrics hm
		ON
			hm.service_date = paep.service_date
			AND hm.route_id = paep.route_id
			AND hm.threshold_id = paep.threshold_id
			AND hm.time_period_type = paep.time_period_type		


	--Revise historical_metrics
			
	UPDATE dbo.historical_metrics
		SET 
			metric_result = 1 - ((hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate)/(hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate))
			,numerator_pax = hm.numerator_pax - endp.existing_numerator + paep.passenger_arrival_estimate
			,denominator_pax = hm.denominator_pax - endp.existing_denominator + paep.passenger_arrival_estimate
		FROM dbo.historical_metrics hm
		
		LEFT JOIN #passenger_arrival_estimate_period paep
			ON
				hm.service_date = paep.service_date
				AND hm.route_id = paep.route_id
				AND hm.threshold_id = paep.threshold_id
				AND hm.time_period_type = paep.time_period_type	
		
		LEFT JOIN #existing_num_denom_period endp
			ON
				endp.service_date = paep.service_date
				AND endp.route_id = paep.route_id
				AND endp.threshold_id = paep.threshold_id
				AND endp.time_period_type = paep.time_period_type		

		WHERE
			hm.service_date = paep.service_date
			AND hm.route_id = paep.route_id
			AND hm.threshold_id = paep.threshold_id
			AND hm.time_period_type = paep.time_period_type	

END		
		
DROP TABLE #passenger_arrival_estimate_slice
DROP TABLE #passenger_arrival_estimate_period
DROP TABLE #existing_num_denom_slice
DROP TABLE #existing_num_denom_period

END
