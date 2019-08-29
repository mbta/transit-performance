
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.ProcessPredictionAccuracyDaily','P') IS NOT NULL
	DROP PROCEDURE dbo.ProcessPredictionAccuracyDaily
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE dbo.ProcessPredictionAccuracyDaily

--Script Version: Master - 1.1.0.0

--This procedure calculates daily prediction accuracy metrics.

	@service_date	DATE

AS


BEGIN
    SET NOCOUNT ON; 


	DECLARE @service_date_process DATE
	SET @service_date_process = @service_date 

	--store daily predictions from the trip_update_denormalized table
	IF OBJECT_ID('dbo.daily_prediction', 'U') IS NOT NULL
	  DROP TABLE dbo.daily_prediction
	;

	CREATE TABLE dbo.daily_prediction(
		service_date					DATE			NOT NULL
		,file_time						INT				NOT NULL
		,file_time_dt					DATETIME
		,route_id						VARCHAR(255)	NOT NULL
		,trip_id						VARCHAR(255)	NOT NULL
		,direction_id					INT				NOT NULL
		,stop_id						VARCHAR(255)	NOT NULL
		,stop_sequence					INT				NOT NULL
		,vehicle_id						VARCHAR(255) 
		,vehicle_label					VARCHAR(255) 
		,predicted_arrival_time			INT				
		,predicted_departure_time		INT
		,predicted_arrival_time_sec		INT
		,predicted_departure_time_sec	INT
		,vehicle_timestamp				INT
		)
	;

	INSERT INTO dbo.daily_prediction
	(
		service_date
		,file_time
		,file_time_dt
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,predicted_arrival_time
		,predicted_departure_time
		,predicted_arrival_time_sec
		,predicted_departure_time_sec
		,vehicle_timestamp
	)

	SELECT
		CONVERT(DATE, trip_start_date)
		,p.header_timestamp
		,dbo.fnConvertEpochToDateTime(p.header_timestamp)
		,p.route_id
		,p.trip_id
		,p.direction_id
		,p.stop_id
		,p.stop_sequence
		,p.vehicle_id
		,p.vehicle_label
		,p.predicted_arrival_time
		,p.predicted_departure_time
		,p.predicted_arrival_time - dbo.fnConvertDateTimeToEpoch(trip_start_date)
		,p.predicted_departure_time - dbo.fnConvertDateTimeToEpoch(trip_start_date)
		,p.vehicle_timestamp
	FROM dbo.gtfsrt_tripupdate_denormalized p

	WHERE
		CONVERT(DATE, p.trip_start_date) = @service_date_process

	--save the last prediction made during a given minute into a consolidated table 
	--(e.g. if two predictions are made bw 1:20 and 1:21, say at 1:20:15 and 1:20:35, only the one made at 1:20:35 will be saved)
	IF OBJECT_ID('dbo.daily_prediction_consolidated', 'U') IS NOT NULL
	  DROP TABLE dbo.daily_prediction_consolidated
	;

	CREATE TABLE dbo.daily_prediction_consolidated(
		service_date				DATE			NOT NULL
		,file_time					INT				NOT NULL
		,file_time_dt				DATETIME
		,route_id					VARCHAR(255)	NOT NULL
		,trip_id					VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,stop_sequence				INT				NOT NULL
		,vehicle_id					VARCHAR(255) 
		,vehicle_label				VARCHAR(255) 
		,predicted_arrival_time		INT 
		,predicted_departure_time	INT
		,predicted_arrival_time_sec		INT
		,predicted_departure_time_sec	INT
		,vehicle_timestamp			INT
		)
	;

	INSERT INTO dbo.daily_prediction_consolidated
	(
		service_date
		,file_time
		,file_time_dt
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,predicted_arrival_time
		,predicted_departure_time
		,predicted_arrival_time_sec		
		,predicted_departure_time_sec	
		,vehicle_timestamp
	)
	
	SELECT
		service_date
		,file_time
		,file_time_dt
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,predicted_arrival_time
		,predicted_departure_time
		,predicted_arrival_time_sec		
		,predicted_departure_time_sec	
		,vehicle_timestamp
	FROM (
		SELECT 
			service_date
			,file_time
			,file_time_dt
			,route_id
			,trip_id
			,direction_id
			,stop_id
			,stop_sequence
			,vehicle_id
			,vehicle_label
			,predicted_arrival_time
			,predicted_departure_time
			,predicted_arrival_time_sec		
			,predicted_departure_time_sec	
			,vehicle_timestamp
			,ROW_NUMBER() OVER (PARTITION BY service_date, trip_id, stop_id, stop_sequence, DATEADD(s,-DATEPART(s,file_time_dt),file_time_dt)
			ORDER BY file_time DESC)  as rn
		FROM 
		dbo.daily_prediction
	) t
	WHERE rn = 1
	--ORDER BY trip_id, stop_id, file_time_dt

	--save the scheduled and actual times for the last updated prediction of each minute
	IF OBJECT_ID('dbo.daily_prediction_disaggregate', 'U') IS NOT NULL
	  DROP TABLE dbo.daily_prediction_disaggregate
	;

	CREATE TABLE dbo.daily_prediction_disaggregate(
		service_date				DATE			NOT NULL
		,file_time					INT				NOT NULL
		,file_time_dt				DATETIME
		,route_type					INT
		,route_id					VARCHAR(255)	NOT NULL
		,trip_id					VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,stop_sequence				INT				NOT NULL
		,vehicle_id					VARCHAR(255) 
		,vehicle_label				VARCHAR(255) 
		,stop_order_flag			INT --1 for origin, 2 for mid, 3 for destination stop
		,scheduled_arrival_time		INT
		,scheduled_departure_time	INT	
		,predicted_arrival_time		INT 
		,predicted_departure_time	INT
		,predicted_arrival_time_sec		INT
		,predicted_departure_time_sec	INT
		,actual_arrival_time		INT
		,actual_departure_time		INT
		,arrival_seconds_away		INT
		,departure_seconds_away		INT
		,arrival_prediction_error	INT
		,departure_prediction_error	INT
		,vehicle_timestamp			INT
		)
	;

	INSERT INTO dbo.daily_prediction_disaggregate
	(
		service_date
		,file_time
		,file_time_dt
		,route_type
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,stop_order_flag
		,scheduled_arrival_time
		,scheduled_departure_time	
		,predicted_arrival_time
		,predicted_departure_time
		,predicted_arrival_time_sec		
		,predicted_departure_time_sec	
		,actual_arrival_time
		,actual_departure_time
		,arrival_seconds_away
		,departure_seconds_away
		,arrival_prediction_error
		,departure_prediction_error
		,vehicle_timestamp
	)

	SELECT
		p.service_date
		,p.file_time
		,p.file_time_dt
		,st.route_type
		,p.route_id
		,p.trip_id
		,p.direction_id
		,p.stop_id
		,p.stop_sequence
		,p.vehicle_id
		,p.vehicle_label
		,st.stop_order_flag AS stop_order_flag
		,dbo.fnConvertDateTimeToEpoch(st.service_date)+st.arrival_time_sec AS scheduled_arrival_time
		,dbo.fnConvertDateTimeToEpoch(st.service_date)+st.departure_time_sec AS scheduled_departure_time	
		,p.predicted_arrival_time
		,p.predicted_departure_time
		,p.predicted_arrival_time_sec		
		,p.predicted_departure_time_sec	
		,e1.event_time AS actual_arrival_time
		,e2.event_time AS actual_departure_time
		,p.predicted_arrival_time - p.file_time AS arrival_seconds_away
		,p.predicted_departure_time - p.file_time AS departure_seconds_away
		,e1.event_time - p.predicted_arrival_time AS arrival_prediction_error
		,e2.event_time - p.predicted_departure_time AS departure_prediction_error
		,p.vehicle_timestamp
	FROM dbo.daily_prediction_consolidated p
	
		LEFT JOIN dbo.daily_event e1
	ON
			p.service_date = e1.service_date 
		AND
			p.trip_id = e1.trip_id
		AND
			p.stop_id = e1.stop_id
		AND
			p.stop_sequence = e1.stop_sequence
		AND
			e1.event_type = 'ARR'
		AND
			e1.suspect_record = 0 

	LEFT JOIN dbo.daily_event e2
	ON
			p.service_date = e2.service_date
		AND
			p.trip_id = e2.trip_id	
		AND
			p.stop_id = e2.stop_id
		AND
			p.stop_sequence = e2.stop_sequence
		AND
			e2.event_type = 'DEP'
		AND
			e2.suspect_record = 0

	LEFT JOIN daily_stop_times_sec st
		ON
			p.service_date = st.service_date
		AND
			p.trip_id = st.trip_id
		AND
			p.stop_id = st.stop_id
		AND
			p.stop_sequence = st.stop_sequence

	--create table with bins and thresholds for each prediction
	IF OBJECT_ID('dbo.daily_prediction_threshold','U') IS NOT NULL
		DROP TABLE dbo.daily_prediction_threshold
	;

	CREATE TABLE dbo.daily_prediction_threshold(
		service_date				DATE			NOT NULL
		,file_time					INT				NOT NULL
		,route_type					INT
		,route_id					VARCHAR(255)	NOT NULL
		,trip_id					VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,stop_sequence				INT				NOT NULL
		,stop_order_flag			INT --1 for origin, 2 for mid, 3 for destination stop
		,predicted_time				INT
		,actual_time				INT
		,time_slice_id				VARCHAR(255)
		,seconds_away				INT
		,prediction_error			INT
		,threshold_id				VARCHAR(255)
		,bin_lower					INT
		,bin_upper					INT
		,pred_error_threshold_lower	INT
		,pred_error_threshold_upper	INT
		,prediction_within_threshold INT
		,prediction_in_bin			INT
	)
	;

	INSERT INTO  dbo.daily_prediction_threshold(
		service_date				
		,file_time					
		,route_type					
		,route_id					
		,trip_id				
		,direction_id			
		,stop_id					
		,stop_sequence				
		,stop_order_flag			
		,predicted_time				
		,actual_time				
		,time_slice_id			
		,seconds_away				
		,prediction_error			
		,threshold_id			
		,bin_lower					
		,bin_upper					
		,pred_error_threshold_lower	
		,pred_error_threshold_upper	
		,prediction_within_threshold 
		,prediction_in_bin			
	)

	SELECT
		t.service_date
		,t.file_time
		,t.route_type
		,t.route_id
		,t.trip_id
		,t.direction_id
		,t.stop_id
		,t.stop_sequence
		,t.stop_order_flag
		,t.predicted_time
		,t.actual_time
		,b.time_slice_id
		,t.seconds_away
		,t.prediction_error
		,a.threshold_id
		,a.bin_lower
		,a.bin_upper
		,a.pred_error_threshold_lower
		,a.pred_error_threshold_upper
		,CASE 
			WHEN prediction_error BETWEEN a.pred_error_threshold_lower AND a.pred_error_threshold_upper THEN 1
			ELSE 0
		END AS prediction_within_thresholds
		,1 AS prediction_in_bin
	FROM (
			SELECT 
				b.service_date
				,b.route_type
				,b.route_id
				,b.trip_id
				,b.direction_id
				,b.stop_id
				,b.stop_sequence
				,b.stop_order_flag
				,b.file_time
				,b.file_time_dt
				,CASE
						WHEN b.stop_order_flag = 1 THEN b.predicted_departure_time
						WHEN b.stop_order_flag = 2 THEN b.predicted_arrival_time
						WHEN b.stop_order_flag = 3 THEN b.predicted_arrival_time
					END AS predicted_time
				,CASE
					WHEN b.stop_order_flag = 1 THEN b.predicted_departure_time_sec
					WHEN b.stop_order_flag = 2 THEN b.predicted_arrival_time_sec
					WHEN b.stop_order_flag = 3 THEN b.predicted_arrival_time_sec
				END AS predicted_time_sec
				,CASE
						WHEN b.stop_order_flag = 1 THEN b.actual_departure_time
						WHEN b.stop_order_flag = 2 THEN b.actual_arrival_time
						WHEN b.stop_order_flag = 3 THEN b.actual_arrival_time
					END AS actual_time
				,CASE
						WHEN b.stop_order_flag = 1 THEN b.departure_seconds_away
						WHEN b.stop_order_flag = 2 THEN b.arrival_seconds_away
						WHEN b.stop_order_flag = 3 THEN b.arrival_seconds_away
					END AS seconds_away --mbta wants this based on predicted time. 
				,CASE
						WHEN b.stop_order_flag = 1 THEN b.departure_prediction_error
						WHEN b.stop_order_flag = 2 THEN b.arrival_prediction_error
						WHEN b.stop_order_flag = 3 THEN b.arrival_prediction_error
					END AS prediction_error
			FROM dbo.daily_prediction_disaggregate b 
		) AS t
	JOIN dbo.config_prediction_threshold a
	ON
			t.seconds_away >= a.bin_lower
		AND
			t.seconds_away <= a.bin_upper 
		AND
			a.route_type = t.route_type
		AND
			t.actual_time IS NOT NULL
	JOIN dbo.config_time_slice b
	ON
			t.predicted_time_sec >= b.time_slice_start_sec
		AND
			t.predicted_time_sec < b.time_slice_end_sec

	--calculate prediction quality

	--create table for daily metrics
	IF OBJECT_ID('dbo.daily_prediction_metrics', 'U') IS NOT NULL
	  DROP TABLE dbo.daily_prediction_metrics
	;

	CREATE TABLE dbo.daily_prediction_metrics(
		route_id								VARCHAR(255) NOT NULL
		,threshold_id							VARCHAR(255) NOT NULL
		,threshold_name							VARCHAR(255) NOT NULL
		,threshold_type							VARCHAR(255) NOT NULL
		,total_predictions_within_threshold		INT
		,total_predictions_in_bin				INT
		,metric_result							FLOAT
	)
	;

	INSERT INTO dbo.daily_prediction_metrics(
		route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,total_predictions_within_threshold
		,total_predictions_in_bin
		,metric_result
	)


	SELECT
		route_id
		,cpt.threshold_id
		,cpt.threshold_name
		,cpt.threshold_type
		,SUM(prediction_within_threshold) AS total_predictions_within_threshold
		,SUM(prediction_in_bin) AS total_predictions_in_bin
		,SUM(prediction_within_threshold)/(SUM(prediction_in_bin)*1.0) AS metric_result
	FROM
		dbo.daily_prediction_threshold dpt
	JOIN dbo.config_prediction_threshold cpt
		ON 
				dpt.threshold_id = cpt.threshold_id
			AND
				dpt.route_type = cpt.route_type
	GROUP BY 
		route_id
		,cpt.threshold_id
		,cpt.threshold_name
		,cpt.threshold_type
	ORDER BY 
		route_id
		,cpt.threshold_id
		,cpt.threshold_name
		,cpt.threshold_type

	--create table for daily disaggregate metrics (by route, direction, stop and time slice)
	IF OBJECT_ID('dbo.daily_prediction_metrics_disaggregate', 'U') IS NOT NULL
	  DROP TABLE dbo.daily_prediction_metrics_disaggregate
	;

	CREATE TABLE dbo.daily_prediction_metrics_disaggregate(
		route_id								VARCHAR(255)	NOT NULL
		,direction_id							INT				NOT NULL
		,stop_id								VARCHAR(255)	NOT NULL
		,time_slice_id							VARCHAR(255)	NOT NULL
		,threshold_id							VARCHAR(255)	NOT NULL
		,threshold_name							VARCHAR(255)	NOT NULL
		,threshold_type							VARCHAR(255)	NOT NULL
		,total_predictions_within_threshold		INT
		,total_predictions_in_bin				INT
		,metric_result							FLOAT
	)
	;

	INSERT INTO dbo.daily_prediction_metrics_disaggregate(
		route_id								
		,direction_id							
		,stop_id								
		,time_slice_id							
		,threshold_id							
		,threshold_name							
		,threshold_type							
		,total_predictions_within_threshold	
		,total_predictions_in_bin							
		,metric_result							
	)

	SELECT
		route_id
		,direction_id
		,stop_id
		,time_slice_id
		,cpt.threshold_id
		,cpt.threshold_name
		,cpt.threshold_type
		,SUM(prediction_within_threshold) AS total_predictions_within_threshold
		,SUM(prediction_in_bin) AS total_in_bin
		,SUM(prediction_within_threshold)/(SUM(prediction_in_bin)*1.0) AS metric_result
	FROM
		dbo.daily_prediction_threshold dpt
	JOIN dbo.config_prediction_threshold cpt
		ON 
				dpt.threshold_id = cpt.threshold_id
			AND
				dpt.route_type = cpt.route_type
	GROUP BY 
		route_id
		,direction_id
		,stop_id
		,time_slice_id
		,cpt.threshold_id
		,cpt.threshold_name
		,cpt.threshold_type
	ORDER BY 
		route_id
		,direction_id
		,stop_id
		,time_slice_id
		,cpt.threshold_id
		,cpt.threshold_name
		,cpt.threshold_type

	--write to historical table
	IF 
	(
		SELECT
			COUNT(*)
		FROM dbo.historical_prediction_metrics
		WHERE
			service_date = @service_date_process 
	) > 0

	DELETE FROM dbo.historical_prediction_metrics
	WHERE
		service_date = @service_date_process

	INSERT INTO dbo.historical_prediction_metrics
		(
			service_date
			,route_id								
			,threshold_id							
			,threshold_name							
			,threshold_type							
			,total_predictions_within_threshold	
			,total_predictions_in_bin							
			,metric_result							
		)

	SELECT 
			@service_date_process
			,route_id	
			,threshold_id
			,threshold_name
			,threshold_type
			,total_predictions_within_threshold
			,total_predictions_in_bin
			,metric_result
	
	FROM dbo.daily_prediction_metrics 

	IF 
	(
		SELECT
			COUNT(*)
		FROM dbo.historical_prediction_metrics_disaggregate
		WHERE
			service_date = @service_date_process 
	) > 0

	DELETE FROM dbo.historical_prediction_metrics_disaggregate
	WHERE
		service_date = @service_date_process

	INSERT INTO dbo.historical_prediction_metrics_disaggregate
	SELECT
		@service_date_process
		,route_id
		,direction_id
		,stop_id
		,time_slice_id
		,threshold_id
		,threshold_name
		,threshold_type
		,total_predictions_within_threshold
		,total_predictions_in_bin
		,metric_result
	FROM daily_prediction_metrics_disaggregate


END

GO
