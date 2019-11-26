
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('CreateTodayRTProcess','P') IS NOT NULL
	DROP PROCEDURE dbo.CreateTodayRTProcess
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.CreateTodayRTProcess

--Script Version: Master - 1.1.0.0

--This procedure sets up the today_rt tables. These tables store the real-time information for today's events. 
--They are updated in real-time by the ProcessRTEvent stored procedure that runs every minute via the ProcessRTEvent Job.

AS


BEGIN
	SET NOCOUNT ON;

	--Create today_rt_event table. This table stores today's events in real-time
	IF OBJECT_ID('dbo.today_rt_event','U') IS NOT NULL
		DROP TABLE dbo.today_rt_event
		;

	CREATE TABLE dbo.today_rt_event
	(
		record_id		INT				NOT NULL
		,service_date	DATE			NOT NULL
		,file_time		INT				NOT NULL
		,route_id		VARCHAR(255)	NOT NULL
		,trip_id		VARCHAR(255)	NOT NULL
		,direction_id	INT				NOT NULL
		,stop_id		VARCHAR(255)	NOT NULL
		,stop_sequence	INT				NOT NULL
		,vehicle_id		VARCHAR(255)	NOT NULL
		,vehicle_label	VARCHAR(255)
		,event_type		CHAR(3)			NOT NULL
		,event_time		INT				NOT NULL
		,event_time_sec	INT				NOT NULL
	)
	;
	CREATE NONCLUSTERED INDEX IX_today_rt_event_1
	ON dbo.today_rt_event (service_date,trip_id,direction_id,stop_id,stop_sequence,vehicle_id,event_type,event_time_sec)
	INCLUDE (record_id,route_id)


	CREATE NONCLUSTERED INDEX IX_today_rt_event_2
	ON dbo.today_rt_event (service_date,route_id,direction_id,stop_id,event_type,event_time_sec)
	INCLUDE (record_id,trip_id,stop_sequence,vehicle_id)


	CREATE NONCLUSTERED INDEX IX_today_rt_event_3
	ON dbo.today_rt_event (event_type)
	INCLUDE (service_date,route_id,trip_id,stop_id,vehicle_id,event_time_sec)


	CREATE NONCLUSTERED INDEX IX_today_rt_event_4
	ON dbo.today_rt_event (service_date,trip_id,direction_id,vehicle_id,event_type,stop_sequence,event_time_sec)
	INCLUDE (record_id,route_id,stop_id)


	CREATE NONCLUSTERED INDEX IX_today_rt_event_5
	ON dbo.today_rt_event (service_date,direction_id,stop_id,event_type,event_time_sec)
	INCLUDE (record_id,route_id,trip_id,stop_sequence,vehicle_id)

	--Create today_rt_cd_time table. This table stores today's dwell times in real-time
	IF OBJECT_ID('dbo.today_rt_cd_time','U') IS NOT NULL
		DROP TABLE dbo.today_rt_cd_time
		;

	CREATE TABLE dbo.today_rt_cd_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,cd_stop_id			VARCHAR(255)	NOT NULL
		,cd_stop_sequence	INT				NOT NULL
		,cd_direction_id	INT				NOT NULL
		,cd_route_id		VARCHAR(255)	NOT NULL
		,cd_trip_id			VARCHAR(255)	NOT NULL
		,cd_vehicle_id		VARCHAR(255)	NOT NULL
		,c_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,cd_time_sec		INT				NOT NULL
	)

	CREATE NONCLUSTERED INDEX IX_today_rt_cd_time_1
	ON dbo.today_rt_cd_time (service_date,d_record_id)
	INCLUDE (cd_stop_id,cd_stop_sequence,cd_direction_id,cd_route_id,cd_trip_id,cd_vehicle_id,c_record_id,c_time_sec,d_time_sec,cd_time_sec)


	--Create today_rt_de_time table. This table stores today's travel times in real-time
	IF OBJECT_ID('dbo.today_rt_de_time','U') IS NOT NULL
		DROP TABLE dbo.today_rt_de_time
		;

	CREATE TABLE dbo.today_rt_de_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,d_stop_id			VARCHAR(255)	NOT NULL
		,e_stop_id			VARCHAR(255)	NOT NULL
		,d_stop_sequence	INT				NOT NULL
		,e_stop_sequence	INT				NOT NULL
		,de_direction_id	INT				NOT NULL
		,de_route_id		VARCHAR(255)	NOT NULL
		,de_trip_id			VARCHAR(255)	NOT NULL
		,de_vehicle_id		VARCHAR(255)	NOT NULL
		,d_record_id		INT				NOT NULL
		,e_record_id		INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,e_time_sec			INT				NOT NULL
		,de_time_sec		INT				NOT NULL
	)
	;

	--Create today_rt_cde_time table. This table stores today's dwell times + travel times in real-time
	IF OBJECT_ID('dbo.today_rt_cde_time','U') IS NOT NULL
		DROP TABLE dbo.today_rt_cde_time
		;

	CREATE TABLE dbo.today_rt_cde_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,cd_stop_id			VARCHAR(255)	NOT NULL
		,e_stop_id			VARCHAR(255)	NOT NULL
		,cd_stop_sequence	INT				NOT NULL
		,e_stop_sequence	INT				NOT NULL
		,cde_direction_id	INT				NOT NULL
		,cde_route_id		VARCHAR(255)	NOT NULL
		,cde_trip_id		VARCHAR(255)	NOT NULL
		,cde_vehicle_id		VARCHAR(255)	NOT NULL
		,c_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,e_record_id		INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,e_time_sec			INT				NOT NULL
		,cd_time_sec		INT				NOT NULL
		,de_time_sec		INT				NOT NULL
	)
	;
	--create indexes to improve processing time
	CREATE NONCLUSTERED INDEX IX_today_rt_cde_time_c_record_id ON today_rt_cde_time (c_record_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_cde_time_d_record_id ON today_rt_cde_time (d_record_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_cde_time_e_record_id ON today_rt_cde_time (e_record_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_cde_time_d_time_sec ON today_rt_cde_time (d_time_sec);

	--Create today_rt_abcde_time table. This table stores today's joined events (abcde_time) in real-time
	IF OBJECT_ID('dbo.today_rt_abcde_time','U') IS NOT NULL
		DROP TABLE dbo.today_rt_abcde_time
		;

	CREATE TABLE dbo.today_rt_abcde_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,abcd_stop_id		VARCHAR(255)	NOT NULL
		,e_stop_id			VARCHAR(255)	NOT NULL
		,ab_stop_sequence	INT				NOT NULL
		,cd_stop_sequence	INT				NOT NULL
		,e_stop_sequence	INT				NOT NULL
		,abcde_direction_id	INT				NOT NULL
		,ab_route_id		VARCHAR(255)	NOT NULL
		,cde_route_id		VARCHAR(255)	NOT NULL
		,ab_trip_id			VARCHAR(255)	NOT NULL
		,cde_trip_id		VARCHAR(255)	NOT NULL
		,ab_vehicle_id		VARCHAR(255)	NOT NULL
		,cde_vehicle_id		VARCHAR(255)	NOT NULL
		,a_record_id		INT				NOT NULL
		,b_record_id		INT				NOT NULL
		,c_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,e_record_id		INT				NOT NULL
		,a_time_sec			INT				NOT NULL
		,b_time_sec			INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,e_time_sec			INT				NOT NULL
		,cd_time_sec		INT				NOT NULL
		,de_time_sec		INT				NOT NULL
		,bd_time_sec		INT				NOT NULL
	)
	;

	--Create today_rt_bd_sr_all_time table. This table stores today's headway times at a stop between trips of all routes in real-time
	IF OBJECT_ID('dbo.today_rt_bd_sr_all_time','U') IS NOT NULL
		DROP TABLE dbo.today_rt_bd_sr_all_time
		;

	CREATE TABLE dbo.today_rt_bd_sr_all_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,bd_stop_id			VARCHAR(255)	NOT NULL
		,b_stop_sequence	INT				NOT NULL
		,d_stop_sequence	INT				NOT NULL
		,b_route_id			VARCHAR(255)	NOT NULL
		,d_route_id			VARCHAR(255)	NOT NULL
		,bd_direction_id	INT				NOT NULL
		,b_trip_id			VARCHAR(255)	NOT NULL
		,d_trip_id			VARCHAR(255)	NOT NULL
		,b_vehicle_id		VARCHAR(255)	NOT NULL
		,d_vehicle_id		VARCHAR(255)	NOT NULL
		,b_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,b_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,bd_time_sec		INT				NOT NULL
	)
	;

	--Create today_rt_bd_sr_same_time table. This table stores today's headway times at a stop between trips of the same routes in real-time
	IF OBJECT_ID('dbo.today_rt_bd_sr_same_time','U') IS NOT NULL
		DROP TABLE dbo.today_rt_bd_sr_same_time
		;

	CREATE TABLE dbo.today_rt_bd_sr_same_time
	(
		service_date		VARCHAR(255)	NOT NULL
		,bd_stop_id			VARCHAR(255)	NOT NULL
		,b_stop_sequence	INT				NOT NULL
		,d_stop_sequence	INT				NOT NULL
		,bd_route_id		VARCHAR(255)	NOT NULL
		,bd_direction_id	INT				NOT NULL
		,b_trip_id			VARCHAR(255)	NOT NULL
		,d_trip_id			VARCHAR(255)	NOT NULL
		,b_vehicle_id		VARCHAR(255)	NOT NULL
		,d_vehicle_id		VARCHAR(255)	NOT NULL
		,b_record_id		INT				NOT NULL
		,d_record_id		INT				NOT NULL
		,b_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,bd_time_sec		INT				NOT NULL
	)

	--Create today_rt_travel_time_disaggregate. This table stores today's disaggregate travel times in real-time
	IF OBJECT_ID('dbo.today_rt_travel_time_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.today_rt_travel_time_disaggregate
		;

	CREATE TABLE dbo.today_rt_travel_time_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,from_stop_id				VARCHAR(255)	NOT NULL
		,to_stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,route_type					INT				NOT NULL 
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,travel_time_sec			INT				NOT NULL
		,benchmark_travel_time_sec	INT 
	)

	--Create today_rt_headway_time_od_disaggregate. This table stores today's disaggregate headway times at a stop between trips traveling to the same O-D pair in real-time
	IF OBJECT_ID('dbo.today_rt_headway_time_od_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.today_rt_headway_time_od_disaggregate
		;

	CREATE TABLE dbo.today_rt_headway_time_od_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,to_stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,prev_route_id				VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,headway_time_sec			INT				NOT NULL
		,benchmark_headway_time_sec	INT 
	)
	;
	CREATE NONCLUSTERED INDEX IX_today_rt_headway_time_od_disaggregate_1
	ON dbo.today_rt_headway_time_od_disaggregate (stop_id,to_stop_id)
	INCLUDE (service_date,route_id,prev_route_id,direction_id,start_time_sec,end_time_sec,headway_time_sec,benchmark_headway_time_sec)

	--Create today_rt_headway_time_sr_all_disaggregate. This table stores today's disaggregate heaway times at a stop between trips of all routes in real-time
	IF OBJECT_ID('dbo.today_rt_headway_time_sr_all_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.today_rt_headway_time_sr_all_disaggregate
		;

	CREATE TABLE dbo.today_rt_headway_time_sr_all_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,prev_route_id				VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,headway_time_sec			INT				NOT NULL
		,benchmark_headway_time_sec	INT 
	)
	;

	CREATE NONCLUSTERED INDEX IX_today_rt_headway_time_sr_all_disaggregate_1
	ON dbo.today_rt_headway_time_sr_all_disaggregate (stop_id)
	INCLUDE (service_date,route_id,prev_route_id,direction_id,start_time_sec,end_time_sec,headway_time_sec,benchmark_headway_time_sec)


	--Create today_rt_headway_time_sr_all_disaggregate. This table stores today's disaggregate heaway times at a stop between trips of the same route in real-time
	IF OBJECT_ID('dbo.today_rt_headway_time_sr_same_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.today_rt_headway_time_sr_same_disaggregate
		;

	CREATE TABLE dbo.today_rt_headway_time_sr_same_disaggregate
	(
		service_date				VARCHAR(255)	NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,prev_route_id				VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,start_time_sec				INT				NOT NULL
		,end_time_sec				INT				NOT NULL
		,headway_time_sec			INT				NOT NULL
		,benchmark_headway_time_sec	INT -- ADDED
	)
	;

	CREATE NONCLUSTERED INDEX IX_today_rt_headway_time_sr_same_disaggregate_1
	ON dbo.today_rt_headway_time_sr_same_disaggregate (stop_id,route_id)
	INCLUDE (service_date,prev_route_id,direction_id,start_time_sec,end_time_sec,headway_time_sec,benchmark_headway_time_sec)

	--Create today_rt_dwell_time_disaggregate. This table stores today's disaggregate dwell times in real-time
	IF OBJECT_ID('dbo.today_rt_dwell_time_disaggregate','u') IS NOT NULL
		DROP TABLE dbo.today_rt_dwell_time_disaggregate
		;

	CREATE TABLE dbo.today_rt_dwell_time_disaggregate
	(
		service_date	VARCHAR(255)	NOT NULL
		,stop_id		VARCHAR(255)	NOT NULL
		,route_id		VARCHAR(255)	NOT NULL
		,direction_id	INT				NOT NULL
		,start_time_sec	INT				NOT NULL
		,end_time_sec	INT				NOT NULL
		,dwell_time_sec	INT				NOT NULL
	)
	;

	--Create today_rt_travel_time_threshold_pax. This table stores the components to calculate today's passenger weighted travel time metrics
	IF OBJECT_ID('dbo.today_rt_travel_time_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.today_rt_travel_time_threshold_pax
	--
	CREATE TABLE dbo.today_rt_travel_time_threshold_pax
	(
		service_date									VARCHAR(255)	NOT NULL
		,from_stop_id									VARCHAR(255)	NOT NULL
		,to_stop_id										VARCHAR(255)	NOT NULL
		,direction_id									INT				NOT NULL
		,prev_route_id									VARCHAR(255)	NOT NULL
		,route_id										VARCHAR(255)	NOT NULL
		,trip_id										VARCHAR(255)	NOT NULL
		,start_time_sec									INT				NOT NULL
		,end_time_sec									INT				NOT NULL
		,travel_time_sec								INT				NOT NULL
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_historical_median_travel_time_sec	INT				NULL
		,threshold_scheduled_median_travel_time_sec		INT				NOT NULL
		,threshold_historical_average_travel_time_sec	INT				NULL 
		,threshold_scheduled_average_travel_time_sec	INT				NOT NULL 
		,denominator_pax								FLOAT			NULL
		,historical_threshold_numerator_pax				FLOAT			NULL
		,scheduled_threshold_numerator_pax				FLOAT			NULL
		,denominator_trip								FLOAT			NOT NULL 
		,historical_threshold_numerator_trip			FLOAT			NULL 
		,scheduled_threshold_numerator_trip				FLOAT			NOT NULL 
	)

	--create indexes to improve processing time
	CREATE NONCLUSTERED INDEX IX_today_rt_travel_time_threshold_pax_from_stop_id ON today_rt_travel_time_threshold_pax (from_stop_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_travel_time_threshold_pax_to_stop_id ON today_rt_travel_time_threshold_pax (to_stop_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_travel_time_threshold_pax_prev_route_id ON today_rt_travel_time_threshold_pax (prev_route_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_travel_time_threshold_pax_route_id ON today_rt_travel_time_threshold_pax (route_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_travel_time_threshold_pax_direction_id ON today_rt_travel_time_threshold_pax (direction_id);


	--Create today_rt_wait_time_od_threshold_pax. This table stores the components to calculate today's passenger weighted wait time metrics
	IF OBJECT_ID('dbo.today_rt_wait_time_od_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.today_rt_wait_time_od_threshold_pax
	--
	CREATE TABLE dbo.today_rt_wait_time_od_threshold_pax
	(
		service_date								VARCHAR(255)	NOT NULL
		,from_stop_id								VARCHAR(255)	NOT NULL
		,to_stop_id									VARCHAR(255)	NOT NULL
		,direction_id								INT				NOT NULL
		,prev_route_id								VARCHAR(255)	NOT NULL
		,route_id									VARCHAR(255)	NOT NULL
		,start_time_sec								INT				NOT NULL
		,end_time_sec								INT				NOT NULL
		,max_wait_time_sec							INT				NOT NULL
		,dwell_time_sec								INT				NOT NULL
		,threshold_id								VARCHAR(255)	NOT NULL
		,threshold_historical_median_wait_time_sec	INT				NULL
		,threshold_scheduled_median_wait_time_sec	INT				NOT NULL
		,threshold_historical_average_wait_time_sec	INT				NULL 
		,threshold_scheduled_average_wait_time_sec	INT				NOT NULL 
		,denominator_pax							FLOAT			NULL
		,historical_threshold_numerator_pax			FLOAT			NULL
		,scheduled_threshold_numerator_pax			FLOAT			NULL
		,denominator_trip							FLOAT			NOT NULL 
		,historical_threshold_numerator_trip		FLOAT			NULL 
		,scheduled_threshold_numerator_trip			FLOAT			NOT NULL 
	)

	--create indexes to improve processing time
	CREATE NONCLUSTERED INDEX IX_today_rt_wait_time_od_threshold_pax_from_stop_id ON today_rt_wait_time_od_threshold_pax (from_stop_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_wait_time_od_threshold_pax_to_stop_id ON today_rt_wait_time_od_threshold_pax (to_stop_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_wait_time_od_threshold_pax_prev_route_id ON today_rt_wait_time_od_threshold_pax (prev_route_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_wait_time_od_threshold_pax_route_id ON today_rt_wait_time_od_threshold_pax (route_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_wait_time_od_threshold_pax_direction_id ON today_rt_wait_time_od_threshold_pax (direction_id);

	---headway_threshold_trip
	IF OBJECT_ID('dbo.today_rt_headway_time_threshold_trip','U') IS NOT NULL
		DROP TABLE dbo.today_rt_headway_time_threshold_trip
	--
	CREATE TABLE dbo.today_rt_headway_time_threshold_trip
	(
		service_date									VARCHAR(255)	NOT NULL
		,stop_id										VARCHAR(255)	NOT NULL
		,direction_id									INT				NOT NULL
		,prev_route_id									VARCHAR(255)	NOT NULL
		,route_id										VARCHAR(255)	NOT NULL
		,start_time_sec									INT				NOT NULL
		,end_time_sec									INT				NOT NULL
		,headway_time_sec								INT				NOT NULL
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_scheduled_median_headway_time_sec	INT				NOT NULL
		,threshold_scheduled_average_headway_time_sec	INT				NOT NULL 
		,denominator_trip								FLOAT			NOT NULL 
		,scheduled_threshold_numerator_trip				FLOAT			NOT NULL 
	)

	CREATE NONCLUSTERED INDEX IX_today_rt_headway_time_threshold_trip_stop_id ON today_rt_headway_time_threshold_trip (stop_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_headway_time_threshold_trip_prev_route_id ON today_rt_headway_time_threshold_trip (prev_route_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_headway_time_threshold_trip_route_id ON today_rt_headway_time_threshold_trip (route_id);

	CREATE NONCLUSTERED INDEX IX_today_rt_headway_time_threshold_trip_direction_id ON today_rt_headway_time_threshold_trip (direction_id);

	--save disaggreagate schedule adherence 
	IF OBJECT_ID('dbo.today_rt_schedule_adherence_disaggregate','U') IS NOT NULL
		DROP TABLE dbo.today_rt_schedule_adherence_disaggregate

	CREATE TABLE dbo.today_rt_schedule_adherence_disaggregate
	(
		service_date					VARCHAR(255)	NOT NULL
		,route_id						VARCHAR(255)	NOT NULL
		,route_type						INT				NOT NULL
		,direction_id					INT				NOT NULL
		,trip_id						VARCHAR(255)	NOT NULL
		,stop_sequence					INT				NOT NULL
		,stop_id						VARCHAR(255)	NOT NULL
		,vehicle_id						VARCHAR(255)
		,scheduled_arrival_time_sec		INT
		,actual_arrival_time_sec		INT
		,arrival_delay_sec				INT
		,scheduled_departure_time_sec	INT
		,actual_departure_time_sec		INT
		,departure_delay_sec			INT
		,stop_order_flag				INT -- 1 is first stop, 2 is mid stop, 3 is last stop
	)

	--Create today_rt_schedule_adherence_threshold_pax. This table stores the components to calculate today's passenger weighted wait time metrics for commuter rail
	IF OBJECT_ID('dbo.today_rt_schedule_adherence_threshold_pax','U') IS NOT NULL
		DROP TABLE dbo.today_rt_schedule_adherence_threshold_pax
		;

	CREATE TABLE dbo.today_rt_schedule_adherence_threshold_pax
	(
		service_date						VARCHAR(255)	NOT NULL
		,route_id							VARCHAR(255)	NOT NULL
		,direction_id						INT				NOT NULL
		,trip_id							VARCHAR(255)	NOT NULL
		,stop_sequence						INT				NOT NULL
		,stop_id							VARCHAR(255)	NOT NULL
		,vehicle_id							VARCHAR(255)
		,scheduled_arrival_time_sec			INT
		,actual_arrival_time_sec			INT
		,arrival_delay_sec					INT
		,scheduled_departure_time_sec		INT
		,actual_departure_time_sec			INT
		,departure_delay_sec				INT
		,stop_order_flag					INT -- 1 is first stop, 2 is mid stop, 3 is last stop
		,threshold_id						VARCHAR(255)	NOT NULL
		,threshold_value					INT
		,denominator_pax					FLOAT			NULL
		,scheduled_threshold_numerator_pax	FLOAT			NULL
		,denominator_trip					FLOAT			NOT NULL
		,scheduled_threshold_numerator_trip	FLOAT			NOT NULL

	)


	IF OBJECT_ID('dbo.today_rt_current_metrics','U') IS NOT NULL
		DROP TABLE dbo.today_rt_current_metrics

	CREATE TABLE dbo.today_rt_current_metrics
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


END





GO