--Only run this at the very start of setting up the system 

---run this script in the transit-performance database
--USE transit_performance
--GO

--create service_date table  
IF OBJECT_ID('dbo.service_date','U') IS NOT NULL
	DROP TABLE dbo.service_date

CREATE TABLE dbo.service_date
(
	service_date			DATE			NOT NULL PRIMARY KEY
	,day_of_the_week		VARCHAR(255)	NOT NULL
	,day_type_id			VARCHAR(255)	NOT NULL
	,day_type_id_exception	VARCHAR(255)	NULL
)

--Create Event Time Table which stores arrival and departure events and times 
IF OBJECT_ID('dbo.rt_event','U') IS NOT NULL
	DROP TABLE dbo.rt_event

GO

CREATE TABLE dbo.rt_event
(
	record_id				INT	IDENTITY PRIMARY KEY
	,service_date			DATE
	,file_time				INT
	,route_id				VARCHAR(255)
	,trip_id				VARCHAR(255)
	,direction_id			INT
	,stop_id				VARCHAR(255)
	,stop_sequence			INT
	,vehicle_id				VARCHAR(255)
	,event_type				CHAR(3)
	,event_time				INT
	,event_time_sec			INT
	,event_processed_rt		BIT	DEFAULT 0
	,event_processed_daily	BIT	DEFAULT 0
	,vehicle_label			VARCHAR(255)
)

CREATE NONCLUSTERED INDEX IX_rt_event_service_date
ON rt_event (service_date);

CREATE NONCLUSTERED INDEX IX_rt_event_route_id
ON rt_event (route_id);

CREATE NONCLUSTERED INDEX IX_rt_event_trip_id
ON rt_event (trip_id);

CREATE NONCLUSTERED INDEX IX_rt_event_direction_id
ON rt_event (direction_id);

CREATE NONCLUSTERED INDEX IX_rt_event_stop_id
ON rt_event (stop_id);

CREATE NONCLUSTERED INDEX IX_rt_event_stop_sequence
ON rt_event (stop_sequence);

CREATE NONCLUSTERED INDEX IX_rt_event_vehicle_id
ON rt_event (vehicle_id);

CREATE NONCLUSTERED INDEX IX_rt_event_event_type
ON rt_event (event_type);

CREATE NONCLUSTERED INDEX IX_rt_event_event_time
ON rt_event (event_time);

CREATE NONCLUSTERED INDEX IX_rt_event_event_time_sec
ON rt_event (event_time_sec);

CREATE NONCLUSTERED INDEX IX_rt_event_event_processed_rt
ON rt_event (event_processed_rt);

CREATE NONCLUSTERED INDEX IX_rt_event_event_processed_daily
ON rt_event (event_processed_daily);

CREATE NONCLUSTERED INDEX IX_rt_event_index_1
ON dbo.rt_event (service_date,event_processed_rt)
INCLUDE (record_id,file_time,route_id,trip_id,direction_id,stop_id,
stop_sequence,vehicle_id,event_type,event_time,event_time_sec)

CREATE NONCLUSTERED INDEX IX_rt_event_index_2
ON dbo.rt_event (service_date,event_time_sec)
INCLUDE (record_id,event_time)

CREATE NONCLUSTERED INDEX IX_rt_event_index_3
ON dbo.rt_event (service_date)
INCLUDE (record_id,file_time,route_id,trip_id,direction_id,stop_id,
stop_sequence,vehicle_id,event_type,event_time,event_time_sec,
event_processed_rt,event_processed_daily)

-- create event_rt_trip and event_rt_trip archive tables to store latest predicted times  
IF OBJECT_ID('dbo.event_rt_trip','U') IS NOT NULL
	DROP TABLE dbo.event_rt_trip;

CREATE TABLE dbo.event_rt_trip
(
	service_date		DATE			
	,file_time			INT				
	,route_id			VARCHAR(255)	
	,trip_id			VARCHAR(255)	
	,direction_id		INT				
	,stop_id			VARCHAR(255)	
	,stop_sequence		INT				
	,vehicle_id			VARCHAR(255)	
	,event_type			CHAR(3)			
	,event_time			INT				
	,event_identifier	VARCHAR(255)
	,vehicle_label		VARCHAR(255)	
);

IF OBJECT_ID('dbo.event_rt_trip_archive','U') IS NOT NULL
	DROP TABLE dbo.event_rt_trip_archive;

CREATE TABLE dbo.event_rt_trip_archive
(
	service_date		DATE			
	,file_time			INT				
	,route_id			VARCHAR(255)	
	,trip_id			VARCHAR(255)	
	,direction_id		INT				
	,stop_id			VARCHAR(255)	
	,stop_sequence		INT				
	,vehicle_id			VARCHAR(255)	
	,event_type			CHAR(3)			
	,event_time			INT				
	,event_identifier	VARCHAR(255)
	,vehicle_label		VARCHAR(255)	
);

CREATE NONCLUSTERED INDEX IX_event_rt_trip_archive_index_1
ON dbo.event_rt_trip_archive (service_date,direction_id)
INCLUDE (trip_id)

CREATE NONCLUSTERED INDEX IX_event_rt_trip_archive_index_2
ON dbo.event_rt_trip_archive (service_date,direction_id)
INCLUDE (route_id,stop_id)

CREATE NONCLUSTERED INDEX IX_event_rt_trip_archive_index_3
ON dbo.event_rt_trip_archive (service_date,event_type,event_time)
INCLUDE (file_time,route_id,trip_id,direction_id,stop_id,stop_sequence,
vehicle_id)

-- create alert tables

IF OBJECT_ID('rt_alert','U') IS NOT NULL
DROP TABLE rt_alert

CREATE TABLE rt_alert
(
	record_id					INT IDENTITY
	,file_time					INT NOT NULL
	,alert_id					VARCHAR(255) NOT NULL
	,version_id					INT NOT NULL	
	,cause						VARCHAR(255)
	,effect						VARCHAR(255)
	,header_text				VARCHAR(255)
	,description_text			VARCHAR(3100)
	,url						VARCHAR(255)
	,closed						BIT NOT NULL DEFAULT 0
	,PRIMARY KEY (alert_id, version_id)
)

IF OBJECT_ID('rt_alert_active_period','U') IS NOT NULL
DROP TABLE rt_alert_active_period

CREATE TABLE rt_alert_active_period
(
	alert_id				VARCHAR(255) NOT NULL
	,version_id				INT NOT NULL
	,active_period_start	INT NULL
	,active_period_end		INT NULL
)

CREATE NONCLUSTERED INDEX IX_rt_alert_active_period_1
ON dbo.rt_alert_active_period (active_period_start,active_period_end)
INCLUDE (alert_id,version_id)

CREATE NONCLUSTERED INDEX IX_rt_alert_active_period_2
ON dbo.rt_alert_active_period (alert_id,version_id)

IF OBJECT_ID('rt_alert_informed_entity','U') IS NOT NULL
DROP TABLE rt_alert_informed_entity

CREATE TABLE rt_alert_informed_entity
(
	alert_id				VARCHAR(255) NOT NULL
	,version_id				INT NOT NULL
	,agency_id				VARCHAR(255) NULL
	,route_id				VARCHAR(255) NULL
	,route_type				INT NULL
	,trip_id				VARCHAR(255) NULL
	,stop_id				VARCHAR(255) NULL
)

CREATE NONCLUSTERED INDEX IX_rt_alert_informed_entity_1
ON dbo.rt_alert_informed_entity (alert_id,version_id)
INCLUDE (route_id,trip_id,stop_id)

-- create gtfsrt_tripupdate_denormalized to store all trip update data
IF OBJECT_ID('dbo.gtfsrt_tripupdate_denormalized', 'U') IS NOT NULL
  DROP TABLE dbo.gtfsrt_tripupdate_denormalized
;

CREATE TABLE dbo.gtfsrt_tripupdate_denormalized(
	gtfs_realtime_version				VARCHAR(255)
	,incrementality						VARCHAR(255)
	,header_timestamp					INT NOT NULL
	,feed_entity_id						VARCHAR(255)
	,trip_id							VARCHAR(255) NOT NULL
	,trip_delay							INT
	,route_id							VARCHAR(255) 
	,direction_id						INT 
	,trip_start_date					CHAR(8) 
	,trip_start_time					VARCHAR(8)
	,trip_schedule_relationship			VARCHAR(255)
	,vehicle_id							VARCHAR(255)
	,vehicle_label						VARCHAR(255)
	,vehicle_license_plate				VARCHAR(255)
	,vehicle_timestamp					INT
	,stop_id							VARCHAR(255)
	,stop_sequence						INT
	,predicted_arrival_time				INT
	,predicted_arrival_delay			INT
	,predicted_arrival_uncertainty		INT	
	,predicted_departure_time			INT
	,predicted_departure_delay			INT
	,predicted_departure_uncertainty	INT	
	,stop_schedule_relationship			VARCHAR(255)
	)

CREATE NONCLUSTERED INDEX IX_gtfsrt_tripupdate_denormalized_start_date ON gtfsrt_tripupdate_denormalized(trip_start_date);

-- Create all historical tables 
IF OBJECT_ID('dbo.historical_event','U') IS NOT NULL
	DROP TABLE dbo.historical_event;

CREATE TABLE dbo.historical_event
(
	record_id				INT				NOT NULL
	,service_date			DATE			NOT NULL
	,file_time				INT				NOT NULL
	,route_id				VARCHAR(255)	NOT NULL
	,route_type				INT				NULL
	,trip_id				VARCHAR(255)	NOT NULL
	,direction_id			INT				NOT NULL
	,stop_id				VARCHAR(255)	NOT NULL
	,stop_sequence			INT				NOT NULL
	,vehicle_id				VARCHAR(255)	NOT NULL
	,event_type				CHAR(3)			NOT NULL
	,event_time				INT				NOT NULL
	,event_time_sec			INT				NOT NULL
	,event_processed_rt		BIT				NOT NULL
	,event_processed_daily	BIT				NOT NULL
	,suspect_record			BIT				NOT NULL
	,vehicle_label			VARCHAR(255)	
);

CREATE NONCLUSTERED INDEX IX_historical_event_service_date
ON dbo.historical_event (service_date)

CREATE NONCLUSTERED INDEX IX_historical_event_1
ON dbo.historical_event (service_date,suspect_record,event_time)
INCLUDE (route_id,trip_id,direction_id,stop_id,stop_sequence,vehicle_id,event_type,event_time_sec,vehicle_label)

IF OBJECT_ID('dbo.historical_travel_time_disaggregate','U') IS NOT NULL
	DROP TABLE dbo.historical_travel_time_disaggregate;

CREATE TABLE dbo.historical_travel_time_disaggregate
(
	service_date				VARCHAR(255)	NOT NULL
	,from_stop_id				VARCHAR(255)	NOT NULL
	,to_stop_id					VARCHAR(255)	NOT NULL
	,route_id					VARCHAR(255)	NOT NULL
	,route_type					INT				NULL
	,direction_id				INT				NOT NULL
	,start_time_sec				INT				NOT NULL
	,end_time_sec				INT				NOT NULL
	,travel_time_sec			INT				NOT NULL
	,benchmark_travel_time_sec	INT
)

CREATE NONCLUSTERED INDEX IX_historical_travel_time_service_date
ON dbo.historical_travel_time_disaggregate (service_date)

CREATE NONCLUSTERED INDEX IX_historical_travel_time_disaggregate_index_1
ON dbo.historical_travel_time_disaggregate (from_stop_id,to_stop_id,
direction_id,end_time_sec)
INCLUDE (service_date,travel_time_sec)

CREATE NONCLUSTERED INDEX IX_historical_travel_time_disaggregate_index_2
ON dbo.historical_travel_time_disaggregate (from_stop_id,to_stop_id,
route_type)
INCLUDE (service_date,route_id,direction_id,start_time_sec,end_time_sec,
travel_time_sec,benchmark_travel_time_sec)

IF OBJECT_ID('dbo.historical_headway_time_od_disaggregate','U') IS NOT NULL
	DROP TABLE dbo.historical_headway_time_od_disaggregate;

CREATE TABLE dbo.historical_headway_time_od_disaggregate
(
	service_date				VARCHAR(255)	NOT NULL
	,stop_id					VARCHAR(255)	NOT NULL
	,to_stop_id					VARCHAR(255)	NOT NULL
	,route_id					VARCHAR(255)	NOT NULL
	,prev_route_id				VARCHAR(255)	NOT NULL
	,route_type					INT				NULL
	,direction_id				INT				NOT NULL
	,start_time_sec				INT				NOT NULL
	,end_time_sec				INT				NOT NULL
	,headway_time_sec			INT				NOT NULL
	,benchmark_headway_time_sec	INT
);

CREATE NONCLUSTERED INDEX
IX_historical_headway_time_od_disaggregate_service_date
ON dbo.historical_headway_time_od_disaggregate (service_date)

CREATE NONCLUSTERED INDEX IX_historical_headway_time_od_disaggregate_index_1
ON dbo.historical_headway_time_od_disaggregate (stop_id,to_stop_id)
INCLUDE (service_date,route_id,prev_route_id,direction_id,start_time_sec,
end_time_sec,headway_time_sec,benchmark_headway_time_sec)

CREATE NONCLUSTERED INDEX IX_historical_headway_time_od_disaggregate_index_2
ON dbo.historical_headway_time_od_disaggregate (service_date,stop_id)
INCLUDE (to_stop_id,route_id,prev_route_id,direction_id,start_time_sec,
end_time_sec,headway_time_sec,benchmark_headway_time_sec,route_type)

CREATE NONCLUSTERED INDEX IX_historical_headway_time_od_disaggregate_index_3
ON dbo.historical_headway_time_od_disaggregate (stop_id,to_stop_id,
direction_id,end_time_sec)
INCLUDE (service_date,headway_time_sec)

IF OBJECT_ID('dbo.historical_headway_time_sr_all_disaggregate','U') IS NOT NULL
	DROP TABLE dbo.historical_headway_time_sr_all_disaggregate;

CREATE TABLE dbo.historical_headway_time_sr_all_disaggregate
(
	service_date				VARCHAR(255)	NOT NULL
	,stop_id					VARCHAR(255)	NOT NULL
	,route_id					VARCHAR(255)	NOT NULL
	,prev_route_id				VARCHAR(255)	NOT NULL
	,route_type					INT				NULL
	,direction_id				INT				NOT NULL
	,start_time_sec				INT				NOT NULL
	,end_time_sec				INT				NOT NULL
	,headway_time_sec			INT				NOT NULL
	,benchmark_headway_time_sec	INT
);

CREATE NONCLUSTERED INDEX
IX_historical_headway_time_sr_all_disaggregate_service_date
ON dbo.historical_headway_time_sr_all_disaggregate (service_date)

CREATE NONCLUSTERED INDEX IX_historical_headway_time_sr_all_disaggregate_index_1
ON dbo.historical_headway_time_sr_all_disaggregate (stop_id)
INCLUDE (service_date,route_id,prev_route_id,direction_id,start_time_sec,
end_time_sec,headway_time_sec,benchmark_headway_time_sec)

CREATE NONCLUSTERED INDEX IX_historical_headway_time_sr_all_disaggregate_index_2
ON dbo.historical_headway_time_sr_all_disaggregate (service_date,stop_id)
INCLUDE (route_id,prev_route_id,direction_id,start_time_sec,end_time_sec,
headway_time_sec,benchmark_headway_time_sec,route_type)

IF OBJECT_ID('dbo.historical_headway_time_sr_same_disaggregate','U') IS NOT
	NULL
	DROP TABLE dbo.historical_headway_time_sr_same_disaggregate;

CREATE TABLE dbo.historical_headway_time_sr_same_disaggregate
(
	service_date				VARCHAR(255)	NOT NULL
	,stop_id					VARCHAR(255)	NOT NULL
	,route_id					VARCHAR(255)	NOT NULL
	,prev_route_id				VARCHAR(255)
	,route_type					INT				NULL
	,direction_id				INT				NOT NULL
	,start_time_sec				INT				NOT NULL
	,end_time_sec				INT				NOT NULL
	,headway_time_sec			INT				NOT NULL
	,benchmark_headway_time_sec	INT
);

CREATE NONCLUSTERED INDEX
IX_historical_headway_time_sr_same_disaggregate_service_date
ON dbo.historical_headway_time_sr_same_disaggregate (service_date)

CREATE NONCLUSTERED INDEX
IX_historical_headway_time_sr_same_disaggregate_index_1
ON dbo.historical_headway_time_sr_same_disaggregate (stop_id,route_id)
INCLUDE (service_date,prev_route_id,direction_id,start_time_sec,
end_time_sec,headway_time_sec,benchmark_headway_time_sec)

CREATE NONCLUSTERED INDEX
IX_historical_headway_time_sr_same_disaggregate_index_2
ON dbo.historical_headway_time_sr_same_disaggregate (service_date,stop_id)
INCLUDE (route_id,direction_id,start_time_sec,end_time_sec,
headway_time_sec,benchmark_headway_time_sec,prev_route_id,route_type)

IF OBJECT_ID('dbo.historical_dwell_time_disaggregate','U') IS NOT NULL
	DROP TABLE dbo.historical_dwell_time_disaggregate;

CREATE TABLE dbo.historical_dwell_time_disaggregate
(
	service_date	VARCHAR(255)	NOT NULL
	,stop_id		VARCHAR(255)	NOT NULL
	,route_id		VARCHAR(255)	NOT NULL
	,direction_id	INT				NOT NULL
	,start_time_sec	INT				NOT NULL
	,end_time_sec	INT				NOT NULL
	,dwell_time_sec	INT				NOT NULL
);

CREATE NONCLUSTERED INDEX IX_historical_dwell_time_disaggregate_service_date
ON dbo.historical_dwell_time_disaggregate (service_date)

CREATE NONCLUSTERED INDEX IX_historical_dwell_time_disaggregate_index_1
ON dbo.historical_dwell_time_disaggregate (stop_id)
INCLUDE (service_date,route_id,direction_id,start_time_sec,end_time_sec,
dwell_time_sec)

IF OBJECT_ID('dbo.historical_travel_time_threshold_pax','U') IS NOT NULL
	DROP TABLE dbo.historical_travel_time_threshold_pax;

CREATE TABLE dbo.historical_travel_time_threshold_pax
(
	service_date									VARCHAR(255)	NOT NULL
	,from_stop_id									VARCHAR(255)	NOT NULL
	,to_stop_id										VARCHAR(255)	NOT NULL
	,direction_id									INT				NOT NULL
	,prev_route_id									VARCHAR(255)	NOT NULL
	,route_id										VARCHAR(255)	NOT NULL
	,start_time_sec									INT				NOT NULL
	,end_time_sec									INT				NOT NULL
	,travel_time_sec								INT				NOT NULL
	,threshold_id									VARCHAR(255)	NOT NULL
	,threshold_historical_median_travel_time_sec	INT				NULL
	,threshold_scheduled_median_travel_time_sec		INT				NULL
	,threshold_historical_average_travel_time_sec	INT				NULL
	,threshold_scheduled_average_travel_time_sec	INT				NULL
	,denominator_pax								FLOAT			NULL
	,historical_threshold_numerator_pax				FLOAT			NULL
	,scheduled_threshold_numerator_pax				FLOAT			NULL
	,denominator_trip								FLOAT			NULL
	,historical_threshold_numerator_trip			FLOAT			NULL
	,scheduled_threshold_numerator_trip				FLOAT			NULL
)

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_from_stop_id
ON historical_travel_time_threshold_pax (from_stop_id);

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_to_stop_id
ON historical_travel_time_threshold_pax (to_stop_id);

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_prev_route_id
ON historical_travel_time_threshold_pax (prev_route_id);

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_route_id
ON historical_travel_time_threshold_pax (route_id);

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_direction_id
ON historical_travel_time_threshold_pax (direction_id);

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_end_time_sec
ON historical_travel_time_threshold_pax (end_time_sec);

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_threshold_id
ON historical_travel_time_threshold_pax (threshold_id);

CREATE NONCLUSTERED INDEX IX_historical_travel_time_threshold_pax_service_date
ON dbo.historical_travel_time_threshold_pax (service_date)

IF OBJECT_ID('dbo.historical_wait_time_od_threshold_pax','U') IS NOT NULL
	DROP TABLE dbo.historical_wait_time_od_threshold_pax

CREATE TABLE dbo.historical_wait_time_od_threshold_pax
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
	,threshold_scheduled_median_wait_time_sec	INT				NULL
	,threshold_historical_average_wait_time_sec	INT				NULL
	,threshold_scheduled_average_wait_time_sec	INT				NULL
	,denominator_pax							FLOAT			NULL
	,historical_threshold_numerator_pax			FLOAT			NULL
	,scheduled_threshold_numerator_pax			FLOAT			NULL
	,denominator_trip							FLOAT			NULL
	,historical_threshold_numerator_trip		FLOAT			NULL
	,scheduled_threshold_numerator_trip			FLOAT			NULL
)

CREATE NONCLUSTERED INDEX IX_historical_wait_time_od_threshold_pax_from_stop_id
ON historical_wait_time_od_threshold_pax (from_stop_id);

CREATE NONCLUSTERED INDEX IX_historical_wait_time_od_threshold_pax_to_stop_id
ON historical_wait_time_od_threshold_pax (to_stop_id);

CREATE NONCLUSTERED INDEX IX_historical_wait_time_od_threshold_pax_prev_route_id
ON historical_wait_time_od_threshold_pax (prev_route_id);

CREATE NONCLUSTERED INDEX IX_historical_wait_time_od_threshold_pax_route_id
ON historical_wait_time_od_threshold_pax (route_id);

CREATE NONCLUSTERED INDEX IX_historical_wait_time_od_threshold_pax_direction_id
ON historical_wait_time_od_threshold_pax (direction_id);

CREATE NONCLUSTERED INDEX IX_historical_wait_time_od_threshold_pax_end_time_sec
ON historical_wait_time_od_threshold_pax (end_time_sec);

CREATE NONCLUSTERED INDEX IX_historical_wait_time_od_threshold_pax_service_date
ON [dbo].[historical_wait_time_od_threshold_pax] (service_date)

IF OBJECT_ID('dbo.historical_metrics','U') IS NOT NULL
	DROP TABLE dbo.historical_metrics

CREATE TABLE dbo.historical_metrics
(
	service_date		VARCHAR(255)	NOT NULL
	,route_id			VARCHAR(255)	NOT NULL
	,threshold_id		VARCHAR(255)	NOT NULL
	,threshold_name		VARCHAR(255)	NOT NULL
	,threshold_type		VARCHAR(255)	NOT NULL
	,time_period_type	VARCHAR(255)	NOT NULL
	,metric_result		FLOAT
	,metric_result_trip	FLOAT
	,numerator_pax		FLOAT
	,denominator_pax	FLOAT
	,numerator_trip		FLOAT
	,denominator_trip	FLOAT
)

CREATE NONCLUSTERED INDEX IX_historical_metrics_service_date
ON dbo.historical_metrics (service_date);

IF OBJECT_ID('dbo.historical_prediction_metrics','U') IS NOT NULL
	DROP TABLE dbo.historical_prediction_metrics

CREATE TABLE dbo.historical_prediction_metrics
(
		service_date							VARCHAR(255) NOT NULL
		,route_id								VARCHAR(255) NOT NULL
		,threshold_id							VARCHAR(255) NOT NULL
		,threshold_name							VARCHAR(255) NOT NULL
		,threshold_type							VARCHAR(255)
		,total_predictions_within_threshold		INT
		,total_predictions_in_bin				INT
		,metric_result							FLOAT
)

CREATE NONCLUSTERED INDEX IX_historical_prediction_metrics_service_date
ON dbo.historical_prediction_metrics (service_date);

IF OBJECT_ID('dbo.historical_prediction_metrics_disaggregate','U') IS NOT NULL
	DROP TABLE dbo.historical_prediction_metrics_disaggregate

CREATE TABLE dbo.historical_prediction_metrics_disaggregate
(
		service_date							VARCHAR(255) NOT NULL
		,route_id								VARCHAR(255) NOT NULL
		,direction_id							INT NOT NULL
		,stop_id								VARCHAR(255) NOT NULL
		,time_slice_id							VARCHAR(255) NOT NULL
		,threshold_id							VARCHAR(255) NOT NULL
		,threshold_name							VARCHAR(255) NOT NULL
		,threshold_type							VARCHAR(255)
		,total_predictions_within_threshold		INT
		,total_predictions_in_bin				INT
		,metric_result							FLOAT
)

CREATE NONCLUSTERED INDEX IX_historical_prediction_metrics_disaggregate_service_date
ON dbo.historical_prediction_metrics_disaggregate (service_date);

IF OBJECT_ID('dbo.historical_schedule_adherence_disaggregate','U') IS NOT NULL
	DROP TABLE dbo.historical_schedule_adherence_disaggregate;

CREATE TABLE dbo.historical_schedule_adherence_disaggregate
(
	service_date					VARCHAR(255)	NOT NULL
	,route_id						VARCHAR(255)	NOT NULL
	,route_type						INT
	,direction_id					INT				NOT NULL
	,trip_id						VARCHAR(255)	NOT NULL
	,stop_sequence					INT				NOT NULL
	,stop_id						VARCHAR(255)	NOT NULL
	,vehicle_id						VARCHAR(255)	NOT NULL
	,scheduled_arrival_time_sec		INT
	,actual_arrival_time_sec		INT
	,arrival_delay_sec				INT
	,scheduled_departure_time_sec	INT
	,actual_departure_time_sec		INT
	,departure_delay_sec			INT
	,stop_order_flag				INT
)

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_disaggregate_service_date
ON dbo.historical_schedule_adherence_disaggregate (service_date)

CREATE NONCLUSTERED INDEX IX_historical_schedule_adherence_disaggregate_route_id
ON historical_schedule_adherence_disaggregate (route_id);

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_disaggregate_direction_id
ON historical_schedule_adherence_disaggregate (direction_id);

CREATE NONCLUSTERED INDEX IX_historical_schedule_adherence_disaggregate_stop_id
ON historical_schedule_adherence_disaggregate (stop_id);

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_disaggregate_actual_arrival_time_sec
ON historical_schedule_adherence_disaggregate (actual_arrival_time_sec);

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_disaggregate_actual_departure_time_sec
ON historical_schedule_adherence_disaggregate (actual_departure_time_sec);

CREATE NONCLUSTERED INDEX IX_historical_schedule_adherence_disaggregate_index_1
ON dbo.historical_schedule_adherence_disaggregate (stop_id)
INCLUDE (service_date,route_id,direction_id,trip_id,
scheduled_arrival_time_sec,actual_arrival_time_sec,arrival_delay_sec,
scheduled_departure_time_sec,actual_departure_time_sec,departure_delay_sec,
stop_order_flag)

IF OBJECT_ID('dbo.historical_schedule_adherence_threshold_pax','U') IS NOT NULL
	DROP TABLE dbo.historical_schedule_adherence_threshold_pax;

CREATE TABLE dbo.historical_schedule_adherence_threshold_pax
(
	service_date						VARCHAR(255)	NOT NULL
	,route_id							VARCHAR(255)	NOT NULL
	,route_type							INT
	,direction_id						INT				NOT NULL
	,trip_id							VARCHAR(255)	NOT NULL
	,stop_sequence						INT				NOT NULL
	,stop_id							VARCHAR(255)	NOT NULL
	,vehicle_id							VARCHAR(255)	NOT NULL
	,scheduled_arrival_time_sec			INT
	,actual_arrival_time_sec			INT
	,arrival_delay_sec					INT
	,scheduled_departure_time_sec		INT
	,actual_departure_time_sec			INT
	,departure_delay_sec				INT
	,stop_order_flag					INT
	,threshold_id						VARCHAR(255)	NOT NULL
	,threshold_value_lower				INT
	,threshold_value_upper				INT
	,denominator_pax					FLOAT
	,scheduled_threshold_numerator_pax	FLOAT
	,denominator_trip					FLOAT
	,scheduled_threshold_numerator_trip	FLOAT
)

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_threshold_pax_route_id
ON historical_schedule_adherence_threshold_pax (route_id);

CREATE NONCLUSTERED INDEX IX_historical_schedule_adherence_threshold_pax_stop_id
ON historical_schedule_adherence_threshold_pax (stop_id);

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_threshold_pax_direction_id
ON historical_schedule_adherence_threshold_pax (direction_id);

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_threshold_pax_actual_arrival_time_sec
ON historical_schedule_adherence_threshold_pax (actual_arrival_time_sec);

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_threshold_pax_actual_departure_time_sec
ON historical_schedule_adherence_threshold_pax (actual_departure_time_sec);

CREATE NONCLUSTERED INDEX
IX_historical_schedule_adherence_threshold_pax_service_date
ON historical_schedule_adherence_threshold_pax (service_date);

IF OBJECT_ID('dbo.historical_headway_time_threshold_trip','U') IS NOT NULL
	DROP TABLE dbo.historical_headway_time_threshold_trip

CREATE TABLE dbo.historical_headway_time_threshold_trip
(
	service_date									VARCHAR(255)	NOT NULL
	,stop_id										VARCHAR(255)	NOT NULL
	,direction_id									INT				NOT NULL
	,prev_route_id									VARCHAR(255)	NOT NULL
	,route_id										VARCHAR(255)	NOT NULL
	,start_time_sec									INT				NOT NULL
	,end_time_sec									INT				NOT NULL
	,headway_time_sec								INT				NOT NULL
	,time_period_id									VARCHAR(255)	NOT NULL
	,time_period_type								VARCHAR(255)	NOT NULL
	,threshold_id									VARCHAR(255)	NOT NULL
	,threshold_lower_scheduled_median_headway_time_sec	INT			NULL
	,threshold_upper_scheduled_median_headway_time_sec	INT			NULL
	,threshold_lower_scheduled_average_headway_time_sec	INT			NULL
	,threshold_upper_scheduled_average_headway_time_sec	INT			NULL
	,denominator_trip								FLOAT			NOT NULL
	,scheduled_threshold_numerator_trip				FLOAT			NOT NULL
)

CREATE NONCLUSTERED INDEX IX_historical_headway_time_threshold_trip_stop_id
ON historical_headway_time_threshold_trip (stop_id);

CREATE NONCLUSTERED INDEX
IX_historical_headway_time_threshold_trip_prev_route_id
ON historical_headway_time_threshold_trip (prev_route_id);

CREATE NONCLUSTERED INDEX IX_historical_headway_time_threshold_trip_route_id
ON historical_headway_time_threshold_trip (route_id);

CREATE NONCLUSTERED INDEX IX_historical_headway_time_threshold_trip_direction_id
ON historical_headway_time_threshold_trip (direction_id);

CREATE NONCLUSTERED INDEX IX_historical_headway_time_threshold_trip_service_date
ON historical_headway_time_threshold_trip (service_date);

IF OBJECT_ID('dbo.historical_missed_stop_times_scheduled','U') IS NOT NULL
	DROP TABLE dbo.historical_missed_stop_times_scheduled;

CREATE TABLE dbo.historical_missed_stop_times_scheduled
(
	record_id								INT
	,service_date							DATE
	,trip_id								VARCHAR(255)
	,stop_sequence							INT
	,stop_id								VARCHAR(255)
	,scheduled_arrival_time_sec				INT
	,scheduled_departure_time_sec			INT
	,actual_arrival_time_sec				INT
	,actual_departure_time_sec				INT
	,max_before_stop_sequence				INT
	,max_before_arrival_time_sec			FLOAT
	,max_before_departure_time_sec			FLOAT
	,max_before_event_time_arrival_sec		FLOAT
	,max_before_event_time_departure_sec	FLOAT
	,min_after_stop_sequence				INT
	,min_after_arrival_time_sec				FLOAT
	,min_after_departure_time_sec			FLOAT
	,min_after_event_time_arrival_sec		FLOAT
	,min_after_event_time_departure_sec		FLOAT
	,expected_arrival_time_sec				INT
	,expected_departure_time_sec			INT
)

CREATE NONCLUSTERED INDEX IX_historical_missed_stop_times_scheduled_service_date
ON historical_missed_stop_times_scheduled (service_date);

IF OBJECT_ID('dbo.historical_headway_adherence_threshold_pax','U') IS NOT NULL
	DROP TABLE dbo.historical_headway_adherence_threshold_pax;

CREATE TABLE dbo.historical_headway_adherence_threshold_pax
(
	record_id							INT IDENTITY(1,1) NOT NULL,
	service_date						VARCHAR(255) NOT NULL,
	route_id							VARCHAR(255) NOT NULL,
	route_type							INT NOT NULL,
	direction_id						INT NOT NULL,
	trip_id								VARCHAR(255) NOT NULL,
	stop_id								VARCHAR(255) NOT NULL,
	stop_order_flag						INT NOT NULL,
	checkpoint_id						VARCHAR(255) NULL,
	start_time_sec						INT NOT NULL,
	end_time_sec						INT NOT NULL,
	actual_headway_time_sec				INT NULL,
	scheduled_headway_time_sec			INT NULL,
	threshold_id						VARCHAR(255) NOT NULL,
	threshold_id_lower					VARCHAR(255) NULL,
	threshold_id_upper					VARCHAR(255) NULL,
	threshold_value_lower				VARCHAR(255) NULL,
	threshold_value_upper				VARCHAR(255) NULL,
	denominator_pax						FLOAT NULL,
	scheduled_threshold_numerator_pax	FLOAT NULL
) 

IF OBJECT_ID('dbo.historical_trip_run_time_adherence_threshold_pax','U') IS NOT NULL
	DROP TABLE dbo.historical_trip_run_time_adherence_threshold_pax;

CREATE TABLE dbo.historical_trip_run_time_adherence_threshold_pax
(
	record_id							INT IDENTITY(1,1) NOT NULL,
	service_date						VARCHAR(255) NOT NULL,
	route_id							VARCHAR(255) NOT NULL,
	route_type							INT NOT NULL,
	direction_id						INT NOT NULL,
	trip_id								VARCHAR(255) NOT NULL,
	start_time_sec						INT NOT NULL,
	end_time_sec						INT NOT NULL,
	actual_run_time_sec					INT NOT NULL,
	scheduled_run_time_sec				INT NOT NULL,
	threshold_id						VARCHAR(255) NOT NULL,
	threshold_id_lower					VARCHAR(255) NULL,
	threshold_id_upper					VARCHAR(255) NULL,
	threshold_value_lower				VARCHAR(255) NULL,
	threshold_value_upper				VARCHAR(255) NULL,
	denominator_pax						FLOAT NULL,
	scheduled_threshold_numerator_pax	FLOAT NULL
) 

IF OBJECT_ID('dbo.deleted_from_abcde_time','U') IS NOT NULL
	DROP TABLE dbo.deleted_from_abcde_time

CREATE TABLE dbo.deleted_from_abcde_time
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
	,abcde_route_type	INT				NOT NULL
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

--Create tables for configuration files 
-- Create Day Type Table 
IF OBJECT_ID('dbo.config_day_type','U') IS NOT NULL
	DROP TABLE dbo.config_day_type

CREATE TABLE dbo.config_day_type
(
	day_type_id	VARCHAR(255)	NOT NULL PRIMARY KEY
	,day_type	VARCHAR(255)
);

-- Create Day Type Day of Week Table 
IF OBJECT_ID('dbo.config_day_type_dow','U') IS NOT NULL
	DROP TABLE dbo.config_day_type_dow

CREATE TABLE dbo.config_day_type_dow
(
	day_type_id			VARCHAR(255)	NOT NULL
	,day_of_the_week	VARCHAR(255)	NOT NULL
);

-- Create Config Mode Threshold Table 
IF OBJECT_ID('dbo.config_mode_threshold','U') IS NOT NULL
	DROP TABLE dbo.config_mode_threshold

CREATE TABLE dbo.config_mode_threshold
(
	route_type		INT
	,threshold_id	VARCHAR(255)	NOT NULL
);

--Create passenger arrival rate table for headway-based services 
IF OBJECT_ID('dbo.config_passenger_arrival_rate','U') IS NOT NULL
	DROP TABLE dbo.config_passenger_arrival_rate

CREATE TABLE dbo.config_passenger_arrival_rate
(
	day_type_id				VARCHAR(255)	NOT NULL
	,time_slice_id			VARCHAR(255)	NOT NULL
	,from_stop_id			VARCHAR(255)	NOT NULL
	,to_stop_id				VARCHAR(255)	NOT NULL
	,passenger_arrival_rate	FLOAT			NOT NULL
)

CREATE NONCLUSTERED INDEX IX_config_passenger_arrival_rate_index_1
ON dbo.config_passenger_arrival_rate (from_stop_id,to_stop_id)
INCLUDE (day_type_id,time_slice_id,passenger_arrival_rate)

--Create passenger information table for schedule-based services 
IF OBJECT_ID('dbo.config_passenger_od_load_cr','U') IS NOT NULL
	DROP TABLE dbo.config_passenger_od_load_cr

CREATE TABLE dbo.config_passenger_od_load_cr
(
	route_id						VARCHAR(255)
	,trip_id						VARCHAR(255)
	,trip_short_name				VARCHAR(255)
	,from_stop_sequence				INT
	,from_stop_id					VARCHAR(255)
	,to_stop_sequence				INT
	,to_stop_id						VARCHAR(255)
	,from_stop_passenger_on			INT
	,to_stop_passenger_off			INT
	,from_stop_passenger_on_flag	INT
	,to_stop_passenger_off_flag		INT
	,sum_passenger_off_subset		INT
	,num_passenger_off_subset		INT
)

CREATE NONCLUSTERED INDEX config_passenger_od_load_cr_index_1
ON dbo.config_passenger_od_load_CR (route_id,trip_id,from_stop_id)
INCLUDE (from_stop_passenger_on)

--Create config threshold table 
IF OBJECT_ID('dbo.config_threshold','U') IS NOT NULL
	DROP TABLE dbo.config_threshold

CREATE TABLE dbo.config_threshold
(
	threshold_id			VARCHAR(255)	PRIMARY KEY
	,threshold_name			VARCHAR(255)	NOT NULL
	,threshold_type			VARCHAR(255)	NOT NULL
	,threshold_priority		INT				NOT NULL
	,min_max_equal			VARCHAR(255)	NOT NULL
	,upper_lower			VARCHAR(255)	NOT NULL
	,parent_threshold_id	VARCHAR(255)
	,parent_child			INT				NOT NULL
)

-- Create table for headway and travel time threshold calculation   
IF OBJECT_ID('dbo.config_threshold_calculation','U') IS NOT NULL
	DROP TABLE dbo.config_threshold_calculation

CREATE TABLE dbo.config_threshold_calculation
(
	threshold_calculation_id	VARCHAR(255)	PRIMARY KEY
	,threshold_id				VARCHAR(255)	NOT NULL
	,multiply_by				FLOAT			NOT NULL
	,add_to						FLOAT			NOT NULL
)

--Create Time Period Table 
IF OBJECT_ID('dbo.config_time_period','U') IS NOT NULL
	DROP TABLE dbo.config_time_period

CREATE TABLE dbo.config_time_period
(
	time_period_id				VARCHAR(255)	PRIMARY KEY
	,day_type					VARCHAR(255)	NOT NULL
	,time_period_sequence		INT				NOT NULL
	,time_period_type			VARCHAR(255)	NOT NULL
	,time_period_name			VARCHAR(255)	NOT NULL
	,time_period_start_time		TIME
	,time_period_end_time		TIME
	,time_period_start_time_sec	INT				NOT NULL
	,time_period_end_time_sec	INT				NOT NULL
);

--Create Time Slice Table 
IF OBJECT_ID('dbo.config_time_slice','U') IS NOT NULL
	DROP TABLE dbo.config_time_slice

CREATE TABLE dbo.config_time_slice
(
	time_slice_id				VARCHAR(255)	PRIMARY KEY
	,time_slice_start_sec		INT
	,time_slice_end_sec			INT
	,time_slice_start_date_time	TIME
	,time_slice_end_date_time	TIME
);

-- Create Config Stop Order Flag Threshold Table 
IF OBJECT_ID('dbo.config_stop_order_flag_threshold','U') IS NOT NULL
	DROP TABLE dbo.config_stop_order_flag_threshold

CREATE TABLE dbo.config_stop_order_flag_threshold
(
	stop_order_flag		INT
	,threshold_id	VARCHAR(255)	NOT NULL
);

--Create Prediction Thresholds Table
IF OBJECT_ID('dbo.config_prediction_threshold','U') IS NOT NULL
	DROP TABLE dbo.config_prediction_threshold

CREATE TABLE dbo.config_prediction_threshold
(
	threshold_id				VARCHAR(255)	NOT NULL
	,threshold_name				VARCHAR(255)	NOT NULL
	,threshold_type				VARCHAR(255)	NOT NULL
	,route_type					INT				
	,bin_lower					INT
	,bin_upper					INT
	,pred_error_threshold_lower	INT
	,pred_error_threshold_upper	INT
)

--Create Dashboard Thresholds Table
IF OBJECT_ID('dbo.config_dashboard_threshold','U') IS NOT NULL
	DROP TABLE dbo.config_dashboard_threshold

CREATE TABLE dbo.config_dashboard_threshold
(
	dashboard_id	VARCHAR(255) PRIMARY KEY
	,dashboard_name	VARCHAR(255)
	,threshold_id	VARCHAR(255)
)