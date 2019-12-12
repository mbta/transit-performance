
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('ProcessRTEvent','P') IS NOT NULL
	DROP PROCEDURE dbo.ProcessRTEvent

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ProcessRTEvent

--Script Version: Master - 1.1.1.1

--This procedure processes all the real-time events. It is executed by the process_rt_event trigger ON INSERT into the dbo.rt_event table.

AS


BEGIN
	SET NOCOUNT ON;


	DECLARE @current_service_date DATE
	SET @current_service_date = dbo.fnConvertDateTimeToServiceDate(GETDATE())

	--Terminals and Park Street for eliminating double headways
	DECLARE @multiple_berths as TABLE
	(
		route_id			VARCHAR(255)
		,direction_id		INT
		,stop_id			VARCHAR(255)
	)

	INSERT INTO @multiple_berths
	VALUES
		('Blue',0,'70059')
		,('Blue',1,'70038')
		,('Orange',0,'70036')
		,('Orange',1,'70001')
		,('Red',0,'70061')
		,('Red',1,'70105')
		,('Red',1,'70094')
		,('Green-B',0,'70196')
		,('Green-C',0,'70197')
		,('Green-D',0,'70198')
		,('Green-E',0,'70199')
		,('Green-B',1,'70200')
		,('Green-C',1,'70200')
		,('Green-D',1,'70200')
		,('Green-E',1,'70200')
		,('Green-B',0,'70210')
		,('Green-C',0,'70210')
		,('Green-D',0,'70210')
		,('Green-E',0,'70210')
		,('Green-B',1,'70106')
		,('Green-C',1,'70238')
		,('Green-D',1,'70160')
		,('Green-E',1,'70260')

	UPDATE dbo.rt_event
		SET direction_id = t.direction_id
		FROM gtfs.trips t
		WHERE
				dbo.rt_event.trip_id = t.trip_id
			AND 
				dbo.rt_event.direction_id IS NULL
			AND 
				dbo.rt_event.service_date = @current_service_date

	UPDATE dbo.rt_event
		SET direction_id = rds.direction_id
		FROM gtfs.route_direction_stop rds
		WHERE
				rds.route_id = dbo.rt_event.route_id
			AND 
				rds.stop_id = dbo.rt_event.stop_id
			AND 
				dbo.rt_event.direction_id IS NULL
			AND 
				dbo.rt_event.service_date = @current_service_date

	UPDATE dbo.rt_event
		SET direction_id = 3
		WHERE
				dbo.rt_event.direction_id IS NULL
			AND 
				dbo.rt_event.service_date = @current_service_date

	UPDATE dbo.rt_event
		SET event_time_sec = DATEDIFF(s,service_date,dbo.fnConvertEpochToDateTime(event_time))
		WHERE
				dbo.rt_event.event_time_sec IS NULL
			AND 
				dbo.rt_event.service_date = @current_service_date

	--create temporary table to store information about events for all files that have been inserted into the dbo.rt_event table,
	-- but have not yet been processed in real-time
	DECLARE @unprocessed_events_all AS TABLE
	(
		record_id		INT
		,service_date	DATE
		,file_time		INT
		,route_id		VARCHAR(255)
		,trip_id		VARCHAR(255)
		,direction_id	INT
		,stop_id		VARCHAR(255)
		,stop_sequence	INT
		,vehicle_id		VARCHAR(255)
		,vehicle_label	VARCHAR(255)
		,event_type		CHAR(3)
		,event_time		INT
		,event_time_sec	INT
	)
	INSERT INTO @unprocessed_events_all
	(
		record_id
		,service_date
		,file_time
		,route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,event_type
		,event_time
		,event_time_sec
	)

		SELECT
			record_id
			,service_date
			,file_time
			,route_id
			,trip_id
			,direction_id
			,stop_id
			,stop_sequence
			,vehicle_id
			,vehicle_label
			,event_type
			,event_time
			,event_time_sec
		FROM dbo.rt_event
		WHERE
				event_processed_rt = 0 --event has not been processed in real-time
			AND										
				service_date = @current_service_date 

	IF (SELECT COUNT(*) FROM @unprocessed_events_all) > 0

	BEGIN

		--create temporary table to store information about events for one file that has not yet been processed in real-time
		DECLARE @unprocessed_events_file AS TABLE
		(
			record_id		INT
			,service_date	DATE
			,file_time		INT
			,route_id		VARCHAR(255)
			,trip_id		VARCHAR(255)
			,direction_id	INT
			,stop_id		VARCHAR(255)
			,stop_sequence	INT
			,vehicle_id		VARCHAR(255)
			,vehicle_label	VARCHAR(255)
			,event_type		CHAR(3)
			,event_time		INT
			,event_time_sec	INT
		)

		--create temporary table to store information about files that have not yet been processed in real-time
		DECLARE @unprocessed_files AS TABLE
		(
			file_time		INT
			,file_time_num	INT	IDENTITY
		)

		--insert into unprocessed_files table the file time of the files in unprocessed_events_all that have not yet been processed
		INSERT INTO @unprocessed_files
		(
			file_time
		)
			SELECT DISTINCT
				file_time
			FROM @unprocessed_events_all
			ORDER BY
				file_time ASC

		DECLARE @file_time_num_current INT
		SET @file_time_num_current = 1

		DECLARE @file_time_num_max INT
		SET @file_time_num_max = (SELECT MAX(file_time_num) FROM @unprocessed_files)

		--step through unprocessed files one at a time starting with the oldest unprocessed file
		WHILE @file_time_num_current <= 10 --@file_time_num_max -- currently limiting to 10 files at a time to keep processing time down

		BEGIN
			--SELECT * FROM @unprocessed_events_file
			INSERT INTO @unprocessed_events_file -- put information for particular file from unprocessed_events_all into unprocessed_events_file
			(
				record_id
				,service_date
				,file_time
				,route_id
				,trip_id
				,direction_id
				,stop_id
				,stop_sequence
				,vehicle_id
				,vehicle_label
				,event_type
				,event_time
				,event_time_sec
			)

				SELECT
					record_id
					,service_date
					,file_time
					,route_id
					,trip_id
					,direction_id
					,stop_id
					,stop_sequence
					,vehicle_id
					,vehicle_label
					,event_type
					,event_time
					,event_time_sec
				FROM @unprocessed_events_all
				WHERE
					file_time = (SELECT file_time FROM @unprocessed_files WHERE file_time_num = @file_time_num_current)

			UPDATE @unprocessed_events_file
				SET stop_id = ps.stop_id
				FROM @unprocessed_events_file e
				LEFT JOIN
				(
					SELECT rds.route_type, rds.route_id, rds.direction_id, rds.stop_order, rds.stop_id, s.parent_station
					FROM gtfs.route_direction_stop rds
					JOIN gtfs.stops s
					ON
						rds.stop_id = s.stop_id
				) ps
				ON
						e.route_id = ps.route_id
					AND
						e.direction_id = ps.direction_id
					AND
						(SELECT parent_station FROM gtfs.stops s WHERE e.stop_id = s.stop_id) = ps.parent_station
				WHERE		
						e.service_date = @current_service_date
					AND
						e.stop_id NOT IN (SELECT stop_id FROM gtfs.route_direction_stop)

			--Start processing departure events
			--This table stores dwell times in real-time
			DECLARE @today_rt_cd_time_temp AS TABLE
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

			INSERT INTO @today_rt_cd_time_temp
			(
				service_date
				,cd_stop_id
				,cd_stop_sequence
				,cd_direction_id
				,cd_route_id
				,cd_trip_id
				,cd_vehicle_id
				,c_record_id
				,d_record_id
				,c_time_sec
				,d_time_sec
				,cd_time_sec
			)

				SELECT
					tre.service_date
					,tre.stop_id AS cd_stop_id
					,tre.stop_sequence AS cd_stop_sequence
					,tre.direction_id AS cd_direction_id
					,tre.route_id AS cd_route_id
					,tre.trip_id AS cd_trip_id
					,tre.vehicle_id AS cd_vehicle_id
					,tre.record_id AS c_record_id
					,uef.record_id AS d_record_id
					,tre.event_time_sec AS c_time_sec
					,uef.event_time_sec AS d_time_sec
					,uef.event_time_sec - tre.event_time_sec AS cd_time_sec

				FROM @unprocessed_events_file uef --departure at d

				JOIN dbo.today_rt_event tre --arrival at c
					ON
						(
							tre.event_type = 'ARR'
						AND 
							uef.event_type = 'DEP'
						AND	
							tre.service_date = uef.service_date
						AND 
							tre.stop_id = uef.stop_id
						AND 
							tre.stop_sequence = uef.stop_sequence
						AND 
							tre.direction_id = uef.direction_id
						AND 
							tre.vehicle_id = uef.vehicle_id
						AND 
							tre.trip_id = uef.trip_id
						AND 
							uef.event_time_sec >= tre.event_time_sec
						)

			--This table stores headway times at a stop between trips of all routes in real-time
			DECLARE @today_rt_bd_sr_all_time_temp AS TABLE
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
			INSERT INTO @today_rt_bd_sr_all_time_temp
			(
				service_date
				,bd_stop_id
				,b_stop_sequence
				,d_stop_sequence
				,b_route_id
				,d_route_id
				,bd_direction_id
				,b_trip_id
				,d_trip_id
				,b_vehicle_id
				,d_vehicle_id
				,b_record_id
				,d_record_id
				,b_time_sec
				,d_time_sec
				,bd_time_sec
			)

				SELECT
					service_date
					,bd_stop_id
					,b_stop_sequence
					,d_stop_sequence
					,b_route_id
					,d_route_id
					,bd_direction_id
					,b_trip_id
					,d_trip_id
					,b_vehicle_id
					,d_vehicle_id
					,b_record_id
					,d_record_id
					,b_time_sec
					,d_time_sec
					,bd_time_sec

				FROM
				(
					SELECT
						y.service_date
						,y.stop_id AS bd_stop_id
						,x.stop_sequence AS b_stop_sequence
						,y.stop_sequence AS d_stop_sequence
						,x.route_id AS b_route_id
						,y.route_id AS d_route_id
						,y.direction_id AS bd_direction_id
						,x.trip_id AS b_trip_id
						,y.trip_id AS d_trip_id
						,x.vehicle_id AS b_vehicle_id
						,y.vehicle_id AS d_vehicle_id
						,x.record_id AS b_record_id
						,y.record_id AS d_record_id
						,x.event_time_sec AS b_time_sec
						,y.event_time_sec AS d_time_sec
						,y.event_time_sec - x.event_time_sec AS bd_time_sec
						,ROW_NUMBER() OVER ( -- Partition finds the most recent relevant trip travelling from b/d to e.
						PARTITION BY
						y.record_id
						ORDER BY x.event_time_sec DESC) AS rn

					FROM @unprocessed_events_file y --y is the "current" trip

					JOIN dbo.today_rt_event x  --x is the most recent relevant "previous" trip

						ON
							(
								y.event_type = 'DEP'
							AND 
								x.event_type = 'DEP'
							AND 
								y.service_date = x.service_date
							AND 
								y.stop_id = x.stop_id
							AND 
								y.direction_id = x.direction_id
							AND 
								y.vehicle_id <> x.vehicle_id
							AND 
								y.trip_id <> x.trip_id
							--AND 
							--	y.route_id =x.route_id
							AND
								y.event_time_sec > x.event_time_sec
							AND 
								y.event_time_sec - x.event_time_sec <= 1800
							)

				) temp
				WHERE
					rn = 1

			--This table stores headway times at a stop between trips of the same route in real-time
			DECLARE @today_rt_bd_sr_same_time_temp AS TABLE
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

			INSERT INTO @today_rt_bd_sr_same_time_temp
			(
				service_date
				,bd_stop_id
				,b_stop_sequence
				,d_stop_sequence
				,bd_route_id
				,bd_direction_id
				,b_trip_id
				,d_trip_id
				,b_vehicle_id
				,d_vehicle_id
				,b_record_id
				,d_record_id
				,b_time_sec
				,d_time_sec
				,bd_time_sec
			)
				SELECT
					service_date
					,bd_stop_id
					,b_stop_sequence
					,d_stop_sequence
					,bd_route_id
					,bd_direction_id
					,b_trip_id
					,d_trip_id
					,b_vehicle_id
					,d_vehicle_id
					,b_record_id
					,d_record_id
					,b_time_sec
					,d_time_sec
					,bd_time_sec
				FROM
				(
					SELECT
						y.service_date
						,y.stop_id AS bd_stop_id
						,x.stop_sequence AS b_stop_sequence
						,y.stop_sequence AS d_stop_sequence
						,y.route_id AS bd_route_id
						,y.direction_id AS bd_direction_id
						,x.trip_id AS b_trip_id
						,y.trip_id AS d_trip_id
						,x.vehicle_id AS b_vehicle_id
						,y.vehicle_id AS d_vehicle_id
						,x.record_id AS b_record_id
						,y.record_id AS d_record_id
						,x.event_time_sec AS b_time_sec
						,y.event_time_sec AS d_time_sec
						,y.event_time_sec - x.event_time_sec AS bd_time_sec

						,ROW_NUMBER() OVER ( -- Partition finds the most recent relevant trip travelling from b/d to e.
						PARTITION BY
						y.record_id
						ORDER BY x.event_time_sec DESC) AS rn

					FROM @unprocessed_events_file y --y is the "current" trip

					JOIN dbo.today_rt_event x --x is the most recent relevant "previous" trip

						ON
							(
								y.event_type = 'DEP'
							AND 
								x.event_type = 'DEP'
							AND 
								y.service_date = x.service_date
							AND 
								y.stop_id = x.stop_id
							AND 
								y.direction_id = x.direction_id
							AND 
								y.vehicle_id <> x.vehicle_id
							AND 
								y.trip_id <> x.trip_id
							AND 
								y.route_id = x.route_id
							AND 
								y.event_time_sec > x.event_time_sec
							AND 
								y.event_time_sec - x.event_time_sec <= 1800
							)

				) temp
				WHERE
					rn = 1

			-------------commuter rail schedule adherence departures starts---------


			DECLARE @today_rt_departure_time_sec AS TABLE
			(
				service_date					VARCHAR(255)	NOT NULL
				,route_id						VARCHAR(255)	NOT NULL
				,route_type						INT				NOT NULL
				,direction_id					INT				NOT NULL
				,trip_id						VARCHAR(255)	NOT NULL
				,stop_sequence					INT				NOT NULL
				,stop_id						VARCHAR(255)	NOT NULL
				,vehicle_id						VARCHAR(255)
				,scheduled_departure_time_sec	INT
				,actual_departure_time_sec		INT
				,departure_delay_sec			INT
				,stop_order_flag				INT
			)

			INSERT INTO @today_rt_departure_time_sec
			(
				service_date
				,route_id
				,route_type
				,direction_id
				,trip_id
				,stop_sequence
				,stop_id
				,vehicle_id
				,scheduled_departure_time_sec
				,actual_departure_time_sec
				,departure_delay_sec
				,stop_order_flag
			)

				SELECT
					ds.service_date
					,ds.route_id
					,ds.route_type
					,ds.direction_id
					,ds.trip_id
					,ds.stop_sequence
					,ds.stop_id
					,red.vehicle_id
					,ds.departure_time_sec AS scheduled_departure_time
					,red.event_time_sec AS actual_departure_time
					,red.event_time_sec - ds.departure_time_sec AS departure_delay_sec
					,ds.stop_order_flag
				FROM	dbo.today_stop_times_sec ds
						,@unprocessed_events_file red
				WHERE
						red.event_type = 'DEP'
					AND 
						red.service_date = ds.service_date
					AND 
						red.trip_id = ds.trip_id
					AND 
						red.stop_id = ds.stop_id
					AND 
						red.stop_sequence = ds.stop_sequence

			--put the real-time departure in with the real-time arrival in the real-time schedule adherence disaggregate table

			DECLARE @today_rt_schedule_adherence_disaggregate AS TABLE
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

			INSERT INTO @today_rt_schedule_adherence_disaggregate
			(
				service_date
				,route_id
				,route_type
				,direction_id
				,trip_id
				,stop_sequence
				,stop_id
				,vehicle_id
				,scheduled_arrival_time_sec
				,actual_arrival_time_sec
				,arrival_delay_sec
				,scheduled_departure_time_sec
				,actual_departure_time_sec
				,departure_delay_sec
				,stop_order_flag
			)

				SELECT
					dd.service_date
					,dd.route_id
					,dd.route_type
					,dd.direction_id
					,dd.trip_id
					,dd.stop_sequence
					,dd.stop_id
					,dd.vehicle_id
					,ds.arrival_time_sec
					,da.event_time_sec
					,da.event_time_sec - ds.arrival_time_sec AS arrival_delay_sec
					,dd.scheduled_departure_time_sec
					,dd.actual_departure_time_sec
					,dd.departure_delay_sec
					,dd.stop_order_flag
				FROM dbo.today_rt_event da --need to have had an arrival already in order to have a schedule adherence disaggregate with a departure
				JOIN @today_rt_departure_time_sec dd --real-time departure
					ON
							da.service_date = dd.service_date
						AND 
							da.route_id = dd.route_id
						AND 
							da.trip_id = dd.trip_id
						AND 
							da.stop_id = dd.stop_id
						AND 
							da.vehicle_id = dd.vehicle_id
				JOIN dbo.today_stop_times_sec ds
					ON
							dd.service_date = ds.service_date
						AND 
							dd.trip_id = ds.trip_id
						AND 
							dd.stop_id = ds.stop_id
						AND 
							dd.stop_sequence = ds.stop_sequence
				WHERE
					da.event_type = 'ARR'

			-----scheduled adherence temp ends---------------------------------------

			----------------commuter rail schedule adherence departures ends------------------------------------

			--Put the processed departure events into the today_rt_event table
			INSERT INTO dbo.today_rt_event
			(
				record_id
				,service_date
				,file_time
				,route_id
				,trip_id
				,direction_id
				,stop_id
				,stop_sequence
				,vehicle_id
				,vehicle_label
				,event_type
				,event_time
				,event_time_sec
			)
				SELECT
					record_id
					,service_date
					,file_time
					,route_id
					,trip_id
					,direction_id
					,stop_id
					,stop_sequence
					,vehicle_id
					,vehicle_label
					,event_type
					,event_time
					,event_time_sec
				FROM @unprocessed_events_file
				WHERE
					event_type = 'DEP'

			--finished processing Departures----------------------

			--start processing arrivals

			--This table stores travel times in real-time

			DECLARE @today_rt_de_time_temp AS TABLE
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


			INSERT INTO @today_rt_de_time_temp
			(
				service_date
				,d_stop_id
				,e_stop_id
				,d_stop_sequence
				,e_stop_sequence
				,de_direction_id
				,de_route_id
				,de_trip_id
				,de_vehicle_id
				,d_record_id
				,e_record_id
				,d_time_sec
				,e_time_sec
				,de_time_sec
			)
				SELECT
					tre.service_date
					,tre.stop_id AS d_stop_id
					,uef.stop_id AS e_stop_id
					,tre.stop_sequence AS d_stop_sequence
					,uef.stop_sequence AS e_stop_sequence
					,tre.direction_id AS de_direction_id
					,tre.route_id AS de_route_id
					,tre.trip_id AS de_trip_id
					,tre.vehicle_id AS de_vehicle_id
					,tre.record_id AS d_record_id
					,uef.record_id AS e_record_id
					,tre.event_time_sec AS d_time_sec
					,uef.event_time_sec AS e_time_sec
					,uef.event_time_sec - tre.event_time_sec AS de_time_sec

				FROM @unprocessed_events_file uef --arrival at e

				JOIN dbo.today_rt_event tre --departure at d
					ON
						(
							tre.event_type = 'DEP'
						AND 
							uef.event_type = 'ARR'
						AND 
							tre.service_date = uef.service_date
						AND 
							tre.stop_sequence < uef.stop_sequence
						AND 
							tre.direction_id = uef.direction_id
						AND 
							tre.vehicle_id = uef.vehicle_id
						AND 
							tre.trip_id = uef.trip_id
						AND 
							uef.event_time_sec > tre.event_time_sec
						)


			--This table stores dwell + travel times in real-time
			DECLARE @today_rt_cde_time_temp AS TABLE
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

			INSERT INTO @today_rt_cde_time_temp
			(
				service_date
				,cd_stop_id
				,e_stop_id
				,cd_stop_sequence
				,e_stop_sequence
				,cde_direction_id
				,cde_route_id
				,cde_trip_id
				,cde_vehicle_id
				,c_record_id
				,d_record_id
				,e_record_id
				,c_time_sec
				,d_time_sec
				,e_time_sec
				,cd_time_sec
				,de_time_sec
			)
				SELECT
					de.service_date
					,cd.cd_stop_id
					,de.e_stop_id
					,cd.cd_stop_sequence
					,de.e_stop_sequence
					,cd.cd_direction_id AS cde_direction_id
					,cd.cd_route_id AS cde_route_id
					,cd.cd_trip_id AS cde_trip_id
					,cd.cd_vehicle_id AS cde_vehicle_id
					,cd.c_record_id
					,cd.d_record_id
					,de.e_record_id
					,cd.c_time_sec
					,cd.d_time_sec
					,de.e_time_sec
					,cd.cd_time_sec
					,de.de_time_sec

				FROM @today_rt_de_time_temp de

				JOIN dbo.today_rt_cd_time cd
					ON
						(
							de.d_record_id = cd.d_record_id
						AND 
							de.service_date = cd.service_date
						)

			--This table stores the events (abcde_time)
			DECLARE @today_rt_abcde_time_temp AS TABLE
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

			INSERT INTO @today_rt_abcde_time_temp
			(
				service_date
				,abcd_stop_id
				,e_stop_id
				,ab_stop_sequence
				,cd_stop_sequence
				,e_stop_sequence
				,abcde_direction_id
				,ab_route_id
				,cde_route_id
				,ab_trip_id
				,cde_trip_id
				,ab_vehicle_id
				,cde_vehicle_id
				,a_record_id
				,b_record_id
				,c_record_id
				,d_record_id
				,e_record_id
				,a_time_sec
				,b_time_sec
				,c_time_sec
				,d_time_sec
				,e_time_sec
				,cd_time_sec
				,de_time_sec
				,bd_time_sec
			)

				SELECT
					service_date
					,abcd_stop_id
					,e_stop_id
					,ab_stop_sequence
					,cd_stop_sequence
					,e_stop_sequence
					,abcde_direction_id
					,ab_route_id
					,cde_route_id
					,ab_trip_id
					,cde_trip_id
					,ab_vehicle_id
					,cde_vehicle_id
					,a_record_id
					,b_record_id
					,c_record_id
					,d_record_id
					,e_record_id
					,a_time_sec
					,b_time_sec
					,c_time_sec
					,d_time_sec
					,e_time_sec
					,cd_time_sec
					,de_time_sec
					,bd_time_sec

				FROM
				(
					SELECT
						y.service_date
						,y.cd_stop_id AS abcd_stop_id
						,y.e_stop_id
						,x.cd_stop_sequence AS ab_stop_sequence
						,y.cd_stop_sequence
						,y.e_stop_sequence
						,y.cde_direction_id AS abcde_direction_id
						,x.cde_route_id AS ab_route_id
						,y.cde_route_id
						,x.cde_trip_id AS ab_trip_id
						,y.cde_trip_id
						,x.cde_vehicle_id AS ab_vehicle_id
						,y.cde_vehicle_id
						,x.c_record_id AS a_record_id
						,x.d_record_id AS b_record_id
						,y.c_record_id
						,y.d_record_id
						,y.e_record_id
						,x.c_time_sec AS a_time_sec
						,x.d_time_sec AS b_time_sec
						,y.c_time_sec
						,y.d_time_sec
						,y.e_time_sec
						,y.cd_time_sec
						,y.de_time_sec
						,y.d_time_sec - x.d_time_sec AS bd_time_sec

						,ROW_NUMBER() OVER ( -- Partition finds the most recent relevant trip travelling from d to e.
						PARTITION BY
						y.c_record_id
						,y.d_record_id
						,y.e_record_id
						ORDER BY x.d_time_sec DESC) AS rn

					FROM @today_rt_cde_time_temp y --y is the "current" trip

					JOIN dbo.today_rt_cde_time x --x is the most recent relevant "previous" trip

						ON
							(
								y.service_date = x.service_date
							AND 
								y.cd_stop_id = x.cd_stop_id
							AND 
								y.e_stop_id = x.e_stop_id
							AND 
								y.cde_direction_id = x.cde_direction_id
							AND 
								y.cde_vehicle_id <> x.cde_vehicle_id
							AND 
								y.cde_trip_id <> x.cde_trip_id
							AND 
								CASE
									WHEN
											y.cde_route_id IN ('Green-B','Green-C','Green-D','Green-E')
										OR 
										(
												y.cd_stop_id IN (SELECT stop_id FROM @multiple_berths)
											AND 
												y.cde_direction_id = (SELECT DISTINCT direction_id FROM @multiple_berths WHERE stop_id = y.cd_stop_id)
										)
									THEN y.d_time_sec
									ELSE y.c_time_sec
								END > x.d_time_sec --the arrival time of the current trip should be later than the departure time of the previous trip
								--BUT compare departure times only for subway/Green Line terminals and Park Street (in both directions)
							--, but not by more than 30 minutes, as determined by the next statement
							AND 
								y.c_time_sec - x.d_time_sec <= 1800
							)
				) temp
				WHERE
					rn = 1

			--insert into  today_rt tables from @today_rt tables
			INSERT INTO dbo.today_rt_cd_time
			(
				service_date
				,cd_stop_id
				,cd_stop_sequence
				,cd_direction_id
				,cd_route_id
				,cd_trip_id
				,cd_vehicle_id
				,c_record_id
				,d_record_id
				,c_time_sec
				,d_time_sec
				,cd_time_sec
			)
				SELECT
					service_date
					,cd_stop_id
					,cd_stop_sequence
					,cd_direction_id
					,cd_route_id
					,cd_trip_id
					,cd_vehicle_id
					,c_record_id
					,d_record_id
					,c_time_sec
					,d_time_sec
					,cd_time_sec
				FROM @today_rt_cd_time_temp

			INSERT INTO dbo.today_rt_bd_sr_all_time
			(
				service_date
				,bd_stop_id
				,b_stop_sequence
				,d_stop_sequence
				,b_route_id
				,d_route_id
				,bd_direction_id
				,b_trip_id
				,d_trip_id
				,b_vehicle_id
				,d_vehicle_id
				,b_record_id
				,d_record_id
				,b_time_sec
				,d_time_sec
				,bd_time_sec
			)
				SELECT
					service_date
					,bd_stop_id
					,b_stop_sequence
					,d_stop_sequence
					,b_route_id
					,d_route_id
					,bd_direction_id
					,b_trip_id
					,d_trip_id
					,b_vehicle_id
					,d_vehicle_id
					,b_record_id
					,d_record_id
					,b_time_sec
					,d_time_sec
					,bd_time_sec
				FROM @today_rt_bd_sr_all_time_temp

			INSERT INTO dbo.today_rt_bd_sr_same_time
			(
				service_date
				,bd_stop_id
				,b_stop_sequence
				,d_stop_sequence
				,bd_route_id
				,bd_direction_id
				,b_trip_id
				,d_trip_id
				,b_vehicle_id
				,d_vehicle_id
				,b_record_id
				,d_record_id
				,b_time_sec
				,d_time_sec
				,bd_time_sec
			)
				SELECT
					service_date
					,bd_stop_id
					,b_stop_sequence
					,d_stop_sequence
					,bd_route_id
					,bd_direction_id
					,b_trip_id
					,d_trip_id
					,b_vehicle_id
					,d_vehicle_id
					,b_record_id
					,d_record_id
					,b_time_sec
					,d_time_sec
					,bd_time_sec
				FROM @today_rt_bd_sr_same_time_temp


			INSERT INTO dbo.today_rt_de_time
			(
				service_date
				,d_stop_id
				,e_stop_id
				,d_stop_sequence
				,e_stop_sequence
				,de_direction_id
				,de_route_id
				,de_trip_id
				,de_vehicle_id
				,d_record_id
				,e_record_id
				,d_time_sec
				,e_time_sec
				,de_time_sec
			)
				SELECT
					service_date
					,d_stop_id
					,e_stop_id
					,d_stop_sequence
					,e_stop_sequence
					,de_direction_id
					,de_route_id
					,de_trip_id
					,de_vehicle_id
					,d_record_id
					,e_record_id
					,d_time_sec
					,e_time_sec
					,de_time_sec
				FROM @today_rt_de_time_temp


			INSERT INTO dbo.today_rt_cde_time
			(
				service_date
				,cd_stop_id
				,e_stop_id
				,cd_stop_sequence
				,e_stop_sequence
				,cde_direction_id
				,cde_route_id
				,cde_trip_id
				,cde_vehicle_id
				,c_record_id
				,d_record_id
				,e_record_id
				,c_time_sec
				,d_time_sec
				,e_time_sec
				,cd_time_sec
				,de_time_sec
			)
				SELECT
					service_date
					,cd_stop_id
					,e_stop_id
					,cd_stop_sequence
					,e_stop_sequence
					,cde_direction_id
					,cde_route_id
					,cde_trip_id
					,cde_vehicle_id
					,c_record_id
					,d_record_id
					,e_record_id
					,c_time_sec
					,d_time_sec
					,e_time_sec
					,cd_time_sec
					,de_time_sec
				FROM @today_rt_cde_time_temp


			INSERT INTO dbo.today_rt_abcde_time
			(
				service_date
				,abcd_stop_id
				,e_stop_id
				,ab_stop_sequence
				,cd_stop_sequence
				,e_stop_sequence
				,abcde_direction_id
				,ab_route_id
				,cde_route_id
				,ab_trip_id
				,cde_trip_id
				,ab_vehicle_id
				,cde_vehicle_id
				,a_record_id
				,b_record_id
				,c_record_id
				,d_record_id
				,e_record_id
				,a_time_sec
				,b_time_sec
				,c_time_sec
				,d_time_sec
				,e_time_sec
				,cd_time_sec
				,de_time_sec
				,bd_time_sec
			)
				SELECT
					service_date
					,abcd_stop_id
					,e_stop_id
					,ab_stop_sequence
					,cd_stop_sequence
					,e_stop_sequence
					,abcde_direction_id
					,ab_route_id
					,cde_route_id
					,ab_trip_id
					,cde_trip_id
					,ab_vehicle_id
					,cde_vehicle_id
					,a_record_id
					,b_record_id
					,c_record_id
					,d_record_id
					,e_record_id
					,a_time_sec
					,b_time_sec
					,c_time_sec
					,d_time_sec
					,e_time_sec
					,cd_time_sec
					,de_time_sec
					,bd_time_sec
				FROM @today_rt_abcde_time_temp


			INSERT INTO dbo.today_rt_schedule_adherence_disaggregate
			(
				service_date
				,route_id
				,route_type
				,direction_id
				,trip_id
				,stop_sequence
				,stop_id
				,vehicle_id
				,scheduled_arrival_time_sec
				,actual_arrival_time_sec
				,arrival_delay_sec
				,scheduled_departure_time_sec
				,actual_departure_time_sec
				,departure_delay_sec
				,stop_order_flag
			)

				SELECT
					service_date
					,route_id
					,route_type
					,direction_id
					,trip_id
					,stop_sequence
					,stop_id
					,vehicle_id
					,scheduled_arrival_time_sec
					,actual_arrival_time_sec
					,arrival_delay_sec
					,scheduled_departure_time_sec
					,actual_departure_time_sec
					,departure_delay_sec
					,stop_order_flag
				FROM @today_rt_schedule_adherence_disaggregate

			INSERT INTO dbo.today_rt_event
			(
				record_id
				,service_date
				,file_time
				,route_id
				,trip_id
				,direction_id
				,stop_id
				,stop_sequence
				,vehicle_id
				,vehicle_label
				,event_type
				,event_time
				,event_time_sec
			)

				SELECT
					record_id
					,service_date
					,file_time
					,route_id
					,trip_id
					,direction_id
					,stop_id
					,stop_sequence
					,vehicle_id
					,vehicle_label
					,event_type
					,event_time
					,event_time_sec
				FROM @unprocessed_events_file
				WHERE
					event_type = 'ARR'

			--finished processing arrivals
			--Update passenger weighted travel time vs. threshold tables
			INSERT INTO dbo.today_rt_travel_time_threshold_pax
			(
				service_date
				,from_stop_id
				,to_stop_id
				,direction_id
				,prev_route_id
				,route_id
				,trip_id
				,start_time_sec
				,end_time_sec
				,travel_time_sec
				,threshold_id
				,threshold_historical_median_travel_time_sec
				,threshold_scheduled_median_travel_time_sec
				,threshold_historical_average_travel_time_sec 
				,threshold_scheduled_average_travel_time_sec 
				,denominator_pax
				,historical_threshold_numerator_pax
				,scheduled_threshold_numerator_pax
				,denominator_trip					
				,historical_threshold_numerator_trip 
				,scheduled_threshold_numerator_trip 

			)

				SELECT
					abcde.service_date
					,abcde.abcd_stop_id
					,abcde.e_stop_id
					,abcde.abcde_direction_id
					,abcde.ab_route_id
					,abcde.cde_route_id
					,abcde.cde_trip_id
					,abcde.d_time_sec
					,abcde.e_time_sec
					,(abcde.e_time_sec - abcde.d_time_sec) AS travel_time_sec
					,ttt.threshold_id
					,ttt.threshold_historical_median_travel_time_sec
					,ttt.threshold_scheduled_median_travel_time_sec
					,ttt.threshold_historical_average_travel_time_sec 
					,ttt.threshold_scheduled_average_travel_time_sec 
					,(abcde.d_time_sec - abcde.b_time_sec) * par.passenger_arrival_rate AS denominator_pax
					,CASE
						WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_historical_median_travel_time_sec > 0) THEN (abcde.d_time_sec - abcde.b_time_sec) * par.passenger_arrival_rate
						WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_historical_median_travel_time_sec <= 0) THEN 0
						ELSE 0
					END AS historical_threshold_numerator_pax
					,CASE
						WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_scheduled_average_travel_time_sec > 0) THEN (abcde.d_time_sec - abcde.b_time_sec) * par.passenger_arrival_rate
						WHEN ((abcde.e_time_sec - abcde.d_time_sec) - ttt.threshold_scheduled_average_travel_time_sec <= 0) THEN 0
						ELSE 0
					END AS scheduled_threshold_numerator_pax 
					,1 AS denominator_trip 
					,CASE
						WHEN ((abcde.e_time_sec - abcde.d_time_sec) > ttt.threshold_historical_median_travel_time_sec) THEN 1
						ELSE 0
					END AS historical_threshold_numerator_trip 
					,CASE
						WHEN ((abcde.e_time_sec - abcde.d_time_sec) > ttt.threshold_scheduled_average_travel_time_sec) THEN 1
						ELSE 0
					END AS scheduled_threshold_numerator_trip 

				FROM @today_rt_abcde_time_temp abcde

				JOIN dbo.config_time_slice ts
					ON
						(
								abcde.e_time_sec >= ts.time_slice_start_sec
							AND 
								abcde.e_time_sec < ts.time_slice_end_sec
						)
				JOIN dbo.service_date sd
					ON
						(
							abcde.service_date = sd.service_date
						)
				JOIN dbo.config_passenger_arrival_rate par
					ON
						(
								par.day_type_id = sd.day_type_id -- will need to account for exceptions
							AND
								ts.time_slice_id = par.time_slice_id
							AND 
								abcde.abcd_stop_id = par.from_stop_id
							AND 
								abcde.e_stop_id = par.to_stop_id
						)
				JOIN dbo.today_travel_time_threshold ttt
					ON
						(
								abcde.service_date = ttt.service_date
							AND 
								abcde.abcde_direction_id = ttt.direction_id
							AND 
								abcde.abcd_stop_id = ttt.from_stop_id
							AND 
								abcde.e_stop_id = ttt.to_stop_id
							AND 
								ts.time_slice_id = ttt.time_slice_id
							AND 
								threshold_historical_median_travel_time_sec IS NOT NULL
							AND 
								(ttt.route_type = 1 OR ttt.route_type = 0) --subway and green line passenger weighted numbers
							AND
								abcde.cde_route_id = ttt.route_id --added for multiple routes visiting the same stops, green line
						)
				------------------commuter rail travel times pax start-----------------------------------------
				UNION
				SELECT
					dat.service_date AS service_date
					,dat.d_stop_id AS from_stop_id
					,dat.e_stop_id AS to_stop_id
					,dat.de_direction_id AS direction_id
					,dat.de_route_id AS prev_route_id
					,dat.de_route_id AS route_id
					,dat.de_trip_id AS trip_id
					,dat.d_time_sec AS start_time_sec
					,dat.e_time_sec AS end_time_sec
					,dat.de_time_sec AS travel_time_sec
					,dtt.threshold_id AS threshold_id
					,dtt.threshold_historical_median_travel_time_sec AS threshold_historical_median_travel_time_sec
					,dtt.threshold_scheduled_median_travel_time_sec AS threshold_scheduled_median_travel_time_sec
					,dtt.threshold_historical_average_travel_time_sec AS threshold_historical_average_travel_time_sec
					,dtt.threshold_scheduled_average_travel_time_sec AS threshold_scheduled_average_travel_time_sec
					,po.num_passenger_off_subset AS denominator_pax
					,CASE
						WHEN (dat.de_time_sec - dtt.threshold_historical_median_travel_time_sec > 0) THEN po.num_passenger_off_subset
						WHEN (dat.de_time_sec - dtt.threshold_historical_median_travel_time_sec <= 0) THEN 0
						ELSE 0
					END AS historical_threshold_numerator_pax
					,CASE
						WHEN (dat.de_time_sec - dtt.threshold_scheduled_average_travel_time_sec > 0) THEN po.num_passenger_off_subset
						WHEN (dat.de_time_sec - dtt.threshold_scheduled_average_travel_time_sec <= 0) THEN 0
						ELSE 0
					END AS scheduled_threshold_numerator_pax 
					,1 AS denominator_trip 
					,CASE
						WHEN (dat.de_time_sec > dtt.threshold_historical_median_travel_time_sec) THEN 1
						ELSE 0
					END AS historical_threshold_numerator_trip
					,CASE
						WHEN (dat.de_time_sec > dtt.threshold_scheduled_average_travel_time_sec) THEN 1
						ELSE 0
					END AS scheduled_threshold_numerator_trip 
				FROM @today_rt_de_time_temp dat

				JOIN dbo.config_time_slice ts
					ON
						(
								dat.e_time_sec >= ts.time_slice_start_sec
							AND 
								dat.e_time_sec < ts.time_slice_end_sec
						)

				JOIN dbo.service_date sd
					ON
						(
							dat.service_date = sd.service_date
						)

				JOIN dbo.today_travel_time_threshold dtt
					ON
						(
								dat.service_date = dtt.service_date
							AND 
								dat.de_direction_id = dtt.direction_id
							AND 
								dat.de_trip_id = dat.de_trip_id
							AND 
								dat.d_stop_id = dtt.from_stop_id
							AND 
								dat.e_stop_id = dtt.to_stop_id
							AND 
								ts.time_slice_id = dtt.time_slice_id
							AND
								dtt.route_type = 2 --commuter rail passenger weighted numbers
							AND
								dat.de_route_id = dtt.route_id --added for multiple routes visiting the same stops
						)

				JOIN dbo.config_passenger_od_load_CR po
					ON
						(
								dat.de_trip_id = po.trip_id
							AND 
								dat.d_stop_id = po.from_stop_id
							AND 
								dat.e_stop_id = po.to_stop_id
						)
			------------------------CR travel times pax end-------------------------

			--Update passenger weighted wait time vs. threshold tables
			INSERT INTO dbo.today_rt_wait_time_od_threshold_pax
			(
				service_date
				,from_stop_id
				,to_stop_id
				,direction_id
				,prev_route_id
				,route_id
				,start_time_sec
				,end_time_sec
				,max_wait_time_sec
				,dwell_time_sec
				,threshold_id
				,threshold_historical_median_wait_time_sec
				,threshold_scheduled_median_wait_time_sec
				,threshold_historical_average_wait_time_sec
				,threshold_scheduled_average_wait_time_sec 
				,denominator_pax
				,historical_threshold_numerator_pax
				,scheduled_threshold_numerator_pax
				,denominator_trip 
				,historical_threshold_numerator_trip 
				,scheduled_threshold_numerator_trip 
			)
				SELECT
					abcde.service_date
					,abcde.abcd_stop_id
					,abcde.e_stop_id
					,abcde.abcde_direction_id
					,abcde.ab_route_id
					,abcde.cde_route_id
					,abcde.b_time_sec
					,abcde.d_time_sec
					,abcde.c_time_sec - abcde.b_time_sec
					,abcde.d_time_sec - abcde.c_time_sec
					,wtt.threshold_id
					,wtt.threshold_historical_median_wait_time_sec
					,wtt.threshold_scheduled_median_wait_time_sec
					,wtt.threshold_historical_average_wait_time_sec 
					,wtt.threshold_scheduled_average_wait_time_sec 
					,(d_time_sec - b_time_sec) * par.passenger_arrival_rate AS denominator_pax
					,CASE
						WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_historical_median_wait_time_sec > 0) THEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_historical_median_wait_time_sec) * par.passenger_arrival_rate
						WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_historical_median_wait_time_sec <= 0) THEN 0
						ELSE 0
					END AS historical_threshold_numerator_pax
					,CASE
						WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_average_wait_time_sec > 0) THEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_median_wait_time_sec) * par.passenger_arrival_rate
						WHEN ((abcde.c_time_sec - abcde.b_time_sec) - wtt.threshold_scheduled_average_wait_time_sec <= 0) THEN 0
						ELSE 0
					END AS scheduled_threshold_numerator_pax   
					,1 AS denominator_trip  
					,CASE
						WHEN ((abcde.c_time_sec - abcde.b_time_sec) > wtt.threshold_historical_median_wait_time_sec) THEN 1
						ELSE 0
					END AS historical_threshold_numerator_trip 
					,CASE
						WHEN ((abcde.c_time_sec - abcde.b_time_sec) > wtt.threshold_scheduled_average_wait_time_sec) THEN 1
						ELSE 0
					END AS scheduled_threshold_numerator_trip 
				FROM @today_rt_abcde_time_temp abcde

				JOIN dbo.config_time_slice ts
					ON
						(
								abcde.d_time_sec >= ts.time_slice_start_sec
							AND 
								abcde.d_time_sec < ts.time_slice_end_sec
						)

				JOIN dbo.service_date sd
					ON
						(
							abcde.service_date = sd.service_date
						)

				JOIN dbo.config_passenger_arrival_rate par
					ON
						(
								par.day_type_id = sd.day_type_id -- will need to account for exceptions
							AND
								ts.time_slice_id = par.time_slice_id
							AND 
								abcde.abcd_stop_id = par.from_stop_id
							AND 
								abcde.e_stop_id = par.to_stop_id
						)

				JOIN dbo.today_wait_time_od_threshold wtt
					ON
						(
								abcde.service_date = wtt.service_date
							AND 
								abcde.abcde_direction_id = wtt.direction_id
							AND 
								abcde.abcd_stop_id = wtt.stop_id
							AND 
								abcde.e_stop_id = wtt.to_stop_id
							AND 
								ts.time_slice_id = wtt.time_slice_id
							AND 
								threshold_historical_median_wait_time_sec IS NOT NULL
							AND 
								(wtt.route_type = 1 OR wtt.route_type = 0) --subway and green line passenger weighted numbers
						);

			---headway trip metrics

			INSERT INTO dbo.today_rt_headway_time_threshold_trip
			(
				service_date
				,stop_id
				,direction_id
				,prev_route_id
				,route_id
				,start_time_sec
				,end_time_sec
				,headway_time_sec
				,threshold_id
				,threshold_scheduled_median_headway_time_sec
				,threshold_scheduled_average_headway_time_sec 
				,denominator_trip 
				,scheduled_threshold_numerator_trip 
			)

				SELECT
					bd.service_date AS service_date
					,bd.bd_stop_id AS stop_id
					,bd.bd_direction_id AS direction_id
					,bd.b_route_id AS prev_route_id
					,bd.d_route_id AS route_id
					,bd.b_time_sec AS start_time_sec
					,bd.d_time_sec AS end_time_sec
					,bd.d_time_sec - bd.b_time_sec AS headway_time_sec
					,wtt.threshold_id AS threshold_id
					,wtt.threshold_scheduled_median_headway_time_sec AS threshold_scheduled_median_headway_time_sec
					,wtt.threshold_scheduled_average_headway_time_sec AS threshold_scheduled_average_headway_time_sec
					,1 AS denominator_trip
					,CASE
						WHEN ((bd.d_time_sec - bd.b_time_sec) > threshold_scheduled_average_headway_time_sec) THEN 1
						ELSE 0
					END AS scheduled_threshold_numerator_trip
				FROM @today_rt_bd_sr_all_time_temp bd

				JOIN dbo.config_time_slice ts
					ON
						(
								bd.d_time_sec >= ts.time_slice_start_sec
							AND 
								bd.d_time_sec < ts.time_slice_end_sec
						)

				JOIN dbo.service_date sd
					ON
						(
							bd.service_date = sd.service_date
						)

				JOIN dbo.today_headway_time_threshold wtt
					ON
						(
								bd.service_date = wtt.service_date
							AND 
								bd.bd_direction_id = wtt.direction_id
							AND 
								bd.bd_stop_id = wtt.stop_id
							AND 
								ts.time_slice_id = wtt.time_slice_id
							AND
								(wtt.route_type = 1) --subway numbers only
						)

			--today rt schedule adherence weighted by passengers and trips ----

			INSERT INTO dbo.today_rt_schedule_adherence_threshold_pax
			(
				service_date
				,route_id
				,direction_id
				,trip_id
				,stop_sequence
				,stop_id
				,vehicle_id
				,scheduled_arrival_time_sec
				,actual_arrival_time_sec
				,arrival_delay_sec
				,scheduled_departure_time_sec
				,actual_departure_time_sec
				,departure_delay_sec
				,stop_order_flag
				,threshold_id
				,threshold_value
				,denominator_pax
				,scheduled_threshold_numerator_pax
				,denominator_trip
				,scheduled_threshold_numerator_trip
			)

				SELECT DISTINCT
					sad.service_date
					,sad.route_id
					,sad.direction_id
					,sad.trip_id
					,sad.stop_sequence
					,sad.stop_id
					,vehicle_id
					,scheduled_arrival_time_sec
					,actual_arrival_time_sec
					,arrival_delay_sec
					,scheduled_departure_time_sec
					,actual_departure_time_sec
					,departure_delay_sec
					,stop_order_flag
					,th.threshold_id
					,thc.add_to AS threshold_value
					,po.from_stop_passenger_on AS denominator_pax
					,CASE
						WHEN sad.stop_order_flag = 1 AND sad.departure_delay_sec > thc.add_to THEN po.from_stop_passenger_on
						WHEN sad.stop_order_flag = 2 AND sad.arrival_delay_sec > thc.add_to THEN po.from_stop_passenger_on
						WHEN sad.stop_order_flag = 3 AND sad.arrival_delay_sec > thc.add_to THEN po.from_stop_passenger_on
						WHEN sad.stop_order_flag = 1 AND sad.departure_delay_sec <= thc.add_to THEN 0
						WHEN sad.stop_order_flag = 2 AND sad.arrival_delay_sec <= thc.add_to THEN 0
						WHEN sad.stop_order_flag = 3 AND sad.arrival_delay_sec <= thc.add_to THEN 0
						ELSE 0
					END AS scheduled_threshold_numerator_pax
					,1 AS denominator_trip
					,CASE
						WHEN sad.stop_order_flag = 1 AND sad.departure_delay_sec > thc.add_to THEN 1
						WHEN sad.stop_order_flag = 2 AND sad.arrival_delay_sec > thc.add_to THEN 1
						WHEN sad.stop_order_flag = 3 AND sad.arrival_delay_sec > thc.add_to THEN 1
						WHEN sad.stop_order_flag = 1 AND sad.departure_delay_sec <= thc.add_to THEN 0
						WHEN sad.stop_order_flag = 2 AND sad.arrival_delay_sec <= thc.add_to THEN 0
						WHEN sad.stop_order_flag = 3 AND sad.arrival_delay_sec <= thc.add_to THEN 0
						ELSE 0
					END AS scheduled_threshold_numerator_trip


				FROM	@today_rt_schedule_adherence_disaggregate sad
						,dbo.config_passenger_od_load_CR po
						,dbo.config_threshold th
						,dbo.config_threshold_calculation thc
						,dbo.config_mode_threshold mt
				WHERE
						sad.route_id = po.route_id
					AND 
						sad.trip_id = po.trip_id
					AND 
						sad.stop_id = po.from_stop_id
					AND 
						th.threshold_id = thc.threshold_id
					AND 
						th.threshold_type = 'wait_time_schedule_based'
					AND 
						mt.threshold_id = th.threshold_id
					AND 
						mt.threshold_id = thc.threshold_id
					AND 
						sad.route_type = 2 -- commuter rail only

			--save disaggregate travel times for today in real-time 

			INSERT INTO dbo.today_rt_travel_time_disaggregate
			(
				service_date
				,from_stop_id
				,to_stop_id
				,route_id
				,route_type
				,direction_id
				,start_time_sec
				,end_time_sec
				,travel_time_sec
				,benchmark_travel_time_sec
			)

				SELECT
					htt.service_date
					,htt.d_stop_id
					,htt.e_stop_id
					,htt.de_route_id
					,dtb.route_type
					,htt.de_direction_id
					,htt.d_time_sec
					,htt.e_time_sec
					,htt.de_time_sec
					,dtb.scheduled_average_travel_time_sec 
				FROM @today_rt_de_time_temp htt

				JOIN dbo.config_time_slice ts
					ON
						(
								htt.e_time_sec >= ts.time_slice_start_sec
							AND 
								htt.e_time_sec < ts.time_slice_end_sec
						)
				JOIN dbo.today_travel_time_benchmark dtb
					ON
						(
								htt.de_direction_id = dtb.direction_id
							AND 
								htt.d_stop_id = dtb.from_stop_id
							AND 
								htt.e_stop_id = dtb.to_stop_id
							AND 
								htt.de_route_id = dtb.route_id --added because of green line
							AND
								ts.time_slice_id = dtb.time_slice_id
						)

			--Save daily disaggregate headway times between the same origin-destination stops
			INSERT INTO dbo.today_rt_headway_time_od_disaggregate
			(
				service_date
				,stop_id
				,to_stop_id
				,route_id
				,prev_route_id
				,direction_id
				,start_time_sec
				,end_time_sec
				,headway_time_sec
				,benchmark_headway_time_sec
			)

				SELECT
					htt.service_date
					,htt.abcd_stop_id
					,htt.e_stop_id
					,htt.cde_route_id
					,htt.ab_route_id
					,htt.abcde_direction_id
					,htt.b_time_sec
					,htt.d_time_sec
					,htt.bd_time_sec
					,dtb.scheduled_average_headway_sec 
				FROM @today_rt_abcde_time_temp htt

				JOIN dbo.config_time_slice ts
					ON
						(
								htt.d_time_sec >= ts.time_slice_start_sec
							AND 
								htt.d_time_sec < ts.time_slice_end_sec
						)
				JOIN dbo.today_headway_time_od_benchmark dtb
					ON
						(
								htt.abcde_direction_id = dtb.direction_id
							AND 
								htt.abcd_stop_id = dtb.stop_id
							AND 
								htt.e_stop_id = dtb.to_stop_id
							AND 
								ts.time_slice_id = dtb.time_slice_id
						)

			--Save daily disaggregate headway times at a stop for trips of all routes
			INSERT INTO dbo.today_rt_headway_time_sr_all_disaggregate
			(
				service_date
				,stop_id
				,route_id
				,prev_route_id
				,direction_id
				,start_time_sec
				,end_time_sec
				,headway_time_sec
				,benchmark_headway_time_sec
			)

				SELECT
					htt.service_date
					,htt.bd_stop_id
					,htt.d_route_id
					,htt.b_route_id
					,htt.bd_direction_id
					,htt.b_time_sec
					,htt.d_time_sec
					,htt.bd_time_sec
					,dtb.scheduled_average_headway_sec 
				FROM @today_rt_bd_sr_all_time_temp htt

				JOIN dbo.config_time_slice ts
					ON
						(
								htt.d_time_sec >= ts.time_slice_start_sec
							AND 
								htt.d_time_sec < ts.time_slice_end_sec
						)
				JOIN dbo.today_headway_time_sr_all_benchmark dtb
					ON
						(
								htt.bd_direction_id = dtb.direction_id
							AND 
								htt.bd_stop_id = dtb.stop_id
							AND 
								ts.time_slice_id = dtb.time_slice_id
						)

			--Save daily disaggregate headway times at a stop for trips of the same route
			INSERT INTO dbo.today_rt_headway_time_sr_same_disaggregate
			(
				service_date
				,stop_id
				,route_id
				,prev_route_id
				,direction_id
				,start_time_sec
				,end_time_sec
				,headway_time_sec
				,benchmark_headway_time_sec
			)

				SELECT
					htt.service_date
					,htt.bd_stop_id
					,htt.bd_route_id
					,htt.bd_route_id
					,htt.bd_direction_id
					,htt.b_time_sec
					,htt.d_time_sec
					,htt.bd_time_sec
					,dtb.scheduled_average_headway_sec 
				FROM @today_rt_bd_sr_same_time_temp htt

				JOIN dbo.config_time_slice ts
					ON
						(
								htt.d_time_sec >= ts.time_slice_start_sec
							AND 
								htt.d_time_sec < ts.time_slice_end_sec
						)
				JOIN dbo.today_headway_time_sr_same_benchmark dtb
					ON
						(
								htt.bd_route_id = dtb.route_id 
							AND
								htt.bd_direction_id = dtb.direction_id
							AND 
								htt.bd_stop_id = dtb.stop_id
							AND 
								ts.time_slice_id = dtb.time_slice_id
						)


			-- Save disaggregate dwell times

			INSERT INTO dbo.today_rt_dwell_time_disaggregate
			(
				service_date
				,stop_id
				,route_id
				,direction_id
				,start_time_sec
				,end_time_sec
				,dwell_time_sec
			)

				SELECT
					service_date
					,cd_stop_id
					,cd_route_id
					,cd_direction_id
					,c_time_sec
					,d_time_sec
					,cd_time_sec
				FROM @today_rt_cd_time_temp

			--set event_processed_rt flag to 1 to indicate that the record has been processed
			UPDATE dbo.rt_event
				SET event_processed_rt = 1
				FROM dbo.rt_event rte,@unprocessed_events_file uef
				WHERE
						rte.service_date = uef.service_date
					AND 
						rte.record_id = uef.record_id

			DELETE FROM @unprocessed_events_file

			DELETE FROM @today_rt_cd_time_temp

			DELETE FROM @today_rt_bd_sr_all_time_temp

			DELETE FROM @today_rt_bd_sr_same_time_temp

			DELETE FROM @today_rt_abcde_time_temp

			DELETE FROM @today_rt_cde_time_temp

			DELETE FROM @today_rt_de_time_temp

			SET @file_time_num_current = @file_time_num_current + 1

		END

	END

END

GO