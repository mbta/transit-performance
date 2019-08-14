
---run this script in the transit-performance database
--USE transit_performance
--GO

--This stored procedure is called by the getEvents API call.  It selects events for a particular route, direction, stop and time period.

IF OBJECT_ID('dbo.getEvents','P') IS NOT NULL
	DROP PROCEDURE dbo.getEvents
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getEvents
	
	@route_id		VARCHAR(255)
	,@direction_id	INT
	,@stop_id		VARCHAR(255)
	,@vehicle_label	VARCHAR(255)
	,@from_time		DATETIME
	,@to_time		DATETIME
	
AS

BEGIN
    SET NOCOUNT ON; 

	DECLARE @eventstemp AS TABLE
	(
		service_date		DATE
		,route_id			VARCHAR(255)
		,trip_id			VARCHAR(255)
		,direction_id		INT
		,stop_id			VARCHAR(255)
		,stop_name			VARCHAR(255)
		,stop_sequence		INT
		,vehicle_id			VARCHAR(255)
		,vehicle_label		VARCHAR(255)
		,event_type			CHAR(3)
		,event_time			INT
		,event_time_sec		INT
	)

	DECLARE @service_date_from DATE
	SET @service_date_from = dbo.fnConvertDateTimeToServiceDate(@from_time)

	DECLARE @service_date_to DATE
	SET @service_date_to = dbo.fnConvertDateTimeToServiceDate(@to_time)

	IF (@service_date_from = @service_date_to) --only return results for one day

	BEGIN

		DECLARE @service_date  DATE
		SET @service_date = @service_date_from

		DECLARE @today_service_date DATE
		SET @today_service_date = dbo.fnConvertDateTimeToServiceDate(GETDATE())

		IF  @service_date = @today_service_date
		
		BEGIN --if service_date is today, use today_rt_event table

			INSERT INTO @eventstemp
			(
				service_date
				,route_id
				,trip_id
				,direction_id
				,stop_id
				,stop_name
				,stop_sequence
				,vehicle_id
				,vehicle_label
				,event_type
				,event_time
				,event_time_sec
			)

			SELECT 
				service_date
				,route_id
				,trip_id
				,direction_id
				,e.stop_id
				,s.stop_name
				,e.stop_sequence
				,vehicle_id
				,vehicle_label
				,event_type
				,event_time
				,event_time_sec

			FROM
				dbo.today_rt_event e
			JOIN
				gtfs.stops s
			ON
				e.stop_id = s.stop_id

			WHERE
						e.service_date = @service_date
					AND
						(route_id = @route_id OR @route_id IS NULL)
					AND
						(direction_id = @direction_id OR @direction_id IS NULL) 
					AND
						(e.stop_id = @stop_id OR @stop_id IS NULL)
					AND
						(e.vehicle_label = @vehicle_label OR @vehicle_label IS NULL)
					AND
						event_time >= dbo.fnConvertDateTimeToEpoch(@from_time)
					AND
						event_time <= dbo.fnConvertDateTimeToEpoch(@to_time)

		END

		ELSE --if service_date is not today, use historical_event table

		BEGIN

			INSERT INTO @eventstemp
			(
				service_date
				,route_id
				,trip_id
				,direction_id
				,stop_id
				,stop_name
				,stop_sequence
				,vehicle_id
				,vehicle_label
				,event_type
				,event_time
				,event_time_sec
			) 

			SELECT 
				service_date
				,route_id
				,trip_id
				,direction_id
				,e.stop_id
				,s.stop_name
				,e.stop_sequence
				,vehicle_id
				,vehicle_label
				,event_type
				,event_time
				,event_time_sec

			FROM
				dbo.historical_event e
			JOIN
				gtfs.stops s
			ON
				e.stop_id = s.stop_id

			WHERE
						e.service_date = @service_date
					AND
						(route_id = @route_id OR @route_id IS NULL)
					AND
						(direction_id = @direction_id OR @direction_id IS NULL) 
					AND
						(e.stop_id = @stop_id OR @stop_id IS NULL)
					AND
						(e.vehicle_label = @vehicle_label OR @vehicle_label IS NULL)
					AND
						event_time >= dbo.fnConvertDateTimeToEpoch(@from_time)
					AND
						event_time <= dbo.fnConvertDateTimeToEpoch(@to_time)
					AND
						suspect_record = 0

		END

	END

	SELECT
		service_date
		,e.route_id
		,trip_id
		,direction_id
		,stop_id
		,stop_name
		,stop_sequence
		,vehicle_id
		,vehicle_label
		,event_type
		,event_time
		,event_time_sec
	FROM @eventstemp e
	JOIN gtfs.routes r
	ON
		e.route_id = r.route_id
	WHERE
		r.route_type <> 2 --do not return results for Commuter Rail
	ORDER BY event_time

END

GO


