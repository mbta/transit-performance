
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('getDwellTimes','P') IS NOT NULL
	DROP PROCEDURE dbo.getDwellTimes
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE dbo.getDwellTimes

--Script Version: Master - 1.1.0.0

--This Procedure is called by the dwelltimes API call. It selects dwell times for a particular stop (and optionally route + direction) and time period.

	@stop_id VARCHAR(255)
	,@route_id VARCHAR(255)
	,@direction_id VARCHAR(255)
	,@from_time DATETIME
	,@to_time DATETIME


AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @dwelltimestemp AS TABLE
	(
		route_id		VARCHAR(255)
		,direction_id	INT
		,start_time		DATETIME
		,end_time		DATETIME
		,dwell_time_sec	INT
	)

	IF (DATEDIFF(D,@from_time,@to_time) <= 7) --only return results for 7-day span

	BEGIN --if a timespan is less than 7 days, then do the processing, if not return empty set

		INSERT INTO @dwelltimestemp
			SELECT --selects dwell times from today, if the from_time and to_time are today
				route_id
				,direction_id
				,DATEADD(s,start_time_sec,service_date) AS start_time
				,DATEADD(s,end_time_sec,service_date) AS end_time
				,dwell_time_sec
			FROM dbo.today_rt_dwell_time_disaggregate
			WHERE
					stop_id = @stop_id
				AND 
					(route_id = @route_id OR @route_id IS NULL)
				AND 
					(direction_id = @direction_id OR @direction_id IS NULL)
				AND 
					DATEADD(s,end_time_sec,service_date) >= @from_time
				AND 
					DATEADD(s,end_time_sec,service_date) <= @to_time

			UNION

			SELECT --selects dwell times from other days in the past, if the from_time and to_time are not today
				route_id
				,direction_id
				,DATEADD(s,start_time_sec,service_date) AS start_time
				,DATEADD(s,end_time_sec,service_date) AS end_time
				,dwell_time_sec
			FROM dbo.historical_dwell_time_disaggregate
			WHERE
					stop_id = @stop_id
				AND 
					(route_id = @route_id OR @route_id IS NULL)
				AND 
					(direction_id = @direction_id OR @direction_id IS NULL)
				AND 
					DATEADD(s,end_time_sec,service_date) >= @from_time
				AND 
					DATEADD(s,end_time_sec,service_date) <= @to_time

	END--if a timespan is less than 7 days, then do the processing, if not return empty set
	
	SELECT
		d.route_id
		,direction_id
		,start_time
		,end_time
		,dwell_time_sec
	FROM @dwelltimestemp d
	JOIN gtfs.routes r
	ON
		d.route_id = r.route_id
	WHERE
		r.route_type <> 2 --do not return results for Commuter Rail
	ORDER BY end_time

END






GO