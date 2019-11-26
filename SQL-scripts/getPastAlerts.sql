
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.getPastAlerts','P') IS NOT NULL
	DROP PROCEDURE dbo.getPastAlerts
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getPastAlerts

--Script Version: Master - 1.1.0.0
	
--This stored procedure is called by the Alerts API call.  It selects alerts for a particular route, direction, stop and time period.

	@route_id				VARCHAR(255)
	,@stop_id				VARCHAR(255)
	,@trip_id				VARCHAR(255)
	,@from_time				DATETIME 
	,@to_time				DATETIME 
	
AS

BEGIN
    SET NOCOUNT ON; 

	IF (DATEDIFF(D,@from_time,@to_time) <= 31)
	BEGIN --if a timespan is less than 31 days, then do the processing, if not return empty set

		DECLARE @alertstemp AS TABLE
		(
			alert_id				VARCHAR(255)
			,min_first_file_time	DATETIME
			,max_last_file_time		DATETIME
			,closed					INT
		)

		INSERT INTO @alertstemp
		(
			alert_id
			,min_first_file_time
			,max_last_file_time
			,closed
		)

		SELECT DISTINCT 
			a.alert_id
			,dbo.fnConvertEpochToDateTime (MIN(a.first_file_time)) as min_first_file_time
			,dbo.fnConvertEpochToDateTime (MAX(a.last_file_time)) as max_last_file_time
			,a.closed

		FROM
			dbo.rt_alert a
		JOIN
			dbo.rt_alert_active_period p
		ON
				a.alert_id = p.alert_id
			AND
				a.version_id = p.version_id
		JOIN
			dbo.rt_alert_informed_entity e
		ON
				a.alert_id = e.alert_id
			AND
				a.version_id = e.version_id

		WHERE
					(e.route_id = @route_id OR @route_id IS NULL)
				AND
					(e.stop_id = @stop_id OR @stop_id IS NULL)
				AND
					(e.trip_id = @trip_id OR @trip_id IS NULL)
				AND
					p.active_period_start <= dbo.fnConvertDateTimeToEpoch(@to_time)
				AND
					p.active_period_end >= dbo.fnConvertDateTimeToEpoch(@from_time)
		GROUP BY
			a.alert_id
			,a.closed

		ORDER BY a.alert_id

	END

	SELECT DISTINCT 
		alert_id
	FROM @alertstemp 
	WHERE
			min_first_file_time <= @to_time
		AND
			max_last_file_time >= @from_time

END

GO


