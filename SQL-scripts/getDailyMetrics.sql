
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('getDailyMetrics') IS NOT NULL
	DROP PROCEDURE dbo.getDailyMetrics
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE dbo.getDailyMetrics

--Script Version: Master - 1.2.0.0

--This stored procedure is called by the dailymetrics API call.  It selects daily metrics for a particular route (or all routes) and time period.

	@route_ids str_val_type READONLY
	,@from_date VARCHAR(255)
	,@to_date VARCHAR(255)

AS

BEGIN
	SET NOCOUNT ON;

	DECLARE @limit_date DATE = DATEADD(DAY, -90, CONVERT(DATE,GETDATE())) 

	DECLARE @metricstemp AS TABLE
	(
		service_date		VARCHAR(255)
		,route_id			VARCHAR(255)
		,threshold_id		VARCHAR(255)
		,threshold_name		VARCHAR(255)
		,threshold_type		VARCHAR(255)
		,time_period_type	VARCHAR(255)
		,metric_result		FLOAT
		,metric_result_trip	FLOAT
	)

	IF
		(
			(DATEDIFF(D,@from_date,@to_date) <= 31)
		AND 
			((SELECT COUNT(str_val) FROM @route_ids WHERE str_val NOT IN ('Red','Orange','Blue','Green-B','Green-C','Green-D','Green-E'))= 0)
		)

	BEGIN --if a timespan is less than 6 hours and routes are only subway/light rail, then do the processing, if not return empty set

		INSERT INTO @metricstemp
			SELECT --selects pre-calculated daily metrics from days in the past, if the from_date and to_date are not today 
				service_date
				,route_id
				,threshold_id
				,threshold_name
				,threshold_type
				,time_period_type
				,metric_result
				,metric_result_trip --added

			FROM dbo.historical_metrics

			WHERE
					(
						(SELECT COUNT(str_val) FROM @route_ids) = 0
					OR 
						route_id IN (SELECT str_val FROM @route_ids)
					)
				AND 
					service_date >= @from_date
				AND 
					service_date <= @to_date
				AND 
					route_id IN ('Red','Orange','Blue','Green-B','Green-C','Green-D','Green-E')

	END --if a timespan is less than 31 days and routes are only subway/light rail, then do the processing, if not return empty set

	SELECT
		service_date
		,route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,time_period_type
		,metric_result
		,metric_result_trip
	FROM @metricstemp
	WHERE service_date > = @limit_date
	ORDER BY
		service_date,route_id,threshold_id,time_period_type


END







GO