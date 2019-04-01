
---run this script in the transit-performance database
--USE transit_performance
--GO

--This stored procedure is called by the dailymetrics API call.  It selects daily metrics for a particular route (or all routes) and time period.

IF OBJECT_ID('getCurrentMetrics','P') IS NOT NULL
	DROP PROCEDURE dbo.getCurrentMetrics

GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE dbo.getCurrentMetrics

	@route_ids str_val_type READONLY

AS

BEGIN
	SET NOCOUNT ON;

	DECLARE @metricstemp AS TABLE
	(
		route_id						VARCHAR(255)
		,threshold_id					VARCHAR(255)
		,threshold_name					VARCHAR(255)
		,threshold_type					VARCHAR(255)
		,metric_result_last_hour		FLOAT
		,metric_result_current_day		FLOAT
		,metric_result_trip_last_hour	FLOAT
		,metric_result_trip_current_day	FLOAT
	)

	IF
		(
		(
			SELECT
				COUNT(str_val)
			FROM @route_ids
			WHERE
				str_val NOT IN ('Red','Orange','Blue','Green-B','Green-C','Green-D','Green-E')
		)
		= 0)

	BEGIN --if routes are only subway/light rail, then do the processing, if not return empty set

		INSERT INTO @metricstemp
			SELECT --selects pre-calculated daily metrics from days in the past, if the from_date and to_date are not today 
				route_id
				,threshold_id
				,threshold_name
				,threshold_type
				,metric_result_last_hour
				,metric_result_current_day
				,metric_result_trip_last_hour
				,metric_result_trip_current_day

			FROM dbo.today_rt_current_metrics

			WHERE
				(
				(
					SELECT
						COUNT(str_val)
					FROM @route_ids
				)
				= 0
				OR route_id IN
				(
					SELECT
						str_val
					FROM @route_ids
				)
				)
				AND route_id IN ('Red','Orange','Blue','Green-B','Green-C','Green-D','Green-E')


	END --if routes are only subway/light rail, then do the processing, if not return empty set

	SELECT
		route_id
		,threshold_id
		,threshold_name
		,threshold_type
		,metric_result_last_hour
		,metric_result_current_day
		,metric_result_trip_last_hour
		,metric_result_trip_current_day
	FROM @metricstemp
	ORDER BY
		route_id,threshold_id

END








GO