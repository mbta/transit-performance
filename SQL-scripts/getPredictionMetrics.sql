
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('getPredictionMetrics') IS NOT NULL
	DROP PROCEDURE dbo.getPredictionMetrics
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE dbo.getPredictionMetrics

--Script Version: Master - 1.1.0.0

--This stored procedure is called by the predictionmetrics API call.  It selects prediction metrics for a particular route(s), direction(s), and stop(s) and time period.

	@route_id		VARCHAR(255)
	,@direction_id	INT
	,@stop_id		VARCHAR(255)
	,@from_time		DATETIME 
	,@to_time		DATETIME 

AS

BEGIN
	SET NOCOUNT ON;

	DECLARE @metricstemp AS TABLE
	(
		service_date							VARCHAR(255)
		,route_id								VARCHAR(255)
		,direction_id							INT	
		,stop_id								VARCHAR(255)
		,time_slice_start_sec					INT
		,time_slice_end_sec						INT
		,threshold_id							VARCHAR(255)
		,threshold_type							VARCHAR(255)
		,threshold_name							VARCHAR(255)
		,total_predictions_within_threshold		INT
		,total_predictions_in_bin				INT
		,metric_result							FLOAT
	)

	DECLARE @service_date_from DATE
	SET @service_date_from = dbo.fnConvertDateTimeToServiceDate(@from_time)

	DECLARE @service_date_to DATE
	SET @service_date_to = dbo.fnConvertDateTimeToServiceDate(@to_time)

	IF @service_date_from = @service_date_to --only return results for one day
	
	BEGIN

		INSERT INTO @metricstemp
			SELECT 
				service_date
				,route_id
				,direction_id
				,stop_id
				,dbo.fnConvertDateTimeToEpoch (DATEADD(second, t.time_slice_start_sec, service_date)) as time_slice_start_sec
				,dbo.fnConvertDateTimeToEpoch (DATEADD(second, t.time_slice_end_sec, service_date)) as time_slice_end_sec
				,threshold_id
				,threshold_type
				,threshold_name
				,total_predictions_within_threshold
				,total_predictions_in_bin
				,metric_result

			FROM dbo.historical_prediction_metrics_disaggregate p
			JOIN dbo.config_time_slice t
			ON
				p.time_slice_id = t.time_slice_id

			WHERE
					route_id = ISNULL(@route_id, route_id)
				AND
					direction_id = ISNULL(@direction_id, direction_id)
				AND
					stop_id = ISNULL(@stop_id, stop_id)
				AND
					DATEADD(s,time_slice_start_sec,service_date) >= @from_time
				AND
					DATEADD(s,time_slice_end_sec,service_date) <= @to_time

	END 

	SELECT
		service_date
		,route_id
		,direction_id
		,stop_id
		,time_slice_start_sec
		,time_slice_end_sec
		,threshold_id
		,threshold_type
		,threshold_name
		,total_predictions_within_threshold
		,total_predictions_in_bin
		,metric_result
	FROM @metricstemp
	ORDER BY
		service_date
		,route_id
		,direction_id
		,stop_id
		,time_slice_start_sec
		,threshold_id

END

GO