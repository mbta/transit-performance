
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.getPastAlertsInformedEntities','P') IS NOT NULL
	DROP PROCEDURE dbo.getPastAlertsInformedEntities
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getPastAlertsInformedEntities

--Script Version: Master - 1.1.0.0

--This stored procedure is called by the Alerts API call.  It selects alerts for a particular route, direction, stop and time period.
	
	@alert_id			VARCHAR(255)
	,@version_id		INT
	
AS

BEGIN
    SET NOCOUNT ON; 

	DECLARE @alertstempentities AS TABLE
	(
		alert_id				VARCHAR(255)
		,version_id				INT
		,agency_id				VARCHAR(255)
		,route_id				VARCHAR(255)
		,route_type				INT
		,trip_id				VARCHAR(255)
		,stop_id				VARCHAR(255)
	)

		
	INSERT INTO @alertstempentities
	(
		alert_id				
		,version_id									
		,agency_id				
		,route_id				
		,route_type				
		,trip_id				
		,stop_id						
	)

	SELECT DISTINCT 
		e.alert_id
		,e.version_id
		,e.agency_id
		,e.route_id
		,e.route_type
		,e.trip_id
		,e.stop_id
	FROM
		dbo.rt_alert_informed_entity e
	WHERE
			e.alert_id = @alert_id
		AND
			e.version_id = @version_id

	SELECT
		alert_id				
		,version_id									
		,agency_id				
		,route_id				
		,route_type				
		,trip_id				
		,stop_id				
	FROM @alertstempentities
	ORDER BY alert_id, version_id

	

END

GO


