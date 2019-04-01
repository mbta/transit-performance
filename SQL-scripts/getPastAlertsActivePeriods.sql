
---run this script in the transit-performance database
--USE transit_performance
--GO

--This stored procedure is called by the Alerts API call.  It selects alerts for a particular route, direction, stop and time period.

IF OBJECT_ID('dbo.getPastAlertsActivePeriods','P') IS NOT NULL
	DROP PROCEDURE dbo.getPastAlertsActivePeriods
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getPastAlertsActivePeriods
	
	@alert_id				VARCHAR(255)
	,@version_id			INT
	
AS

BEGIN
    SET NOCOUNT ON; 

	DECLARE @alertstempactiveperiod AS TABLE
	(
		alert_id				VARCHAR(255)
		,version_id				INT
		,active_period_start	INT
		,active_period_end		INT
	)

		
	INSERT INTO @alertstempactiveperiod
	(
		alert_id				
		,version_id								
		,active_period_start	
		,active_period_end		
	)

	SELECT DISTINCT 
		p.alert_id
		,p.version_id
		,p.active_period_start
		,p.active_period_end

	FROM
		dbo.rt_alert_active_period p
	WHERE
			p.alert_id = @alert_id
		AND
			p.version_id = @version_id

	SELECT
		alert_id				
		,version_id								
		,active_period_start	
		,active_period_end	
	FROM @alertstempactiveperiod
	ORDER BY alert_id, version_id, active_period_start, active_period_end

END

GO


