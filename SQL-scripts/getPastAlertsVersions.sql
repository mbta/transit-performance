
---run this script in the transit-performance database
--USE transit_performance
--GO

--This stored procedure is called by the Alerts API call.  It selects alerts for a particular route, direction, stop and time period.

IF OBJECT_ID('dbo.getPastAlertsVersions','P') IS NOT NULL
	DROP PROCEDURE dbo.getPastAlertsVersions
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getPastAlertsVersions
	
	@alert_id				VARCHAR(255)
	,@from_time				DATETIME 
	,@to_time				DATETIME 
	,@include_all_versions	BIT = 0 --default is FALSE, do not include all version
	
AS

BEGIN
    SET NOCOUNT ON; 

	DECLARE @alertsversionstemp AS TABLE
	(
		alert_id				VARCHAR(255)
		,version_id				INT
		,valid_from				DATETIME2
		,valid_to				DATETIME2
		,cause					VARCHAR(255)
		,effect					VARCHAR(255)
		,header_text			VARCHAR(255)
		,description_text		VARCHAR(1000)
		,url					VARCHAR(255)
	)
		
	IF @include_all_versions = 1

	BEGIN

		INSERT INTO @alertsversionstemp
		(
			alert_id
			,version_id	
			,valid_from
			,valid_to
			,cause
			,effect
			,header_text
			,description_text
			,url									
		)

		SELECT DISTINCT 
			a.alert_id
			,a.version_id
			,dbo.fnConvertEpochToDateTime (a.first_file_time) as valid_from
			,dbo.fnConvertEpochToDateTime (a.last_file_time) as valid_to
			,a.cause
			,a.effect
			,a.header_text
			,a.description_text
			,a.url

		FROM
			dbo.rt_alert a

		WHERE
					a.alert_id = @alert_id
				AND
					a.closed = 0

	END

	ELSE
	BEGIN

		INSERT INTO @alertsversionstemp
		(
			alert_id
			,version_id
			,valid_from
			,valid_to
			,cause
			,effect
			,header_text
			,description_text
			,url								
		)

		SELECT DISTINCT 
			a.alert_id
			,a.version_id
			,dbo.fnConvertEpochToDateTime (a.first_file_time) as valid_from
			,dbo.fnConvertEpochToDateTime (a.last_file_time) as valid_to
			,a.cause
			,a.effect
			,a.header_text
			,a.description_text
			,a.url

		FROM
			dbo.rt_alert a

		WHERE
					a.alert_id = @alert_id
				AND
					a.first_file_time <= dbo.fnConvertDateTimeToEpoch(@to_time)
				AND
					a.last_file_time >= dbo.fnConvertDateTimeToEpoch(@from_time)
				AND
					a.closed = 0
		
	END

	SELECT
		alert_id				
		,version_id	
		,valid_from
		,valid_to
		,cause
		,effect
		,header_text
		,description_text
		,url								
	FROM @alertsversionstemp
	ORDER BY alert_id, version_id

END

GO


