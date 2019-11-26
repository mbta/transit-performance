
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.ClearData','P') IS NOT NULL
	DROP PROCEDURE dbo.ClearData

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.ClearData

--Script Version: Master - 1.1.0.0 

--This procedure processes all of the events for the service_date being processed. It runs after the PreProcessDaily.

	@number_of_days		INT			--number of days to keep data

AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @number_of_days_process INT = @number_of_days

	DECLARE	@service_date DATE = GETDATE()

	DECLARE @service_date_epoch INT = (SELECT dbo.fnConvertDateTimeToEpoch(@service_date))

	IF OBJECT_ID ('dbo.gtfsrt_tripupdate_denormalized','U') IS NOT NULL

		DELETE FROM dbo.gtfsrt_tripupdate_denormalized
		WHERE header_timestamp < (@service_date_epoch - @number_of_days_process*86400)

	IF OBJECT_ID ('dbo.gtfsrt_vehicleposition_denormalized','U') IS NOT NULL

		DELETE FROM dbo.gtfsrt_vehicleposition_denormalized
		WHERE header_timestamp < (@service_date_epoch - @number_of_days_process*86400)

END

GO