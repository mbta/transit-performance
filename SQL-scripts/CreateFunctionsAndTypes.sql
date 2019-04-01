
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('fnConvertDateTimeToEpoch') IS NOT NULL
	DROP FUNCTION dbo.fnConvertDateTimeToEpoch
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION dbo.fnConvertDateTimeToEpoch 
( 
      @fromDateTime datetime2 --in current timezone
)
RETURNS INT
AS
BEGIN
	DECLARE @timezone_diff_epoch INT
	SET @timezone_diff_epoch = DATEDIFF(s,GETUTCDATE(),GETDATE())
		
	DECLARE @toEpoch INT; ---in GMT
	SET @toEpoch = DATEDIFF(second, '1970-01-01 00:00:00', @fromDateTime)- DATEDIFF(s,GETUTCDATE(),GETDATE());
	
	RETURN @toEpoch;

END



GO

IF OBJECT_ID ('fnConvertDateTimeToServiceDate') IS NOT NULL
DROP FUNCTION dbo.fnConvertDateTimeToServiceDate
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE FUNCTION dbo.fnConvertDateTimeToServiceDate 
( 
      @service_datetime DATETIME 
)
RETURNS DATE
AS
BEGIN
DECLARE	@service_date DATE 

	IF CONVERT(TIME, @service_datetime) > '03:30:00.000'
		SET @service_date = CONVERT(DATE, @service_datetime)
		ELSE SET @service_date = DATEADD(d,-1,CONVERT(DATE, @service_datetime))

	
	RETURN @service_date;

END




GO

IF OBJECT_ID ('fnConvertEpochToDateTime') IS NOT NULL
DROP FUNCTION dbo.fnConvertEpochToDateTime
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION dbo.fnConvertEpochToDateTime 
( 
      @fromEpoch INT --in GMT
)
 RETURNS datetime2
AS
BEGIN
	DECLARE @timezone_diff_epoch INT
	SET @timezone_diff_epoch = DATEDIFF(s,GETUTCDATE(),GETDATE())
		
	DECLARE @toDateTime datetime2; ---in current time zone
	SET @toDateTime = DATEADD(s, @fromEpoch + @timezone_diff_epoch, '1970-01-01');
	
	RETURN @toDateTime;

END




GO

IF TYPE_ID ('dbo.int_val_type') IS NOT NULL
	DROP TYPE dbo.int_val_type
GO


CREATE TYPE dbo.int_val_type AS TABLE(
	int_val int NULL
)
GO

IF TYPE_ID ('dbo.str_val_type') IS NOT NULL
	DROP TYPE dbo.str_val_type
GO

CREATE TYPE dbo.str_val_type AS TABLE(
	str_val varchar(255) NULL
)
GO







