
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('dbo.UpdateGTFSNEXT','P') IS NOT NULL
	DROP PROCEDURE dbo.UpdateGTFSNext
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE PROCEDURE dbo.UpdateGTFSNext

AS

BEGIN
	SET NOCOUNT ON;   

	UPDATE gtfs_next.stop_times
		--Calculate seconds after midnight and update stop times table
	SET 
		arrival_time_sec =

		CASE
			WHEN LEN(arrival_time)=8
				THEN SUBSTRING(arrival_time,1,2)*3600+SUBSTRING(arrival_time,4,2)*60+SUBSTRING(arrival_time,7,2)
			WHEN LEN(arrival_time)=7
				THEN SUBSTRING(arrival_time,1,1)*3600+SUBSTRING(arrival_time,3,2)*60+SUBSTRING(arrival_time,6,2)	
		END
		,
		departure_time_sec  =
		
		CASE
			WHEN LEN(departure_time)=8
				THEN SUBSTRING(departure_time,1,2)*3600+SUBSTRING(departure_time,4,2)*60+SUBSTRING(departure_time,7,2)
			WHEN LEN(departure_time)=7
				THEN SUBSTRING(departure_time,1,1)*3600+SUBSTRING(departure_time,3,2)*60+SUBSTRING(departure_time,6,2)	
		END 
		;
 
--create gtfs_next.route_direction_stop

-- Route List
IF OBJECT_ID('tempdb..#route_list_temp', 'u') is not null 
		DROP TABLE #route_list_temp;
		
CREATE TABLE #route_list_temp (
	agency_id		VARCHAR(255) NOT NULL,
	agency_name		VARCHAR(255) NOT NULL,
	route_type		INT NOT NULL,
	mode_name		VARCHAR(255) NOT NULL,
	route_id		VARCHAR(255) NOT NULL PRIMARY KEY,
	route_name		VARCHAR(255) NOT NULL,
	route_do		INT NOT NULL
	);
		
INSERT INTO #route_list_temp
	SELECT		
			a.agency_id,
			a.agency_name,
			r.route_type,
			'0' as mode_name,
			r.route_id,
			CASE 
				WHEN r.route_long_name is null or r.route_long_name = '' THEN r.route_short_name 
				ELSE r.route_long_name 
				END as route_name,
			
			0 as route_do
	FROM		
		gtfs_next.agency a, gtfs_next.routes r
		
	WHERE	a.agency_name <> 'Massport'
			and a.agency_id = r.agency_id
			
		
--temp table for route_do
			
IF OBJECT_ID('tempdb..#route_do_temp','u') IS NOT NULL
DROP TABLE #route_do_temp
		
CREATE TABLE #route_do_temp (
	route_do		INT IDENTITY PRIMARY KEY,
	route_id		VARCHAR(255),
	)
		
--insert non-bus routes--
		
INSERT INTO #route_do_temp (
		route_id
		)

	SELECT		
		rl.route_id
	FROM		
		#route_list_temp rl
	WHERE
		rl.route_type <> 3
	ORDER BY
		rl.route_type,
		rl.route_name,
		route_id
	;
			
	--insert bus routes--
	--starts with a letter--
		
INSERT INTO #route_do_temp (
		route_id
		)

	SELECT		
		rl.route_id
	FROM		
		#route_list_temp rl
	WHERE
		rl.route_type = 3 --bus
		and left(rl.route_name,1) not like '[0-9]' --starts with a letter
	ORDER BY
		rl.route_name
	;
	--insert bus routes--
	--starts with a number--
		
INSERT INTO #route_do_temp (
		route_id
		)

	SELECT		
		rl.route_id
	FROM		
		#route_list_temp rl
	WHERE
		rl.route_type = 3 --bus
		and left(rl.route_name,1) like '[0-9]' --starts with a number
	ORDER BY 
		CASE WHEN SUBSTRING(rl.route_name,1,4) like '[0-9][0-9][0-9][0-9]'	THEN CAST(SUBSTRING(rl.route_name,1,4) as INT)
				WHEN SUBSTRING(rl.route_name,1,3) like '[0-9][0-9][0-9]'	THEN CAST(SUBSTRING(rl.route_name,1,3) as INT)
				WHEN SUBSTRING(rl.route_name,1,2) like '[0-9][0-9]'         THEN CAST(SUBSTRING(rl.route_name,1,2) as INT)
				WHEN SUBSTRING(rl.route_name,1,1) like '[0-9]'              THEN CAST(SUBSTRING(rl.route_name,1,1) as INT)
				ELSE 10000 -- put any "leftovers" at the end
				END
	;
		

--update route_do
UPDATE #route_list_temp
SET route_do = rdo.route_do
FROM #route_do_temp rdo
WHERE #route_list_temp.route_id = rdo.route_id
		
IF OBJECT_ID('tempdb..#route_do_temp','u') IS NOT NULL
DROP TABLE #route_do_temp
;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='IX_gtfs_next__route_list_routetype' AND object_id = OBJECT_ID('gtfs_next._route_list'))
	CREATE NONCLUSTERED INDEX IX_gtfs_next__route_list_routetype ON #route_list_temp(route_type)


-- Route Direction List -------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#route_direction_list_temp', 'u') is not null 
		DROP TABLE #route_direction_list_temp;
		
CREATE TABLE #route_direction_list_temp (
	route_type INT NOT NULL,
	mode_name VARCHAR(255) NOT NULL,
	route_id VARCHAR(255) NOT NULL,
	route_name VARCHAR(255) NOT NULL,
	route_do INT NOT NULL,
	direction_id INT NOT NULL,
	direction_name VARCHAR(255) NOT NULL,
	route_direction_id VARCHAR(255) PRIMARY KEY NOT NULL
	);

CREATE NONCLUSTERED INDEX IX_gtfs_next__rdl_route_id ON #route_direction_list_temp(route_id)

IF OBJECT_ID('tempdb..#direction_name_temp','u') IS NOT NULL
DROP TABLE #direction_name_temp
		
--temp table for direction name to be overridden
CREATE TABLE #direction_name_temp (
	route_id		VARCHAR(255) NOT NULL,
	direction_id	INT NOT NULL,
	direction_name	VARCHAR(255) NOT NULL
	)
			
INSERT INTO #direction_name_temp
		SELECT DISTINCT rl.route_id, dne.direction_id,dne.direction_name
				FROM #route_list_temp rl, gtfs_next.direction_names_exceptions dne
				WHERE rl.route_name = dne.route_name
		
INSERT INTO #route_direction_list_temp
	SELECT		
			rl.route_type,
			rl.mode_name,
			rl.route_id,
			rl.route_name,
			rl.route_do,
			t.direction_id,
			CASE
				WHEN rl.route_id in (SELECT dnt.route_id FROM #direction_name_temp dnt) THEN (SELECT TOP 1 dnt.direction_name FROM #direction_name_temp dnt WHERE rl.route_id = dnt.route_id and t.direction_id = dnt.direction_id)
				WHEN t.direction_id = 0 THEN 'Outbound'
				WHEN t.direction_id = 1 THEN 'Inbound'
				ELSE 'None'
			END as direction_name,
			rl.route_id + '_' + CAST (t.direction_id as varchar(1)) as route_direction_id
	FROM		
			 #route_list_temp rl, gtfs_next.trips t					
	WHERE	
			rl.route_id = t.route_id
	GROUP BY
			rl.route_type,
			rl.mode_name,
			rl.route_id,
			rl.route_name,
			rl.route_do,
			t.direction_id
	ORDER BY
			rl.route_type,
			rl.route_id,
			t.direction_id

	;

	


-- Route Direction Stop List-------------------------------------------------------------------------------------------------------


IF OBJECT_ID('gtfs_next.route_direction_stop', 'u') is not null 
		DROP TABLE gtfs_next.route_direction_stop;
		
CREATE TABLE gtfs_next.route_direction_stop (
	route_type INT NOT NULL,
	route_id VARCHAR(255) NOT NULL,
	direction_id INT NOT NULL, 
	stop_order INT NOT NULL,
	stop_id VARCHAR(255) NOT NULL, 
	);

CREATE NONCLUSTERED INDEX IX_gtfs_next__rdsl_route_id ON gtfs_next.route_direction_stop(route_id)
				
INSERT INTO gtfs_next.route_direction_stop
	SELECT		
			rdl.route_type,
			rdl.route_id,
			rdl.direction_id, 
			MAX(st.stop_sequence) AS stop_order,
			s.stop_id 
	FROM		
			#route_direction_list_temp rdl,
			gtfs_next.trips t, 
			gtfs_next.stop_times st, 
			gtfs_next.stops s					
	WHERE    
			rdl.route_id			= t.route_id
			and rdl.direction_id	= t.direction_id
			and t.trip_id			= st.trip_id
			and st.stop_id			= s.stop_id 					
	GROUP BY 
			rdl.route_type,
			rdl.mode_name,
			rdl.route_id,
			rdl.route_name,
			rdl.route_do,
			rdl.direction_id, 
			rdl.direction_name,
			rdl.route_direction_id, 
			s.stop_id, 
			s.stop_name,
			s.parent_station,
			s.stop_lat,
			s.stop_lon	
	ORDER BY
			rdl.route_id, 
			rdl.direction_id, 
			stop_order
	;



END




GO


