
---run this script in the transit-performance database
--USE transit_performance
--GO

IF OBJECT_ID('PreProcessToday','P') IS NOT NULL
	DROP PROCEDURE dbo.PreProcessToday

GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE dbo.PreProcessToday 

--Script Version: Master - 1.1.0.0

--This procedure sets up the today tables. These tables store the performance information for the day being processed events after the day has happened.
--This procedure is run at the end of the service_date and processes all information for that date after it has ended. 

	@service_date DATE

AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @service_date_process DATE
	SET @service_date_process = @service_date

	--Create a table to determine valid service_ids for day being processed restricting to light rail and subway only

	DECLARE @day_of_the_week VARCHAR(255);
	SET @day_of_the_week =
	(
		SELECT
			DATENAME(dw,@service_date_process)
	);

	DECLARE @day_type_id VARCHAR(255);
	SET @day_type_id =
	(
		SELECT
			day_type_id
		FROM dbo.config_day_type_dow
		WHERE
			day_of_the_week = @day_of_the_week
	);

	IF @service_date_process NOT IN
		(
			SELECT
				service_date
			FROM dbo.service_date
		)

	BEGIN

		INSERT INTO dbo.service_date
		(
			service_date
			,day_of_the_week
			,day_type_id
			,day_type_id_exception -- this will eventually be a manual update
		)

			SELECT
				@service_date_process
				,@day_of_the_week
				,@day_type_id
				,NULL

	END

	DECLARE @day_type_id_exception VARCHAR(255);
	SET @day_type_id_exception =
	(
		SELECT
			day_type_id_exception
		FROM dbo.service_date
		WHERE
			service_date = @service_date_process
	)

	IF @day_type_id_exception IS NOT NULL
		SET @day_type_id = @day_type_id_exception

	--create a temporary table to define the historical service_dates for the service_date being processed. 
	--Historical is defined as 30 days in the past


	DECLARE @historical_service_dates AS TABLE
	(
		historical_service_date DATE NOT NULL
	)

	INSERT INTO @historical_service_dates
	(
		historical_service_date
	)

		SELECT
			service_date
		FROM dbo.service_date
		WHERE
			service_date < @service_date_process
			AND service_date > DATEADD(D,-30,@service_date_process) -- only comparing day being processed with previous 30 days
			AND
				CASE
					WHEN day_type_id_exception IS NULL THEN day_type_id
					WHEN day_type_id_exception IS NOT NULL THEN day_type_id_exception

				END = @day_type_id

	--handle case where day being processed is an exception


	--SCHEDULED

	-- Determine GTFS service_ids and trip_ids for day being processed
	IF OBJECT_ID('dbo.today_trips','U') IS NOT NULL
		DROP TABLE dbo.today_trips

	CREATE TABLE dbo.today_trips
	(
		service_date	DATE			NOT NULL
		,service_id		VARCHAR(255)	NOT NULL
		,route_type		INT				NOT NULL
		,route_id		VARCHAR(255)	NOT NULL
		,direction_id	INT				NOT NULL
		,trip_id		VARCHAR(255)	NOT NULL
	);


	INSERT INTO dbo.today_trips
	(
		service_date
		,service_id
		,route_type
		,route_id
		,direction_id
		,trip_id
	)
		--Add service_ids from GTFS calendar table

		SELECT
			@service_date_process AS service_date
			,t.service_id
			,r.route_type
			,r.route_id
			,t.direction_id
			,t.trip_id

		FROM	gtfs.calendar c
				,gtfs.trips t
				,gtfs.routes r

		WHERE
			t.service_id = c.service_id
			AND t.route_id = r.route_id
			AND (
			r.route_type = 0
			OR r.route_type = 1
			OR r.route_type = 2 --added cr
			OR
					(
						r.route_type = 3
					AND
						r.route_id IN ('712','713')
					)
			)
			AND (
			(@day_of_the_week = 'Monday'
			AND monday = 1)
			OR (@day_of_the_week = 'Tuesday'
			AND tuesday = 1)
			OR (@day_of_the_week = 'Wednesday'
			AND wednesday = 1)
			OR (@day_of_the_week = 'Thursday'
			AND thursday = 1)
			OR (@day_of_the_week = 'Friday'
			AND friday = 1)
			OR (@day_of_the_week = 'Saturday'
			AND saturday = 1)
			OR (@day_of_the_week = 'Sunday'
			AND sunday = 1)
			)
			AND c.start_date <= @service_date_process
			AND c.end_date >= @service_date_process
		UNION
		--Add/remove service_ids from GTFS calendar_dates table
		SELECT
			@service_date_process AS service_date
			,cd.service_id
			,r.route_type
			,r.route_id
			,t.direction_id
			,t.trip_id

		FROM	gtfs.calendar_dates cd
				,gtfs.trips t
				,gtfs.routes r

		WHERE
			t.service_id = cd.service_id
			AND t.route_id = r.route_id
			AND cd.exception_type = 1 -- service added
			AND
			cd.date = @service_date_process
			AND (
			r.route_type = 0 --green line
			OR
			r.route_type = 1 --subway
			OR
			r.route_type = 2 --cr
			OR
			r.route_type = 3 --bus
			)


	DELETE FROM --delete for exception type 2 (removed for the specified date)	
	dbo.today_trips
	WHERE
		service_id
		IN
		(
			SELECT
				cd.service_id
			FROM gtfs.calendar_dates cd
			WHERE
				cd.exception_type = 2 --service deleted
				AND
				cd.date = @service_date_process
		)

	--table for today stop times sec which helps calculate benchmarks, needs to be real table instead of temp for schedule adherence
	IF OBJECT_ID('dbo.today_stop_times_sec','U') IS NOT NULL
		DROP TABLE dbo.today_stop_times_sec

	CREATE TABLE dbo.today_stop_times_sec
	(
		service_date				DATE			NOT NULL
		,route_type					INT				NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,trip_id					VARCHAR(255)	NOT NULL
		,stop_sequence				INT				NOT NULL
		,stop_id					VARCHAR(255)	NOT NULL
		,arrival_time_sec			INT				NOT NULL
		,departure_time_sec			INT				NOT NULL
		,trip_first_stop_sequence	INT  --needed for cr
		,trip_first_stop_id			VARCHAR(255)	NOT NULL --needed for cr
		,trip_start_time			VARCHAR(255)	NOT NULL --needed for cr
		,trip_last_stop_sequence	INT				NOT NULL --needed for cr
		,trip_last_stop_id			VARCHAR(255)	NOT NULL --needed for cr
		,trip_end_time				VARCHAR(255)	NOT NULL --needed for cr
		,stop_order_flag			INT --  needed for cr, 1 is first stop, 2 is mid stop, 3 is last stop
	)
	;

	--INDEXES for today_stop_times_sec

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_route_type
	ON dbo.today_stop_times_sec (route_type);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_route_id
	ON dbo.today_stop_times_sec (route_id);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_direction_id
	ON dbo.today_stop_times_sec (direction_id);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_trip_id
	ON dbo.today_stop_times_sec (trip_id);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_stop_sequence
	ON dbo.today_stop_times_sec (stop_sequence);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_stop_id
	ON dbo.today_stop_times_sec (stop_id);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_arrival_time_sec
	ON dbo.today_stop_times_sec (arrival_time_sec);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_departure_time_sec
	ON dbo.today_stop_times_sec (departure_time_sec);

	CREATE NONCLUSTERED INDEX IX_today_stop_times_sec_index_1
	ON dbo.today_stop_times_sec (route_type,stop_order_flag)
	INCLUDE (service_date,trip_id,stop_sequence,stop_id)

	--temp table for start times to fill in stop_order_flag
	IF OBJECT_ID('tempdb..#webs_trip_start_time_temp','u') IS NOT NULL
		DROP TABLE #webs_trip_start_time_temp

	CREATE TABLE #webs_trip_start_time_temp
	(
		trip_id						VARCHAR(255)	NOT NULL
		,trip_first_stop_sequence	INT				NOT NULL
		,trip_first_stop_id			VARCHAR(255)	NOT NULL
		,trip_start_time			VARCHAR(255)	NOT NULL
	)

	INSERT INTO #webs_trip_start_time_temp
	(
		trip_id
		,trip_first_stop_sequence
		,trip_first_stop_id
		,trip_start_time
	)
		SELECT
			ss_min.trip_id
			,ss_min.trip_first_stop
			,st.stop_id
			,st.departure_time

		FROM	gtfs.stop_times st
				,
				(
					SELECT
						st.trip_id
						,MIN(st.stop_sequence) AS trip_first_stop
					FROM gtfs.stop_times st
					GROUP BY
						st.trip_id
				) ss_min
				,dbo.today_trips ti

		WHERE
			ss_min.trip_id = st.trip_id
			AND ss_min.trip_first_stop = st.stop_sequence
			AND ss_min.trip_id = ti.trip_id
			AND st.trip_id = ti.trip_id

	--temp table for end times to fill in stop_order_flag

	IF OBJECT_ID('tempdb..#webs_trip_end_time_temp','u') IS NOT NULL
		DROP TABLE #webs_trip_end_time_temp

	CREATE TABLE #webs_trip_end_time_temp
	(
		trip_id						VARCHAR(255)	NOT NULL
		,trip_last_stop_sequence	INT				NOT NULL
		,trip_last_stop_id			VARCHAR(255)	NOT NULL
		,trip_end_time				VARCHAR(255)	NOT NULL
	)

	INSERT INTO #webs_trip_end_time_temp
	(
		trip_id
		,trip_last_stop_sequence
		,trip_last_stop_id
		,trip_end_time
	)

		SELECT
			ss_max.trip_id
			,ss_max.trip_last_stop
			,st.stop_id
			,st.arrival_time

		FROM	gtfs.stop_times st
				,
				(
					SELECT
						st.trip_id
						,MAX(st.stop_sequence) AS trip_last_stop
					FROM gtfs.stop_times st
					GROUP BY
						st.trip_id
				) ss_max
				,dbo.today_trips ti

		WHERE
			ss_max.trip_id = st.trip_id
			AND ss_max.trip_last_stop = st.stop_sequence
			AND ss_max.trip_id = ti.trip_id
			AND st.trip_id = ti.trip_id

	IF OBJECT_ID('tempdb..#webs_trip_time_temp','u') IS NOT NULL
		DROP TABLE #webs_trip_time_temp

	CREATE TABLE #webs_trip_time_temp
	(
		trip_id						VARCHAR(255)	NOT NULL
		,trip_first_stop_sequence	INT				NOT NULL
		,trip_first_stop_id			VARCHAR(255)	NOT NULL
		,trip_start_time			VARCHAR(255)	NOT NULL
		,trip_last_stop_sequence	INT				NOT NULL
		,trip_last_stop_id			VARCHAR(255)	NOT NULL
		,trip_end_time				VARCHAR(255)	NOT NULL
	)

	INSERT INTO #webs_trip_time_temp
	(
		trip_id
		,trip_first_stop_sequence
		,trip_first_stop_id
		,trip_start_time
		,trip_last_stop_sequence
		,trip_last_stop_id
		,trip_end_time
	)

		SELECT
			wts.trip_id
			,wts.trip_first_stop_sequence
			,wts.trip_first_stop_id
			,wts.trip_start_time
			,wte.trip_last_stop_sequence
			,wte.trip_last_stop_id
			,wte.trip_end_time
		FROM	#webs_trip_start_time_temp wts
				,#webs_trip_end_time_temp wte
		WHERE
			wts.trip_id = wte.trip_id

	INSERT INTO dbo.today_stop_times_sec
	(
		service_date
		,route_type
		,route_id
		,direction_id
		,trip_id
		,stop_sequence
		,stop_id
		,arrival_time_sec
		,departure_time_sec
		,trip_first_stop_sequence
		,trip_first_stop_id
		,trip_start_time
		,trip_last_stop_sequence
		,trip_last_stop_id
		,trip_end_time
		,stop_order_flag
	)

		SELECT
			ti.service_date
			,ti.route_type
			,ti.route_id
			,ti.direction_id
			,ti.trip_id
			,sta.stop_sequence AS stop_sequence
			,sta.stop_id AS stop_id
			,sta.arrival_time_sec AS arrival_time_sec
			,sta.departure_time_sec AS departure_time_sec
			,wtt.trip_first_stop_sequence
			,wtt.trip_first_stop_id
			,wtt.trip_start_time
			,wtt.trip_last_stop_sequence
			,wtt.trip_last_stop_id
			,wtt.trip_end_time
			,CASE
				WHEN sta.stop_id = wtt.trip_first_stop_id AND
					sta.stop_sequence = wtt.trip_first_stop_sequence THEN 1
				WHEN sta.stop_id = wtt.trip_last_stop_id AND
					sta.stop_sequence = wtt.trip_last_stop_sequence THEN 3
				ELSE 2
			END AS stop_order_flag

		FROM	gtfs.stop_times sta
				,dbo.today_trips ti
				,#webs_trip_time_temp wtt

		WHERE
			ti.trip_id = sta.trip_id
			AND wtt.trip_id = ti.trip_id
			AND wtt.trip_id = sta.trip_id
	;


	--temp table for travel times which helps calculate benchmarks

	IF OBJECT_ID('tempdb..#today_stop_times_travel_time_sec','U') IS NOT NULL
		DROP TABLE #today_stop_times_travel_time_sec

	CREATE TABLE #today_stop_times_travel_time_sec
	(
		service_date				DATE			NOT NULL
		,route_type					INT				NOT NULL
		,route_id					VARCHAR(255)	NOT NULL
		,direction_id				INT				NOT NULL
		,trip_id					VARCHAR(255)	NOT NULL
		,from_stop_sequence			INT				NOT NULL
		,from_stop_id				VARCHAR(255)	NOT NULL
		,from_arrival_time_sec		INT				NOT NULL
		,from_departure_time_sec	INT				NOT NULL
		,to_stop_sequence			INT				NOT NULL
		,to_stop_id					VARCHAR(255)	NOT NULL
		,to_arrival_time_sec		INT				NOT NULL
		,to_departure_time_sec		INT				NOT NULL
		,travel_time_sec			INT				NOT NULL
	)
	;

	INSERT INTO #today_stop_times_travel_time_sec
	(
		service_date
		,route_type
		,route_id
		,direction_id
		,trip_id
		,from_stop_sequence
		,from_stop_id
		,from_arrival_time_sec
		,from_departure_time_sec
		,to_stop_sequence
		,to_stop_id
		,to_arrival_time_sec
		,to_departure_time_sec
		,travel_time_sec
	)

		SELECT
			ti.service_date
			,ti.route_type
			,ti.route_id
			,ti.direction_id
			,ti.trip_id
			,sta.stop_sequence AS from_stop_sequence
			,sta.stop_id AS from_stop_id
			,sta.arrival_time_sec AS from_arrival_time_sec
			,sta.departure_time_sec AS from_departure_time_sec
			,stb.stop_sequence AS to_stop_sequence
			,stb.stop_id AS to_stop_id
			,stb.arrival_time_sec AS to_arrival_time_sec
			,stb.departure_time_sec AS to_departure_time_sec
			,stb.arrival_time_sec - sta.departure_time_sec AS travel_time_sec

		FROM	gtfs.stop_times sta
				,gtfs.stop_times stb
				,dbo.today_trips ti

		WHERE
			ti.trip_id = sta.trip_id
			AND sta.trip_id = stb.trip_id
			AND sta.stop_sequence < stb.stop_sequence
	;

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_route_type
	ON #today_stop_times_travel_time_sec (route_type);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_route_id
	ON #today_stop_times_travel_time_sec (route_id);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_direction_id
	ON #today_stop_times_travel_time_sec (direction_id);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_trip_id
	ON #today_stop_times_travel_time_sec (trip_id);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_from_stop_sequence
	ON #today_stop_times_travel_time_sec (from_stop_sequence);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_from_stop_id
	ON #today_stop_times_travel_time_sec (from_stop_id);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_from_arrival_time_sec
	ON #today_stop_times_travel_time_sec (from_arrival_time_sec);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_from_departure_time_sec
	ON #today_stop_times_travel_time_sec (from_departure_time_sec);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_to_stop_sequence
	ON #today_stop_times_travel_time_sec (to_stop_sequence);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_to_stop_id
	ON #today_stop_times_travel_time_sec (to_stop_id);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_to_arrival_time_sec
	ON #today_stop_times_travel_time_sec (to_arrival_time_sec);

	CREATE NONCLUSTERED INDEX IX_stop_times_travel_time_sec_today_to_departure_time_sec
	ON #today_stop_times_travel_time_sec (to_departure_time_sec);

	--Create today_abcde_time table. This table stores the day being processed scheduled joined events (abcde_time)
	IF OBJECT_ID('tempdb..#today_abcde_time_scheduled','U') IS NOT NULL
		DROP TABLE #today_abcde_time_scheduled
	--
	CREATE TABLE #today_abcde_time_scheduled
	(
		service_date		VARCHAR(255)	NOT NULL
		,abcd_stop_id		VARCHAR(255)	NOT NULL
		,e_stop_id			VARCHAR(255)	NOT NULL
		,ab_stop_sequence	INT				NOT NULL
		,cd_stop_sequence	INT				NOT NULL
		,e_stop_sequence	INT				NOT NULL
		,abcde_direction_id	INT				NOT NULL
		,abcde_route_type	INT				NOT NULL
		,ab_route_id		VARCHAR(255)	NOT NULL
		,cde_route_id		VARCHAR(255)	NOT NULL
		,ab_trip_id			VARCHAR(255)	NOT NULL
		,cde_trip_id		VARCHAR(255)	NOT NULL
		,a_time_sec			INT				NOT NULL
		,b_time_sec			INT				NOT NULL
		,c_time_sec			INT				NOT NULL
		,d_time_sec			INT				NOT NULL
		,e_time_sec			INT				NOT NULL
	)

	INSERT INTO #today_abcde_time_scheduled
	(
		service_date
		,abcd_stop_id
		,e_stop_id
		,ab_stop_sequence
		,cd_stop_sequence
		,e_stop_sequence
		,abcde_direction_id
		,abcde_route_type
		,ab_route_id
		,cde_route_id
		,ab_trip_id
		,cde_trip_id
		,a_time_sec
		,b_time_sec
		,c_time_sec
		,d_time_sec
		,e_time_sec
	)

		SELECT
			service_date
			,abcd_stop_id
			,e_stop_id
			,ab_stop_sequence
			,cd_stop_sequence
			,e_stop_sequence
			,abcde_direction_id
			,abcde_route_type
			,ab_route_id
			,cde_route_id
			,ab_trip_id
			,cde_trip_id
			,a_time_sec
			,b_time_sec
			,c_time_sec
			,d_time_sec
			,e_time_sec
		FROM
		(

			SELECT

				cde.service_date

				,cde.from_stop_id AS abcd_stop_id
				,cde.to_stop_id AS e_stop_id

				,ab.from_stop_sequence AS ab_stop_sequence
				,cde.from_stop_sequence AS cd_stop_sequence
				,cde.to_stop_sequence AS e_stop_sequence

				,cde.direction_id AS abcde_direction_id
				,ab.route_type AS abcde_route_type
				,ab.route_id AS ab_route_id
				,cde.route_id AS cde_route_id
				,ab.trip_id AS ab_trip_id
				,cde.trip_id AS cde_trip_id

				,ab.from_arrival_time_sec AS a_time_sec
				,ab.from_departure_time_sec AS b_time_sec
				,cde.from_arrival_time_sec AS c_time_sec
				,cde.from_departure_time_sec AS d_time_sec
				,cde.to_arrival_time_sec AS e_time_sec

				,ROW_NUMBER() OVER (
				PARTITION BY
				cde.trip_id
				,cde.from_stop_sequence
				,cde.to_stop_sequence
				ORDER BY ab.from_departure_time_sec DESC,ab.trip_id DESC) AS rn

			FROM #today_stop_times_travel_time_sec cde -- cde is travel from b/c to d

			JOIN #today_stop_times_travel_time_sec ab -- b is the departure of the last relevant train at the "From" stop. Relevant means that the train takes people from abcd ("From" stop) to e ("To" stop)
				ON
					(
					cde.from_stop_id = ab.from_stop_id
					AND cde.to_stop_id = ab.to_stop_id
					AND cde.direction_id = ab.direction_id
					AND ab.route_type = cde.route_type
					AND cde.trip_id <> ab.trip_id
					AND (cde.from_arrival_time_sec > ab.from_departure_time_sec
					OR (cde.from_arrival_time_sec = ab.from_departure_time_sec
					AND cde.trip_id > ab.trip_id)) --to address green line trains at same time issue
					AND
					cde.from_arrival_time_sec - ab.from_departure_time_sec <= 3600 --the departure from the last relevant train within the past 3 hours
					)
		) t

		WHERE
			t.rn = 1

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_ab_route_id ON #today_abcde_time_scheduled (ab_route_id);

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_cde_route_id ON #today_abcde_time_scheduled (cde_route_id);

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_abcde_route_type ON #today_abcde_time_scheduled (abcde_route_type);

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_service_date ON #today_abcde_time_scheduled (service_date);

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_abc_stop_id ON #today_abcde_time_scheduled (abcd_stop_id);

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_d_stop_id ON #today_abcde_time_scheduled (e_stop_id);

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_abcde_direction_id ON #today_abcde_time_scheduled (abcde_direction_id);

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_b_d_time_sec ON #today_abcde_time_scheduled (abcd_stop_id,e_stop_id,abcde_direction_id,b_time_sec) INCLUDE (d_time_sec)

	CREATE NONCLUSTERED INDEX IX_today_abcde_time_scheduled_d_e_time_sec ON #today_abcde_time_scheduled (abcd_stop_id,e_stop_id,abcde_direction_id,d_time_sec) INCLUDE (e_time_sec)




	-- TRAVEL TIME

	--Create a table to calculate and store benchmark average travel time for each O-D pair, 
	--for each line/branch in each direction for each configured time slice for the service day for subway and light rail

	IF OBJECT_ID('dbo.today_travel_time_benchmark','u') IS NOT NULL
		DROP TABLE dbo.today_travel_time_benchmark

	CREATE TABLE dbo.today_travel_time_benchmark
	(
		service_date						VARCHAR(255)	NOT NULL
		,from_stop_id						VARCHAR(255)	NOT NULL
		,to_stop_id							VARCHAR(255)	NOT NULL
		,route_type							INT				NOT NULL
		,route_id							VARCHAR(255)	NOT NULL
		,direction_id						INT				NOT NULL
		,time_slice_id						VARCHAR(255)	NOT NULL
		,historical_average_travel_time_sec	INT				NULL
		,historical_median_travel_time_sec	INT				NULL
		,scheduled_average_travel_time_sec	INT				NULL
		,scheduled_median_travel_time_sec	INT				NULL
	)

	CREATE NONCLUSTERED INDEX today_travel_time_benchmark_index_1
	ON dbo.today_travel_time_benchmark (from_stop_id,to_stop_id,direction_id)
	INCLUDE (route_type,time_slice_id,scheduled_average_travel_time_sec)

	CREATE NONCLUSTERED INDEX IX_today_travel_time_benchmark_2
	ON dbo.today_travel_time_benchmark (from_stop_id,to_stop_id,route_id,direction_id)
	INCLUDE (time_slice_id,scheduled_average_travel_time_sec)

	INSERT INTO dbo.today_travel_time_benchmark
	(
		service_date
		,from_stop_id
		,to_stop_id
		,route_type
		,route_id
		,direction_id
		,time_slice_id
		,historical_average_travel_time_sec
		,historical_median_travel_time_sec
		,scheduled_average_travel_time_sec
		,scheduled_median_travel_time_sec
	)

		SELECT
			@service_date_process
			,ISNULL(his.from_stop_id,sch.from_stop_id) AS from_stop_id
			,ISNULL(his.to_stop_id,sch.to_stop_id) AS to_stop_id
			,ISNULL(his.route_type,sch.route_type) AS route_type
			,ISNULL(his.route_id,sch.route_id) AS route_id
			,ISNULL(his.direction_id,sch.direction_id) AS direction_id
			,ISNULL(his.time_slice_id,sch.time_slice_id) AS time_slice_id
			,his.historical_average_travel_time_sec
			,his.historical_median_travel_time_sec
			,sch.scheduled_average_travel_time_sec
			,sch.scheduled_median_travel_time_sec

		FROM
		(
			SELECT
				from_stop_id
				,to_stop_id
				,direction_id
				,route_type
				,route_id
				,time_slice_id
				,AVG(travel_time_sec) AS historical_average_travel_time_sec
				,MAX(median_travel_time_sec) AS historical_median_travel_time_sec
			FROM
			(
				SELECT
					from_stop_id
					,to_stop_id
					,route_type
					,route_id
					,direction_id
					,time_slice_id
					,travel_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY travel_time_sec) OVER (PARTITION BY
					from_stop_id
					,to_stop_id
					,direction_id
					,time_slice_id
					) AS median_travel_time_sec

				FROM	dbo.historical_travel_time_disaggregate att
						,dbo.config_time_slice
						,@historical_service_dates hsd
				WHERE
					att.service_date = hsd.historical_service_date
					AND att.end_time_sec < time_slice_end_sec
					AND att.end_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				from_stop_id
				,to_stop_id
				,route_type
				,route_id
				,direction_id
				,time_slice_id
		) his

		RIGHT JOIN
		(
			SELECT
				from_stop_id
				,to_stop_id
				,route_type
				,route_id
				,direction_id
				,time_slice_id
				,AVG(travel_time_sec) AS scheduled_average_travel_time_sec
				,MAX(median_travel_time_sec) AS scheduled_median_travel_time_sec
			FROM
			(
				SELECT
					from_stop_id AS from_stop_id
					,to_stop_id AS to_stop_id
					,direction_id AS direction_id
					,route_type AS route_type
					,route_id
					,time_slice_id
					,(to_arrival_time_sec - from_departure_time_sec) AS travel_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (to_arrival_time_sec - from_departure_time_sec)) OVER (PARTITION BY
					from_stop_id
					,to_stop_id
					,direction_id
					,time_slice_id
					) AS median_travel_time_sec

				FROM	#today_stop_times_travel_time_sec att
						,dbo.config_time_slice
				WHERE
					to_arrival_time_sec < time_slice_end_sec
					AND to_arrival_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				from_stop_id
				,to_stop_id
				,route_type
				,route_id
				,direction_id
				,time_slice_id
		) sch

			ON 
				(
				his.from_stop_id = sch.from_stop_id
				AND his.to_stop_id = sch.to_stop_id
				AND his.route_id = sch.route_id --added in because of green line
				AND
				his.direction_id = sch.direction_id
				AND his.time_slice_id = sch.time_slice_id
				)


	IF OBJECT_ID('tempdb..#today_stop_times_travel_time_sec','U') IS NOT NULL
		DROP TABLE #today_stop_times_travel_time_sec

	-- Create table to store travel time threshold for day being processed

	IF OBJECT_ID('dbo.today_travel_time_threshold','U') IS NOT NULL
		DROP TABLE dbo.today_travel_time_threshold

	CREATE TABLE dbo.today_travel_time_threshold
	(
		service_date									VARCHAR(255)	NOT NULL
		,from_stop_id									VARCHAR(255)	NOT NULL
		,to_stop_id										VARCHAR(255)	NOT NULL
		,route_type										INT				NOT NULL
		,route_id										VARCHAR(255)	NOT NULL
		,direction_id									INT				NOT NULL
		,time_slice_id									VARCHAR(255)	NOT NULL
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_historical_median_travel_time_sec	INT				NULL
		,threshold_scheduled_median_travel_time_sec		INT				NULL
		,threshold_historical_average_travel_time_sec	INT				NULL 
		,threshold_scheduled_average_travel_time_sec	INT				NULL 
	)

	CREATE NONCLUSTERED INDEX IX_today_travel_time_threshold_index_1
	ON dbo.today_travel_time_threshold (service_date,from_stop_id,to_stop_id,route_type,direction_id)
	INCLUDE (time_slice_id,threshold_id,threshold_historical_median_travel_time_sec,threshold_scheduled_median_travel_time_sec,threshold_historical_average_travel_time_sec,threshold_scheduled_average_travel_time_sec)


	INSERT INTO dbo.today_travel_time_threshold
	(
		service_date
		,from_stop_id
		,to_stop_id
		,route_type
		,route_id
		,direction_id
		,time_slice_id
		,threshold_id
		,threshold_historical_median_travel_time_sec
		,threshold_scheduled_median_travel_time_sec
		,threshold_historical_average_travel_time_sec 
		,threshold_scheduled_average_travel_time_sec 
	)
		SELECT
			att.service_date
			,att.from_stop_id
			,att.to_stop_id
			,att.route_type
			,att.route_id
			,att.direction_id
			,att.time_slice_id
			,th.threshold_id
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(att.historical_median_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(att.historical_median_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(att.historical_median_travel_time_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_historical_median_travel_time_sec
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(att.scheduled_median_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(att.scheduled_median_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(att.scheduled_median_travel_time_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_scheduled_median_travel_time_sec
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(att.historical_average_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(att.historical_average_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(att.historical_average_travel_time_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_historical_average_travel_time_sec 
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(att.scheduled_average_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(att.scheduled_average_travel_time_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(att.scheduled_average_travel_time_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_scheduled_average_travel_time_sec 

		FROM	dbo.today_travel_time_benchmark att
				,dbo.config_threshold th
				,dbo.config_threshold_calculation thc
				,dbo.config_mode_threshold mt

		WHERE
			th.threshold_id = thc.threshold_id
			AND mt.threshold_id = th.threshold_id
			AND mt.threshold_id = thc.threshold_id
			AND th.threshold_type = 'travel_time'
			AND att.route_type = mt.route_type

		GROUP BY
			att.service_date
			,att.route_type
			,att.route_id
			,att.from_stop_id
			,att.to_stop_id
			,att.direction_id
			,att.time_slice_id
			,th.threshold_id
			,th.min_max_equal


	--WAIT TIME

	--Create a table to calculate and store benchmark headway for trains serving an o-d pair, 
	--in each direction, for each time slice, for every service day for subway and light rail


	IF OBJECT_ID('dbo.today_headway_time_od_benchmark','u') IS NOT NULL
		DROP TABLE dbo.today_headway_time_od_benchmark

	CREATE TABLE dbo.today_headway_time_od_benchmark
	(
		service_date					VARCHAR(255)	NOT NULL
		,stop_id						VARCHAR(255)	NOT NULL
		,to_stop_id						VARCHAR(255)	NOT NULL
		,route_type						INT				NOT NULL
		,direction_id					INT				NOT NULL
		,time_slice_id					VARCHAR(255)	NOT NULL
		,historical_average_headway_sec	INT				NULL
		,historical_median_headway_sec	INT				NULL
		,scheduled_average_headway_sec	INT				NULL
		,scheduled_median_headway_sec	INT				NULL
	)

	CREATE NONCLUSTERED INDEX IX_today_headway_time_od_benchmark_index_1
	ON dbo.today_headway_time_od_benchmark (stop_id,to_stop_id,direction_id)
	INCLUDE (time_slice_id,scheduled_average_headway_sec)

	CREATE NONCLUSTERED INDEX IX_today_headway_time_od_benchmark_route_type
	ON dbo.today_headway_time_od_benchmark (route_type)
	INCLUDE (service_date,stop_id,to_stop_id,direction_id,time_slice_id,historical_average_headway_sec,historical_median_headway_sec,scheduled_average_headway_sec,scheduled_median_headway_sec)


	INSERT INTO dbo.today_headway_time_od_benchmark
	(
		service_date
		,stop_id
		,to_stop_id
		,route_type
		,direction_id
		,time_slice_id
		,historical_average_headway_sec
		,historical_median_headway_sec
		,scheduled_average_headway_sec
		,scheduled_median_headway_sec
	)

		SELECT
			@service_date_process
			,ISNULL(his.stop_id,sch.stop_id) AS from_stop_id
			,ISNULL(his.to_stop_id,sch.to_stop_id) AS to_stop_id
			,ISNULL(his.route_type,sch.route_type) AS route_type
			,ISNULL(his.direction_id,sch.direction_id) AS direction_id
			,ISNULL(his.time_slice_id,sch.time_slice_id) AS time_slice_id
			,his.historical_average_headway_time_sec
			,his.historical_median_headway_time_sec
			,sch.scheduled_average_headway_time_sec
			,sch.scheduled_median_headway_time_sec

		FROM
		(
			SELECT
				stop_id
				,to_stop_id
				,route_type
				,direction_id
				,time_slice_id
				,AVG(headway_time_sec) AS historical_average_headway_time_sec
				,MAX(median_headway_time_sec) AS historical_median_headway_time_sec
			FROM
			(
				SELECT
					stop_id
					,to_stop_id
					,route_type
					,direction_id
					,time_slice_id
					,headway_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY headway_time_sec) OVER (PARTITION BY
					stop_id
					,to_stop_id
					,direction_id
					,time_slice_id
					) AS median_headway_time_sec

				FROM	dbo.historical_headway_time_od_disaggregate aht
						,dbo.config_time_slice
						,@historical_service_dates hsd
				WHERE
					aht.service_date = hsd.historical_service_date
					AND aht.end_time_sec < time_slice_end_sec
					AND aht.end_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				stop_id
				,to_stop_id
				,route_type
				,direction_id
				,time_slice_id
		) his

		RIGHT JOIN
		(
			SELECT
				stop_id
				,to_stop_id
				,route_type
				,direction_id
				,time_slice_id
				,AVG(headway_time_sec) AS scheduled_average_headway_time_sec
				,MAX(median_headway_time_sec) AS scheduled_median_headway_time_sec
			FROM
			(
				SELECT
					abcd_stop_id AS stop_id
					,e_stop_id AS to_stop_id
					,abcde_route_type AS route_type
					,abcde_direction_id AS direction_id
					,time_slice_id
					,(d_time_sec - b_time_sec) AS headway_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (d_time_sec - b_time_sec)) OVER (PARTITION BY
					abcd_stop_id
					,e_stop_id
					,abcde_direction_id
					,time_slice_id
					) AS median_headway_time_sec

				FROM	#today_abcde_time_scheduled att
						,dbo.config_time_slice
				WHERE
					d_time_sec < time_slice_end_sec
					AND d_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				stop_id
				,to_stop_id
				,route_type
				,direction_id
				,time_slice_id
		) sch

			ON
				(
				his.stop_id = sch.stop_id
				AND his.to_stop_id = sch.to_stop_id
				AND his.direction_id = sch.direction_id
				AND his.time_slice_id = sch.time_slice_id
				)


	--Create table to store wait time threshold for today

	IF OBJECT_ID('dbo.today_wait_time_od_threshold','U') IS NOT NULL
		DROP TABLE dbo.today_wait_time_od_threshold

	CREATE TABLE dbo.today_wait_time_od_threshold
	(
		service_date								VARCHAR(255)	NOT NULL
		,stop_id									VARCHAR(255)	NOT NULL
		,to_stop_id									VARCHAR(255)	NOT NULL
		,route_type									INT				NOT NULL
		,direction_id								INT				NOT NULL
		,time_slice_id								VARCHAR(255)	NOT NULL
		,threshold_id								VARCHAR(255)	NOT NULL
		,threshold_historical_median_wait_time_sec	INT				NULL
		,threshold_scheduled_median_wait_time_sec	INT				NULL
		,threshold_historical_average_wait_time_sec	INT				NULL 
		,threshold_scheduled_average_wait_time_sec	INT				NULL 
	)

	CREATE NONCLUSTERED INDEX IX_today_wait_time_od_threshold_index_1
	ON dbo.today_wait_time_od_threshold (service_date,stop_id,to_stop_id,direction_id,route_type,threshold_historical_median_wait_time_sec)
	INCLUDE (time_slice_id,threshold_id,threshold_scheduled_median_wait_time_sec,threshold_historical_average_wait_time_sec,threshold_scheduled_average_wait_time_sec)


	INSERT INTO dbo.today_wait_time_od_threshold
	(
		service_date
		,stop_id
		,to_stop_id
		,route_type
		,direction_id
		,time_slice_id
		,threshold_id
		,threshold_historical_median_wait_time_sec
		,threshold_scheduled_median_wait_time_sec
		,threshold_historical_average_wait_time_sec 
		,threshold_scheduled_average_wait_time_sec 
	)

		SELECT
			aht.service_date
			,aht.stop_id
			,aht.to_stop_id
			,aht.route_type
			,aht.direction_id
			,aht.time_slice_id
			,th.threshold_id
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(aht.historical_median_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(aht.historical_median_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(aht.historical_median_headway_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_historical_median_wait_time_sec
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(aht.scheduled_median_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(aht.scheduled_median_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(aht.scheduled_median_headway_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_scheduled_median_wait_time_sec
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(aht.historical_average_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(aht.historical_average_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(aht.historical_average_headway_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_historical_average_wait_time_sec 
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(aht.scheduled_average_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(aht.scheduled_average_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(aht.scheduled_average_headway_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_scheduled_average_wait_time_sec 

		FROM	dbo.today_headway_time_od_benchmark aht
				,dbo.config_threshold th
				,dbo.config_threshold_calculation thc
				,dbo.config_mode_threshold mt

		WHERE
			th.threshold_id = thc.threshold_id
			AND mt.threshold_id = th.threshold_id
			AND mt.threshold_id = thc.threshold_id
			AND th.threshold_type = 'wait_time_headway_based'
			AND aht.route_type = mt.route_type

		GROUP BY
			aht.service_date
			,aht.stop_id
			,aht.to_stop_id
			,aht.route_type
			,aht.direction_id
			,aht.time_slice_id
			,th.threshold_id
			,th.min_max_equal

	--Create a table to calculate and store benchmark headway for each stop for all trains serving that stop, 
	--in each direction, for each time slice, for every service day for subway and light rail

	IF OBJECT_ID('dbo.today_headway_time_sr_all_benchmark','u') IS NOT NULL
		DROP TABLE dbo.today_headway_time_sr_all_benchmark

	CREATE TABLE dbo.today_headway_time_sr_all_benchmark
	(
		service_date					VARCHAR(255)	NOT NULL
		,stop_id						VARCHAR(255)	NOT NULL
		,route_type						INT				NOT NULL
		,direction_id					INT				NOT NULL
		,time_slice_id					VARCHAR(255)	NOT NULL
		,historical_average_headway_sec	INT				NULL
		,historical_median_headway_sec	INT				NULL
		,scheduled_average_headway_sec	INT				NULL
		,scheduled_median_headway_sec	INT				NULL
	)

	CREATE NONCLUSTERED INDEX IX_today_headway_time_sr_all_benchmark_index_1
	ON dbo.today_headway_time_sr_all_benchmark (stop_id,direction_id)
	INCLUDE (time_slice_id,scheduled_average_headway_sec)

	CREATE NONCLUSTERED INDEX IX_today_headway_time_sr_all_benchmark_index_2
	ON dbo.today_headway_time_sr_all_benchmark (stop_id,route_type,direction_id)
	INCLUDE (time_slice_id,scheduled_average_headway_sec)

	INSERT INTO dbo.today_headway_time_sr_all_benchmark
	(
		service_date
		,stop_id
		,route_type
		,direction_id
		,time_slice_id
		,historical_average_headway_sec
		,historical_median_headway_sec
		,scheduled_average_headway_sec
		,scheduled_median_headway_sec
	)

		SELECT
			@service_date_process
			,ISNULL(his.stop_id,sch.stop_id) AS stop_id
			,ISNULL(his.route_type,sch.route_type) AS route_type
			,ISNULL(his.direction_id,sch.direction_id) AS direction_id
			,ISNULL(his.time_slice_id,sch.time_slice_id) AS time_slice_id
			,his.historical_average_headway_time_sec
			,his.historical_median_headway_time_sec
			,sch.scheduled_average_headway_time_sec
			,sch.scheduled_median_headway_time_sec

		FROM
		(
			SELECT
				stop_id
				,route_type
				,direction_id
				,time_slice_id
				,AVG(headway_time_sec) AS historical_average_headway_time_sec
				,MAX(median_headway_time_sec) AS historical_median_headway_time_sec
			FROM
			(
				SELECT
					stop_id
					,route_type
					,direction_id
					,time_slice_id
					,headway_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY headway_time_sec) OVER (PARTITION BY
					stop_id
					,direction_id
					,time_slice_id
					) AS median_headway_time_sec

				FROM	dbo.historical_headway_time_sr_all_disaggregate aht
						,dbo.config_time_slice
						,@historical_service_dates hsd
				WHERE
					aht.service_date = hsd.historical_service_date
					AND aht.end_time_sec < time_slice_end_sec
					AND aht.end_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				stop_id
				,direction_id
				,route_type
				,time_slice_id
		) his

		RIGHT JOIN
		(
			SELECT
				stop_id
				,route_type
				,direction_id
				,time_slice_id
				,AVG(headway_time_sec) AS scheduled_average_headway_time_sec
				,MAX(median_headway_time_sec) AS scheduled_median_headway_time_sec
			FROM
			(
				SELECT
					stop_id AS stop_id
					,route_type AS route_type
					,direction_id AS direction_id
					,time_slice_id
					,(d_time_sec - b_time_sec) AS headway_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (d_time_sec - b_time_sec)) OVER (PARTITION BY
					stop_id
					,stop_id
					,direction_id
					,time_slice_id
					) AS median_headway_time_sec

				FROM
				(
					SELECT
						stop_id
						,route_type
						,direction_id
						,b_time_sec
						,d_time_sec
						,headway_time_sec
					FROM
					(
						SELECT
							y.stop_id AS stop_id
							,y.route_type AS route_type
							,y.direction_id AS direction_id
							,x.departure_time_sec AS b_time_sec
							,y.departure_time_sec AS d_time_sec
							,y.departure_time_sec - x.departure_time_sec AS headway_time_sec
							,ROW_NUMBER() OVER (PARTITION BY y.stop_id,y.trip_id
							ORDER BY x.departure_time_sec DESC,x.trip_id DESC) AS rn

						FROM	today_stop_times_sec y
								,today_stop_times_sec x


						WHERE
							y.stop_id = x.stop_id
							AND y.trip_id <> x.trip_id
							AND y.direction_id = x.direction_id
							AND y.route_type = x.route_type
							AND (y.departure_time_sec > x.departure_time_sec
							OR (y.departure_time_sec = x.departure_time_sec
							AND y.trip_id > x.trip_id)) --changed to address green line trains at same time issue
							AND
							y.departure_time_sec - x.departure_time_sec <= 1800
					) temp
					WHERE
						rn = 1
				) att
				,dbo.config_time_slice
				WHERE
					d_time_sec < time_slice_end_sec
					AND d_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				stop_id
				,route_type
				,direction_id
				,time_slice_id
		) sch

			ON
				(
				his.stop_id = sch.stop_id
				AND his.direction_id = sch.direction_id
				AND his.time_slice_id = sch.time_slice_id
				)

	----create table to store thresholds for headway trip metrics
	IF OBJECT_ID('dbo.today_headway_time_threshold','U') IS NOT NULL
		DROP TABLE dbo.today_headway_time_threshold

	CREATE TABLE dbo.today_headway_time_threshold
	(
		service_date									VARCHAR(255)	NOT NULL
		,stop_id										VARCHAR(255)	NOT NULL
		,route_type										INT				NOT NULL
		,direction_id									INT				NOT NULL
		,time_slice_id									VARCHAR(255)	NOT NULL
		,threshold_id									VARCHAR(255)	NOT NULL
		,threshold_scheduled_median_headway_time_sec	INT				NULL
		,threshold_scheduled_average_headway_time_sec	INT				NULL 
	)
	INSERT INTO dbo.today_headway_time_threshold
	(
		service_date
		,stop_id
		,route_type
		,direction_id
		,time_slice_id
		,threshold_id
		,threshold_scheduled_median_headway_time_sec
		,threshold_scheduled_average_headway_time_sec
	)

		SELECT
			aht.service_date
			,aht.stop_id
			,aht.route_type
			,aht.direction_id
			,aht.time_slice_id
			,th.threshold_id
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(aht.scheduled_median_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(aht.scheduled_median_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(aht.scheduled_median_headway_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_scheduled_median_headway_time_sec
			,CASE
				WHEN th.min_max_equal = 'min' THEN MIN(aht.scheduled_average_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'max' THEN MAX(aht.scheduled_average_headway_sec * thc.multiply_by + thc.add_to)
				WHEN th.min_max_equal = 'equal' THEN AVG(aht.scheduled_average_headway_sec * thc.multiply_by + thc.add_to)
				ELSE 0
			END AS threshold_scheduled_average_headway_time_sec 

		FROM	dbo.today_headway_time_sr_all_benchmark aht
				,dbo.config_threshold th
				,dbo.config_threshold_calculation thc
				,dbo.config_mode_threshold mt

		WHERE
			th.threshold_id = thc.threshold_id
			AND mt.threshold_id = th.threshold_id
			AND mt.threshold_id = thc.threshold_id
			AND th.threshold_type = 'trip_headway_based'
			AND aht.route_type = mt.route_type

		GROUP BY
			aht.service_date
			,aht.stop_id
			,aht.route_type
			,aht.direction_id
			,aht.time_slice_id
			,th.threshold_id
			,th.min_max_equal

	--Create a table to calculate and store benchmark headway for each stop for all trains of the same route serving that stop, 
	--in each direction, for each time slice, for every service day for subway and light rail

	IF OBJECT_ID('dbo.today_headway_time_sr_same_benchmark','u') IS NOT NULL
		DROP TABLE dbo.today_headway_time_sr_same_benchmark

	CREATE TABLE dbo.today_headway_time_sr_same_benchmark
	(
		service_date					VARCHAR(255)	NOT NULL
		,stop_id						VARCHAR(255)	NOT NULL
		,route_type						INT
		,route_id						VARCHAR(255)	NOT NULL 
		,direction_id					INT				NOT NULL
		,time_slice_id					VARCHAR(255)	NOT NULL
		,historical_average_headway_sec	INT				NULL
		,historical_median_headway_sec	INT				NULL
		,scheduled_average_headway_sec	INT				NULL
		,scheduled_median_headway_sec	INT				NULL
	)

	CREATE NONCLUSTERED INDEX IX_today_headway_time_sr_same_benchmark_index_1
	ON dbo.today_headway_time_sr_same_benchmark (stop_id,route_id,direction_id)
	INCLUDE (time_slice_id,scheduled_average_headway_sec)

	INSERT INTO dbo.today_headway_time_sr_same_benchmark
	(
		service_date
		,stop_id
		,route_type
		,route_id
		,direction_id
		,time_slice_id
		,historical_average_headway_sec
		,historical_median_headway_sec
		,scheduled_average_headway_sec
		,scheduled_median_headway_sec
	)

		SELECT
			@service_date_process
			,ISNULL(his.stop_id,sch.stop_id) AS stop_id
			,ISNULL(his.route_type,sch.route_type) AS route_type 
			,ISNULL(his.route_id,sch.route_id) AS route_id 
			,ISNULL(his.direction_id,sch.direction_id) AS direction_id
			,ISNULL(his.time_slice_id,sch.time_slice_id) AS time_slice_id
			,his.historical_average_headway_time_sec
			,his.historical_median_headway_time_sec
			,sch.scheduled_average_headway_time_sec
			,sch.scheduled_median_headway_time_sec

		FROM
		(
			SELECT
				stop_id
				,route_type
				,route_id
				,direction_id
				,time_slice_id
				,AVG(headway_time_sec) AS historical_average_headway_time_sec
				,MAX(median_headway_time_sec) AS historical_median_headway_time_sec
			FROM
			(
				SELECT
					stop_id
					,route_type
					,route_id 
					,direction_id
					,time_slice_id
					,headway_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY headway_time_sec) OVER (PARTITION BY
					stop_id
					,direction_id
					,time_slice_id
					) AS median_headway_time_sec

				FROM	dbo.historical_headway_time_sr_all_disaggregate aht
						,dbo.config_time_slice
						,@historical_service_dates hsd
				WHERE
					aht.service_date = hsd.historical_service_date
					AND aht.end_time_sec < time_slice_end_sec
					AND aht.end_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				stop_id
				,route_type
				,route_id 
				,direction_id
				,time_slice_id
		) his

		RIGHT JOIN
		(
			SELECT
				stop_id
				,route_type
				,route_id 
				,direction_id
				,time_slice_id
				,AVG(headway_time_sec) AS scheduled_average_headway_time_sec
				,MAX(median_headway_time_sec) AS scheduled_median_headway_time_sec
			FROM
			(
				SELECT
					stop_id AS stop_id
					,route_type AS route_type
					,route_id AS route_id
					,direction_id AS direction_id
					,time_slice_id
					,(d_time_sec - b_time_sec) AS headway_time_sec
					,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (d_time_sec - b_time_sec)) OVER (PARTITION BY
					stop_id
					,stop_id
					,direction_id
					,time_slice_id
					) AS median_headway_time_sec

				FROM
				(
					SELECT
						stop_id
						,route_type
						,route_id
						,direction_id
						,b_time_sec
						,d_time_sec
						,headway_time_sec
					FROM
					(
						SELECT
							y.stop_id AS stop_id
							,y.route_type AS route_type
							,y.route_id AS route_id 
							,y.direction_id AS direction_id
							,x.departure_time_sec AS b_time_sec
							,y.departure_time_sec AS d_time_sec
							,y.departure_time_sec - x.departure_time_sec AS headway_time_sec
							,ROW_NUMBER() OVER (PARTITION BY y.stop_id,y.trip_id
							ORDER BY x.departure_time_sec DESC,x.trip_id DESC) AS rn

						FROM	today_stop_times_sec y
								,today_stop_times_sec x


						WHERE
							y.stop_id = x.stop_id
							AND y.trip_id <> x.trip_id
							AND y.direction_id = x.direction_id
							AND y.route_id = x.route_id
							AND (y.departure_time_sec > x.departure_time_sec
							OR (y.departure_time_sec = x.departure_time_sec
							AND y.trip_id > x.trip_id)) --to address green line trains at same time issue
							AND
							y.departure_time_sec - x.departure_time_sec <= 1800
					) temp
					WHERE
						rn = 1
				) att
				,dbo.config_time_slice
				WHERE
					d_time_sec < time_slice_end_sec
					AND d_time_sec >= time_slice_start_sec
			) t
			GROUP BY
				stop_id
				,route_type
				,route_id
				,direction_id
				,time_slice_id
		) sch

			ON
				(
				his.route_id = sch.route_id 
				AND
				his.stop_id = sch.stop_id
				AND his.direction_id = sch.direction_id
				AND his.time_slice_id = sch.time_slice_id
				)


	IF OBJECT_ID('tempdb..#today_abcde_time_scheduled','U') IS NOT NULL
		DROP TABLE #today_abcde_time_scheduled

	IF OBJECT_ID('tempdb..#webs_trip_start_time_temp','u') IS NOT NULL
		DROP TABLE #webs_trip_start_time_temp

	IF OBJECT_ID('tempdb..#webs_trip_end_time_temp','u') IS NOT NULL
		DROP TABLE #webs_trip_end_time_temp

	IF OBJECT_ID('tempdb..#webs_trip_time_temp','u') IS NOT NULL
		DROP TABLE #webs_trip_time_temp

	IF OBJECT_ID('tempdb..#today_stop_times_travel_time_sec','U') IS NOT NULL
		DROP TABLE #today_stop_times_travel_time_sec




END;

GO