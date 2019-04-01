# TRANSIT-performance

The TRANSIT-performance system records and measures transit service performance in real-time from two perspectives: the quality of service provided by the agency including travel time, headway, dwell time, on-time performance and schedule adherence; and the quality of service experienced by customers including passenger weighted wait times and travel times. The system also measures the accuracy of real-time predictions. 

The system primarily uses GTFS and GTFS-realtime as data inputs. Information is recorded in real-time for all subway, light rail, commuter rail routes, and a number of bus routes, for all directions, trips, and stops in the transit system, allowing analysis of a near 100% sample of data. The system will be updated to include bus data for all routes at a later time.

The outputs of the system are web services API calls and data tables to allow internal users and registered developers to access historical and real-time performance information that can be segmented by day and time period as well as by route, direction, or stop. The repository provides all the necessary source code to collect and process data in the database, and fetch data for API calls, but does not include the source code for creating APIs. 

This document provides a brief guide for setting up the system. For detailed documentation about the TRANSIT-performance system, see [here](https://docs.google.com/document/d/19GcQ0UZmstbKBPDDD1G9uBnoqmIkWwvCgqfLaxWxfz8/edit#).

## Input Requirements

The TRANSIT-performance system primarily uses GTFS schedule data and the GTFS-realtime Vehicle Positions and Trip Updates feeds as data inputs. Configuration files are also used to set up benchmarks and thresholds against which to measure performance. Passenger weighted metrics also require an origin-destination matrix for headway-based services and passenger information for trips and stops for schedule-based services. 

### GTFS
GTFS is a standardized format for public transit agencies to publish their schedule information. GTFS files are used to determine the scheduled services for the system and compare how the system performs compared to the schedule. The system requires an agency’s GTFS dataset that complies with the GTFS specification and is available in a stable web accessible location.
For details about GTFS, please refer to the [GTFS specification](https://github.com/google/transit/tree/master/gtfs). 

### GTFS-realtime
The GTFS-realtime specification is an extension to GTFS that allows agencies to provide real-time updates about their scheduled service referenced in the GTFS feed. The system requires an agency’s GTFS-realtime feeds that comply with the GTFS-realtime specification and are available as .pb files in a stable web accessible location.

The GTFS-realtime Vehicle Positions feed is used to determine actual arrival and departure events for all trips at all stops in the system. The Vehicle Positions feed must have all data elements required as part of the GTFS-realtime specification and the following optional fields:

* trip_id
* route_id
* schedule_relationship
* id of vehicle
* latitude/longitude of vehicle
* current\_stop\_sequence
* stop_id
* current_status
* timestamp

The GTFS-realtime Trip Updates feed is used to determine predicted arrival and departure events for all trips at all stops in the system. The latest available predicted arrival and departure events are used to estimate actual arrival and departure events for cases where the Vehicle Positions feed does not provide this information. The Trip Updates feed must have all data elements required as part of the GTFS-realtime specification and the following optional fields:

* trip_id
* direction_id
* route_id
* schedule_relationship
* id of vehicle
* stop_sequence
* stop_id
* arrival_time
* departure_time
* timestamp

For details about GTFS-realtime, please refer to the [GTFS-realtime specification](https://github.com/google/transit/tree/master/gtfs-realtime).

### Configuration Files
Configuration files for day types, time periods, thresholds, and passenger arrival rates are used to calculate performance metrics. The system requires configuration files for day types, time periods, thresholds, and passenger arrival rates that are formatted as .csv files. Detailed requirements for specific configuration files can be found in the [documentation](https://docs.google.com/document/d/19GcQ0UZmstbKBPDDD1G9uBnoqmIkWwvCgqfLaxWxfz8/edit#).

## System Functionality

For a description of the system functionality and performance metrics, please refer to the [documentation](https://docs.google.com/document/d/19GcQ0UZmstbKBPDDD1G9uBnoqmIkWwvCgqfLaxWxfz8/edit#).

## System Set-up

### System Requirements
* SQL Server 2014
* Visual Studio 2015
* Windows Server 2012

### Database Initialization
To start, execute the stored procedures and SQL Scripts that initialize the system. These scripts should only be executed at the very start of setting up the system. 

* Execute ‘CreateDatabase’ SQL Script:
	* creates the TRANSIT-performance database
* Execute ‘CreateSQLlogin’ SQL Script:
	* creates an SQL login needed for the applications and services to connect to the database, and to create database tables. The database role should be ‘db_owner’.
* Execute ‘CreateFunctionsAndTypes’ SQL Script:
	* creates user-defined functions to convert times between epoch and DATETIME types
	* creates user-defined data types used in the API calls	
* Execute ‘CreateInitializationTables’ SQL Script:
	* creates the ‘dbo.service\_date’ table which stores information about each service\_date that is processed.
	* creates the tables which store all arrival and departure events and times, all predicted arrival and departure events and times, and all service alerts data 
	* creates the historical tables which store performance information
	* creates the config tables which stores files for day types, time periods, thresholds, and passenger arrival rates
* Execute Create Procedure Scripts for Data Processing Stored Procedures
	* ‘PreProcessDaily’ - This procedure creates the tables that store performance information for the service date being processed. This is usually the previous service date. This stored procedure is executed by the ‘AppLauncher’ application. 
	* ‘PostProcessDaily’ - This procedure processes the performance data for the service date being processed. This is usually the previous service date. This stored procedure is executed by the ‘AppLauncher’ application.
	* ‘ProcessPredictionAccuracyDaily’ - This stored procedure processes the prediction data for the service\_date being processed. This is usually the previous service\_date. This stored procedure is executed by the ‘AppLauncher’ application.
	* ‘CreateTodayRTProcess’ - This stored procedure creates the tables that store real-time performance information for the upcoming service date. This stored procedure is executed by the ‘AppLauncher’ application.
	* ‘PreProcessToday’ - This procedure creates the tables that store performance information for the upcoming service date. This stored procedure is executed by the ‘AppLauncher’ application.
	* ‘ProcessRTEvent’ - This procedure processes the performance data for the current service date in real-time. This stored procedure is set to run as a Job every 1 minute (configurable) in the SQL Server Agent. 
	* ‘ProcessCurrentMetrics’ - This procedure processes the performance data for the current service date in real-time and calculates the current metrics for the day until now and the last hour. This stored procedure is set to run as a Job every 5 minutes (configurable) in the SQL Server Agent.
	* ‘UpdateGTFSNext’ - This procedure is needed by the ‘GTFSUpdate’ application. It creates arrival\_time\_sec and departure\_time\_sec fields in the gtfs.stop_times table and calculates arrival time and departure time in seconds after midnight. It also creates tables for the most common trip patterns for each route and direction based on GTFS.
	* ‘ClearData’ - This procedure cleans up disaggregate Trip Updates and Vehicle Positions data older than 31 days (configurable). This stored procedure is executed by the ‘AppLauncher’ application.													 
* Execute Create Procedure Scripts for Data Fetching Stored Procedures
	* ‘getCurrentMetrics’ - This procedure is called by the ‘currentmetrics’ API call. It retrieves the metrics for a route (or all routes) for the current service date until now and the last hour
	* ‘getDailyMetrics’ - This procedure is called by the ‘dailymetrics’ API call.It retrieves the daily metrics for a route (or all routes) for the requested service date(s)
	* ‘getDwellTimes’ - This procedure is called by the ‘dwells’ API call. It retrieves the dwell times for a stop (optionally filtered by route/direction) for the requested time period.
	* ‘getHeadwayTimes’ - This procedure is called by the ‘headways’ API call. It retrieves the headways for a stop (optionally filtered by route/direction or destination stop) for the requested time period
	* ‘getTravelTimes’ - This procedure is called by the ‘traveltimes’ API call. It retrieves the travel times for an o-d pair (optionally filtered by route/direction) for the requested time period
	* ‘getDailyPredictionMetrics’ - This procedure is called by the ‘dailypredictionmetrics’ API call. It retrieves the daily prediction accuracy metrics for a route (or all routes) for the requested service date(s).
	* ‘getPredictionMetrics’ - This procedure is called by the ‘predictionmetrics’ API call. It retrieves the prediction accuracy metrics in each thirty-minute time slice by route/direction/stop for the requested time period (optionally filtered by route/direction/stop).
	* ‘getEvents’ - This procedure is called by the ‘events’ API call. It retrieves all arrival and departure events for the requested time period (optionally filtered by route/direction/stop/vehicle label). 
	* ‘getPastAlerts’ - This procedure is one of the stored procedures called by the ‘pastalerts’ API call. It retrieves all alerts that were in effect for the requested time period (optionally filtered by route/stop/trip). 
	* ‘getPastAlertsVersions’ - This procedure is one of the stored procedures called by the ‘pastalerts’ API call. It retrieves version information for all the requested alerts. 
	* ‘getPastAlertsActivePeriods’ - This procedure is one of the stored procedures called by the ‘pastalerts’ API call. It retrieves all the active periods for all the requested alert versions. 
	* ‘getPastAlertsInformedEntities’ - This procedure is one of the stored procedures called by the ‘pastalerts’ API call. It retrieves all the informed entities for all the requested alert versions.

### Application and Services Initialization
Next add the applications and services that update the system and process data in real-time and daily. To run the applications and services, configuration files of the type .config.sample should be renamed to .config. Values in these files should be updated or added as necessary.

* ‘AppLauncher’
	* This application executes the following, in listed order, for checking for new GTFS and config files, daily processing of performance data for the previous service date, and setting up real-time processing for today
		* ‘GTFSUpdate’
		* ‘ConfigUpdate’
		* ‘PreProcessDaily’
		* ‘PostProcessDaily’
		* 'ProcessPredictionAccuracyDaily'
		* ‘PreProcessToday’
		* ‘CreateTodayRTProcess’
		* 'ClearData'
	* The order in which the tasks are executed is determined by the ‘tasks.json’ file. In this file, update the database configuration settings.
	* This application should be scheduled to run through the Windows Task Scheduler once per day  after the previous service day has ended and before the next service day begins (for example, at 3:00 AM).
* ‘GTFSUpdate’
	* This application checks the GTFS zip file source location and updates the database whenever a new GTFS zip file is added to the source location. This application is executed by ‘AppLauncher’.
	* To test this application, run the executable file. The tables that are part of the ‘gtfs’ schema in the database should be populated with the information from the GTFS files.
* ‘ConfigUpdate’
	* This application checks the config file source location and updates the database whenever a change to a config file has been made. This application is executed by ‘AppLauncher’
	* The configuration files should follow the structure outlined in the ‘config\_files\_structure.json’ file (also provided in the [documentation](https://docs.google.com/document/d/19GcQ0UZmstbKBPDDD1G9uBnoqmIkWwvCgqfLaxWxfz8/edit#)).
	* Place the configuration files in the config file source. 
	* To test this application, run the executable file. The database tables beginning with ‘config’ should be populated with the information from the configuration files.
* ‘gtfsrt\_events\_vp\_current\_status’
	* This service checks the GTFS-realtime VehiclePositions.pb source location every 5 seconds (configurable) and updates the database with new actual arrival and departure events. This service needs to be installed as a service then set to run continuously.
	* To test this service, start the service and check that the ‘rt_event’ table in the database is being populated with arrival and departure events. 
* ‘gtfsrt\_events\_tu\_latest\_prediction’
	* This service checks the GTFS-realtime TripUpdates.pb source location every 30 seconds (configurable) and updates the database with new predicted arrival and departure events. This service also archives predicted arrival and departure events for the previous day at the time given by the “RESETTIME” parameter in the config file. This service needs to be installed as a service then set to run continuously.
	* To test this service, start the service and check that the ‘event\_rt\_trip’ table in the database is being populated with the latest predicted times. The next day, all records from the previous day should have been moved to the ‘event\_rt\_trip\_archive’ table and the ‘event\_rt\_trip’ table should only contain records for the current day.
* ‘gtfsrt\_tripupdate\_denormalized’
	* This service checks the GTFS-realtime TripUpdates.pb source location every 30 seconds (configurable) and records all predicted arrival and departure events in the database every time the file is fetched. This service needs to be installed as a service then set to run continuously.
	* To test this service, start the service and check that the ‘gtfsrt\_tripupdate\_denormalized’ table is being populated with predicted arrival and departure events.
* ‘gtfsrt\_alerts’
	* This service checks the GTFS-realtime ServiceAlerts.pb source location every 15 seconds (configurable) and updates the database with new alerts and versions of existing alerts. This service needs to be installed as a service then set to run continuously.
	* To test this service, start the service and check that the ‘rt\_alert’, ‘rt\_alert\_active\_period’, rt_alert\_informed\_entity’ tables are being populated with alert information.	

Next, set up the jobs in SQL Server Agent to process data in real-time.

* ‘ProcessRTEvent’ - Set the procedure ‘dbo.ProcessRTEvent’ to execute every minute (configurable).
* ‘ProcessCurrentMetrics’ - Set the procedure ‘dbo.ProcessCurrentMetrics’ to execute every five minutes (configurable).


## Outputs

Once the system is set up, the AppLauncher will trigger all processes needed to run the system. Once all processes are executed, the system will provide:

* performance metrics data for the service date processed (this is usually the previous service date). The data will be found in the 'daily' database tables.
* scheduled service information for the upcoming day. The data will be found in the 'today' database tables .

In real-time, the system will provide:

* processed real-time performance metrics data for today. This data will be found in the 'today_rt' database tables. Data in these tables will be added/updated in real-time throughout the day.

API calls can be built using the Data Fetching Procedures created during the database initialization.

## About

This system was developed through a partnership between MBTA (Boston, MA) and  IBI Group and has been in operation since January 2015.

## License

TRANSIT-performance is licensed under the [MIT License](https://github.com/ibi-group/transit-performance/blob/master/LICENSE). 

## Code of Conduct

TRANSIT-performance is governed by the [Contributor Covenant](https://github.com/ibi-group/transit-performance/blob/master/CODE_OF_CONDUCT.md), version 1.4.
