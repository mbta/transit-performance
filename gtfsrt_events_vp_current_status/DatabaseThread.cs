using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading;

using log4net;

namespace gtfsrt_events_vp_current_status
{
    internal class DatabaseThread
    {
        private readonly EventQueue eventQueue;
        private readonly ILog Log;
        private readonly string sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();

        internal void ThreadRun()
        {
            while (true)
            {
                var eventList = new List<Event>();
                while (eventQueue.GetCount() > 0)
                {
                    // eventList = new List<Event>();
                    var _event = eventQueue.Dequeue();
                    eventList.Add(_event);
                }
                if (eventList.Count > 0)
                {
                    AddEventsToDatabase(eventList);
                }
                eventList.Clear();
                Thread.Sleep(100);
            }
        }

        private void AddEventsToDatabase(List<Event> eventList)
        {
            try
            {
                Log.Info("Start AddEventsToDatabase");

                var dataTable = GetEventTable();
                AddRowsToTable(dataTable, eventList);

                Log.Info("End AddRowsToTable");

                InsertInDatabase(dataTable);

                Log.Info("End InsertInDatabase");
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
            }
        }

        private void InsertInDatabase(DataTable dataTable)
        {
            try
            {
                var sqlConnection = new SqlConnection(sqlConnectionString);
                sqlConnection.Open();
                using (var s = new SqlBulkCopy(sqlConnectionString, SqlBulkCopyOptions.FireTriggers))
                {
                    s.DestinationTableName = dataTable.TableName;
                    foreach (var column in dataTable.Columns)
                        s.ColumnMappings.Add(column.ToString(), column.ToString());
                    s.WriteToServer(dataTable);
                }
                sqlConnection.Close();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
            }
        }

        private void AddRowsToTable(DataTable dataTable, List<Event> eventList)
        {
            var ignoreRoutes = GetIgnoreRoute();
            foreach (var _event in eventList)
            {
                if (!ignoreRoutes.Contains(_event.routeId))
                {
                    AddEventToRow(dataTable, _event);
                }
                else
                {
                    Log.Info("Ignored route: " + _event.routeId);
                }
            }
        }

        private List<string> GetIgnoreRoute()
        {
            var ignoreRoutes = ConfigurationManager.AppSettings["IgnoreRoutes"];
            var listOfIgnoreRoutes = new List<string>(ignoreRoutes.Split(','));
            return listOfIgnoreRoutes;
        }

        private List<string> GetAcceptRoute()
        {
            var ignoreRoutes = ConfigurationManager.AppSettings["AcceptRoutes"];
            var listOfIgnoreRoutes = new List<string>(ignoreRoutes.Split(','));
            return listOfIgnoreRoutes;
        }

        private void AddEventToRow(DataTable dataTable, Event _event)
        {
            var eventRow = dataTable.NewRow();
            eventRow["service_date"] = _event.serviceDate;
            eventRow["route_id"] = _event.routeId;
            eventRow["trip_id"] = _event.tripId;
            eventRow["direction_id"] = _event.directionId ?? (object) DBNull.Value;
            eventRow["stop_id"] = _event.stopId;
            eventRow["vehicle_id"] = _event.vehicleId;
            eventRow["vehicle_label"] = _event.vehicleLabel;
            eventRow["event_type"] = _event.eventType;
            eventRow["event_time"] = _event.eventTime;
            eventRow["stop_sequence"] = _event.stopSequence;
            eventRow["file_time"] = _event.fileTimestamp;

            dataTable.Rows.Add(eventRow);

        }

        private DataTable GetEventTable()
        {
            var eventTable = new DataTable("dbo.rt_event");

            var service_date = new DataColumn("service_date", typeof (DateTime));
            var route_id = new DataColumn("route_id", typeof (string));
            var trip_id = new DataColumn("trip_id", typeof (string));
            var direction_id = new DataColumn("direction_id", typeof (int)) {AllowDBNull = true};
            var stop_id = new DataColumn("stop_id", typeof (string));
            var vehicle_id = new DataColumn("vehicle_id", typeof (string));
            var vehicle_label = new DataColumn("vehicle_label", typeof (string));
            var event_type = new DataColumn("event_type", typeof (string));
            var event_time = new DataColumn("event_time", typeof (int));
            var stop_sequence = new DataColumn("stop_sequence", typeof (int));
            var file_time = new DataColumn("file_time", typeof (int));

            eventTable.Columns.Add(service_date);
            eventTable.Columns.Add(route_id);
            eventTable.Columns.Add(trip_id);
            eventTable.Columns.Add(direction_id);
            eventTable.Columns.Add(stop_id);
            eventTable.Columns.Add(vehicle_id);
            eventTable.Columns.Add(vehicle_label);
            eventTable.Columns.Add(event_type);
            eventTable.Columns.Add(event_time);
            eventTable.Columns.Add(stop_sequence);
            eventTable.Columns.Add(file_time);

            return eventTable;
        }

        public DatabaseThread(ILog Log, EventQueue eventQueue)
        {
            // TODO: Complete member initialization
            this.Log = Log;
            this.eventQueue = eventQueue;
        }
    }
}