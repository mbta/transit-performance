using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;

using log4net;

namespace gtfsrt_events_tu_latest_prediction
{
    internal class DatabaseThread
    {
        private readonly string SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();
        private readonly BlockingQueue<Event> InsertQueue;
        private readonly BlockingQueue<Event> UpdateQueue;
        private readonly ILog Log;
        private DataTable EventTable { get; set; }

        public DatabaseThread(ILog Log, BlockingQueue<Event> InsertEventQueue, BlockingQueue<Event> UpdateEventQueue)
        {
            // TODO: Complete member initialization
            this.Log = Log;

            InsertQueue = InsertEventQueue;
            UpdateQueue = UpdateEventQueue;
        }

        internal void ThreadRun()
        {
            while (true)
            {
                var updateEventList = new List<Event>();
                var insertEventList = new List<Event>();

                try
                {
                    while (UpdateQueue.GetCount() > 0)
                    {
                        var _event = UpdateQueue.Dequeue();
                        updateEventList.Add(_event);
                        insertEventList.Add(_event);
                    }
                    while (InsertQueue.GetCount() > 0)
                    {
                        var _event = InsertQueue.Dequeue();
                        insertEventList.Add(_event);
                    }
                    UpdateDatabaseTable(updateEventList, insertEventList);
                }
                catch (Exception ex)
                {
                    Log.Error(ex.Message);
                    Log.Error(ex.StackTrace);
                }
                Thread.Sleep(100);
            }
        }

        private void UpdateDatabaseTable(List<Event> updateEventList, List<Event> insertEventList)
        {
            if (updateEventList.Count > 0)
                DeleteRows(updateEventList);
            if (insertEventList.Count > 0)
                InsertRows(insertEventList);
        }

        private void InsertRows(List<Event> insertEventList)
        {
            if (EventTable == null)
            {
                CreateEventTable();
            }
            if (EventTable == null)
                return;

            EventTable.Clear();
            AddRows(insertEventList);
            Log.Debug("Trying to insert " + insertEventList.Count + " rows in database.");

            using (var connection = new SqlConnection(SqlConnectionString))
            {
                using (var sbc = new SqlBulkCopy(connection))
                {
                    connection.Open();
                    sbc.DestinationTableName = EventTable.TableName;
                    foreach (var column in EventTable.Columns)
                    {
                        sbc.ColumnMappings.Add(column.ToString(), column.ToString());
                    }
                    sbc.WriteToServer(EventTable);
                    connection.Close();
                    Log.Debug("Inserted  " + EventTable.Rows.Count + " rows in database.");
                }
            }
        }

        private void AddRows(List<Event> insertEventList)
        {
            foreach (var _event in insertEventList)
            {
                var eventRow = EventTable.NewRow();
                eventRow["service_date"] = _event.ServiceDate;
                eventRow["route_id"] = _event.RouteId;
                eventRow["trip_id"] = _event.TripId;
                eventRow["direction_id"] = _event.DirectionId ?? (object) DBNull.Value;
                eventRow["stop_id"] = _event.StopId;
                eventRow["vehicle_id"] = _event.VehicleId;
                eventRow["event_type"] = _event._EventType == EventType.PRA ? "PRA" : "PRD";
                eventRow["event_time"] = _event.EventTime;
                eventRow["file_time"] = _event.FileTimestamp;
                eventRow["event_identifier"] = _event.GetEventIdentifier();
                eventRow["stop_sequence"] = _event.StopSequence;
                eventRow["vehicle_label"] = _event.VehicleLabel;

                EventTable.Rows.Add(eventRow);
            }
                var withLabels = EventTable.Rows.Cast<DataRow>().Count(x => !x.IsNull("vehicle_label"));
        }

        private void CreateEventTable()
        {
            EventTable = new DataTable {TableName = "dbo.event_rt_trip"};

            var route_id = new DataColumn("route_id", typeof (string));
            var trip_id = new DataColumn("trip_id", typeof (string));
            var direction_id = new DataColumn("direction_id", typeof (int));
            var stop_id = new DataColumn("stop_id", typeof (string));
            var event_type = new DataColumn("event_type", typeof (string));
            var event_time = new DataColumn("event_time", typeof (int));
            var file_time = new DataColumn("file_time", typeof (int));
            var event_identifier = new DataColumn("event_identifier", typeof (string));
            var service_date = new DataColumn("service_date", typeof (DateTime));
            var vehicle_id = new DataColumn("vehicle_id", typeof (string));
            var stop_sequence = new DataColumn("stop_sequence", typeof (int));
            var vehicle_label = new DataColumn("vehicle_label", typeof (string));

            EventTable.Columns.Add(route_id);
            EventTable.Columns.Add(trip_id);
            EventTable.Columns.Add(direction_id);
            EventTable.Columns.Add(stop_id);
            EventTable.Columns.Add(event_type);
            EventTable.Columns.Add(event_time);
            EventTable.Columns.Add(file_time);
            EventTable.Columns.Add(event_identifier);
            EventTable.Columns.Add(service_date);
            EventTable.Columns.Add(vehicle_id);
            EventTable.Columns.Add(stop_sequence);
            EventTable.Columns.Add(vehicle_label);
        }

        private void DeleteRows(List<Event> updateEventList)
        {
            var deleteList = GetDeleteList(updateEventList);
            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();
                var query = "DELETE FROM dbo.event_rt_trip WHERE event_identifier in " + deleteList;
                var cmd = new SqlCommand
                          {
                              CommandText = query,
                              CommandTimeout = 20,
                              Connection = connection
                          };
                Log.Debug("Begin delete operation.");
                var rowsDeleted = cmd.ExecuteNonQuery();
                Log.Debug("Number of rows deleted from event table " + rowsDeleted + ".");
            }
        }

        private string GetDeleteList(List<Event> updateEventList)
        {
            //Log.Debug("Building delete list");
            var sbr = new StringBuilder();
            sbr.Append("(");
            foreach (var _event in updateEventList)
            {
                var temp = _event.GetEventIdentifier();
                sbr.Append("'");
                sbr.Append(temp);
                sbr.Append("'");
                sbr.Append(",");
            }
            sbr.Append("''");
            sbr.Append(")");
            //Log.Debug("Builded delete list with "+ updateEventList.Count +" items");
            return sbr.ToString();
        }
    }
}