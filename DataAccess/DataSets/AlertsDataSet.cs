using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

using IBI.DataAccess.DataSets.AlertsDataSetTableAdapters;
using IBI.DataAccess.Models;

using log4net;

namespace IBI.DataAccess.DataSets
{
    partial class AlertsDataSet
    {
        internal static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        public static readonly string PERIOD_END_CHANGE_SECONDS = ConfigurationManager.AppSettings["PERIOD_END_CHANGE_SECONDS"];

        internal static int ActivePeriodEndChangeSeconds => string.IsNullOrEmpty(PERIOD_END_CHANGE_SECONDS) ? 300 : int.Parse(PERIOD_END_CHANGE_SECONDS);

        partial class rt_alertRow
        {
            public AlertData Alert => new AlertData
            {
                AlertId = alert_id,
                Cause = IscauseNull() ? null : cause,
                Effect = IseffectNull() ? null : effect,
                DescriptionText = Isdescription_textNull() ? null : description_text,
                HeaderText = Isheader_textNull() ? null : header_text,
                Url = IsurlNull() ? null : url,
                ActivePeriods = Getrt_alert_active_periodRows().Select(x => x.ActivePeriod).ToList(),
                InformedEntities = Getrt_alert_informed_entityRows().Select(x => x.InformedEntity).ToList(),
                Closed = closed
            };
        }

        partial class rt_alert_active_periodRow
        {
            public AlertActivePeriodData ActivePeriod => new AlertActivePeriodData
            {
                AlertId = alert_id,
                ActivePeriodStart = (ulong)(Isactive_period_startNull() ? 0 : active_period_start),
                ActivePeriodEnd = (ulong)(Isactive_period_endNull() ? 0 : active_period_end)
            };
        }

        partial class rt_alert_informed_entityRow
        {
            public AlertInformedEntityData InformedEntity => new AlertInformedEntityData
            {
                AlertId = alert_id,
                AgencyId = Isagency_idNull() ? null : agency_id,
                RouteId = Isroute_idNull() ? null : route_id,
                RouteType = Isroute_typeNull() ? 0 : route_type,
                StopId = Isstop_idNull() ? null : stop_id,
                TripId = Istrip_idNull() ? null : trip_id
            };
        }

        private readonly string SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();

        public void SaveAlertsDenormalized(IEnumerable<AlertData> alerts)
        {
            var activePeriodsDataTable = new gtfsrt_alert_active_period_denormalizedDataTable { TableName = "dbo.gtfsrt_alert_active_period_denormalized" };
            var alertsDataTable = new gtfsrt_alert_denormalizedDataTable { TableName = "dbo.gtfsrt_alert_denormalized" };
            var informedEntitiesDataTable = new gtfsrt_alert_informed_entity_denormalizedDataTable { TableName = "dbo.gtfsrt_alert_informed_entity_denormalized" };

            foreach (var alert in alerts)
            {
                var newAlertRow = alertsDataTable.Newgtfsrt_alert_denormalizedRow();

                newAlertRow.gtfs_realtime_version = alert.GtfsRealtimeVersion;
                newAlertRow.incrementality = alert.Incrementality;
                newAlertRow.header_timestamp = (int)alert.HeaderTimestamp;
                newAlertRow.alert_id = alert.AlertId;
                newAlertRow.cause = alert.Cause;
                newAlertRow.effect = alert.Effect;
                newAlertRow.header_text = alert.HeaderText;
                newAlertRow.header_language = alert.HeaderLanguage;
                newAlertRow.description_text = alert.DescriptionText;
                newAlertRow.description_language = alert.DescriptionLanguage;
                newAlertRow.url = alert.Url;

                alertsDataTable.Rows.Add(newAlertRow);

                foreach (var activePeriod in alert.ActivePeriods)
                {
                    var newActivePeriodRow = activePeriodsDataTable.Newgtfsrt_alert_active_period_denormalizedRow();

                    newActivePeriodRow.header_timestamp = (int)activePeriod.HeaderTimestamp;
                    newActivePeriodRow.active_period_start = (int)activePeriod.ActivePeriodStart;
                    if (activePeriod.ActivePeriodEnd > 0)
                        newActivePeriodRow.active_period_end = (int)activePeriod.ActivePeriodEnd;
                    newActivePeriodRow.alert_id = activePeriod.AlertId;

                    activePeriodsDataTable.Rows.Add(newActivePeriodRow);
                }

                foreach (var informedEntity in alert.InformedEntities)
                {
                    var newInformedEntityRow = informedEntitiesDataTable.Newgtfsrt_alert_informed_entity_denormalizedRow();

                    newInformedEntityRow.header_timestamp = (int)informedEntity.HeaderTimestamp;
                    newInformedEntityRow.alert_id = informedEntity.AlertId;
                    newInformedEntityRow.agency_id = informedEntity.AgencyId;
                    if (!string.IsNullOrEmpty(informedEntity.RouteId))
                        newInformedEntityRow.route_id = informedEntity.RouteId;
                    newInformedEntityRow.route_type = informedEntity.RouteType;
                    if (!string.IsNullOrEmpty(informedEntity.TripId))
                        newInformedEntityRow.trip_id = informedEntity.TripId;
                    if (!string.IsNullOrEmpty(informedEntity.StopId))
                        newInformedEntityRow.stop_id = informedEntity.StopId;

                    informedEntitiesDataTable.Rows.Add(newInformedEntityRow);
                }
            }

            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transaction))
                    {
                        Utils.BulkInsert(alertsDataTable, sqlBulkCopy);
                        Utils.BulkInsert(informedEntitiesDataTable, sqlBulkCopy);
                        Utils.BulkInsert(activePeriodsDataTable, sqlBulkCopy);

                        transaction.Commit();
                    }
                }
            }
        }

        public int SaveAlerts(List<AlertData> alerts, bool tempTables)
        {
            var alertsSaved = 0;

            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();

                var alertsTableAdapter = new rt_alertTableAdapter { Connection = connection, ClearBeforeFill = true};
                var activePeriodsTableAdapter = new rt_alert_active_periodTableAdapter { Connection = connection, ClearBeforeFill = true };
                var informedEntitiesTableAdapter = new rt_alert_informed_entityTableAdapter { Connection = connection, ClearBeforeFill = true };

                if (tempTables)
                {
                    alertsTableAdapter.SetTempTable();
                    activePeriodsTableAdapter.SetTempTable();
                    informedEntitiesTableAdapter.SetTempTable();
                }

                var alertIds = string.Join(",", alerts.Select(x => $"'{x.AlertId}'"));
                //var query = $" where alert_id in ({alertIds})";

                EnforceConstraints = false;

                alertsTableAdapter.SelectCommand.CommandText = alertsTableAdapter.SelectByMaxVersionCommand.CommandText.Replace("'000000'", alertIds);
                alertsTableAdapter.Fill(rt_alert);

                activePeriodsTableAdapter.SelectCommand.CommandText = activePeriodsTableAdapter.SelectByMaxVersionCommand.CommandText.Replace("'000000'", alertIds);
                activePeriodsTableAdapter.Fill(rt_alert_active_period);

                informedEntitiesTableAdapter.SelectCommand.CommandText = informedEntitiesTableAdapter.SelectByMaxVersionCommand.CommandText.Replace("'000000'", alertIds);
                informedEntitiesTableAdapter.Fill(rt_alert_informed_entity);

                foreach (var alert in alerts)
                {
                    if (alert.AlertId == null)
                        continue;

                    var existingAlert = rt_alert.Where(x => x.alert_id == alert.AlertId).OrderByDescending(x => x.version_id).FirstOrDefault();

                    if (existingAlert != null && !existingAlert.Islast_file_timeNull() && existingAlert.last_file_time >= (int)alert.HeaderTimestamp)
                    {
                        // ignore, we have a newer (or the same) file with this alert that was processed
                        continue;
                    }

                    var equal = existingAlert != null && alert.Equals(existingAlert.Alert);

                    if (equal)
                    {
                        // update only last file time...
                        existingAlert.last_file_time = (int)alert.HeaderTimestamp;
                        alertsSaved++;
                    }
                    else
                    {
                        alert.CheckPeriodEndChange = true;
                        equal = existingAlert != null && alert.Equals(existingAlert.Alert);

                        alertsSaved++;

                        if (existingAlert == null || !equal)
                            InsertAlert(alert, existingAlert);
                        else
                            UpdateAlert(alert, existingAlert);
                    }
                }

                using (var transaction = connection.BeginTransaction())
                {
                    alertsTableAdapter.Transaction = transaction;
                    alertsTableAdapter.Update(rt_alert);

                    informedEntitiesTableAdapter.Transaction = transaction;
                    informedEntitiesTableAdapter.Update(rt_alert_informed_entity);

                    activePeriodsTableAdapter.Transaction = transaction;
                    activePeriodsTableAdapter.Update(rt_alert_active_period);

                    transaction.Commit();
                }
            }

            return alertsSaved;
        }

        public void CopyAlertsInTempTablesAndDeleteAfterBlackout(int startTime)
        {
            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();

                var alertsTableAdapter = new rt_alert_tempTableAdapter { Connection = connection };
                var activePeriodsTableAdapter = new rt_alert_active_period_tempTableAdapter { Connection = connection };
                var informedEntitiesTableAdapter = new rt_alert_informed_entity_tempTableAdapter { Connection = connection };

                var count = alertsTableAdapter.SelectCount();

                alertsTableAdapter.Fill(rt_alert_temp, startTime);
                activePeriodsTableAdapter.Fill(rt_alert_active_period_temp, startTime);
                informedEntitiesTableAdapter.Fill(rt_alert_informed_entity_temp, startTime);

                if ( /*rt_alert_temp.Count > 0 ||*/ count == 0)
                {
                    //activePeriodsTableAdapter.DeleteAll();
                    //informedEntitiesTableAdapter.DeleteAll();
                    //alertsTableAdapter.DeleteAll();

                    var command = new SqlCommand("SET IDENTITY_INSERT rt_alert_temp ON", connection);
                    command.ExecuteNonQuery();
                    alertsTableAdapter.InsertFromMainTable();
                    command.CommandText = "SET IDENTITY_INSERT rt_alert_temp OFF";
                    command.ExecuteNonQuery();
                    activePeriodsTableAdapter.InsertFromMainTable();
                    informedEntitiesTableAdapter.InsertFromMainTable();
                }

                activePeriodsTableAdapter.DeleteQuery(startTime);
                informedEntitiesTableAdapter.DeleteQuery(startTime);
                alertsTableAdapter.DeleteQuery(startTime);

                //alertsTableAdapter.Fill(rt_alert_temp, startTime);
                //activePeriodsTableAdapter.Fill(rt_alert_active_period_temp, startTime);
                //informedEntitiesTableAdapter.Fill(rt_alert_informed_entity_temp, startTime);
            }
        }

        public int CheckClosedAlerts(List<AlertData> alerts, List<string> alertIdsToClose, out int moreToClose, int closeMax = 200, bool useTemporaryTables = false)
        {
            var alertsSaved = 0;

            moreToClose = 0;

            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();

                var alertsTableAdapter = new rt_alertTableAdapter { Connection = connection };
                var alertsTimeTableAdapter = new rt_alert_timesTableAdapter { Connection = connection };
                var activePeriodsTableAdapter = new rt_alert_active_periodTableAdapter { Connection = connection, ClearBeforeFill = false };
                var informedEntitiesTableAdapter = new rt_alert_informed_entityTableAdapter { Connection = connection, ClearBeforeFill = false };

                if (useTemporaryTables)
                {
                    alertsTableAdapter.SetTempTable();
                    activePeriodsTableAdapter.SetTempTable();
                    informedEntitiesTableAdapter.SetTempTable();
                    alertsTimeTableAdapter.SetTempTable();

                    //rt_alert.TableName = rt_alert.TableName + "_temp";
                    //rt_alert_active_period.TableName = rt_alert_active_period.TableName + "_temp";
                    //rt_alert_informed_entity.TableName = rt_alert_informed_entity.TableName + "_temp";
                }

                if (alertIdsToClose == null || !alertIdsToClose.Any())
                    alertsTableAdapter.FillByOpenAlerts(rt_alert);
                else
                {
                    var alertIds = string.Join(",", alertIdsToClose.Select(x => $"'{x}'"));
                    var query = $" and (alerts.alert_id in ({alertIds}))";

                    alertsTableAdapter.SelectOpenAlertsCommand.CommandText += query;
                    alertsTableAdapter.FillByOpenAlerts(rt_alert);
                }

                if (!rt_alert.Any())
                    return 0;

                var currentTime = DateTime.UtcNow;
                var currentAlertIds = alerts.Select(x => x.AlertId).OrderBy(x => x).ToList();
                var fileTime = alerts.FirstOrDefault()?.HeaderTimestamp ?? Utils.GetSecondsFromUtc(currentTime);

                var alertsToClose = rt_alert.Where(x => !currentAlertIds.Contains(x.alert_id)).OrderBy(x => x.alert_id).ToList();
                var closedAlertsDataTable = new rt_alertDataTable { TableName = rt_alert_temp.TableName };
                var closedActivePeriodsDataTable = new rt_alert_active_periodDataTable { TableName = rt_alert_active_period_temp.TableName };
                var closedInformedEntitiesDataTable = new rt_alert_informed_entityDataTable { TableName = rt_alert_informed_entity_temp.TableName };

                moreToClose = Math.Max(alertsToClose.Count - closeMax, 0);

                if (alertsToClose.Count == 0)
                    return 0;

                foreach (var alertRow in alertsToClose.Take(closeMax))
                {
                    activePeriodsTableAdapter.ClearBeforeFill = true;
                    informedEntitiesTableAdapter.ClearBeforeFill = true;
                    alertsTimeTableAdapter.ClearBeforeFill = true;

                    activePeriodsTableAdapter.FillByAlertIdVersionId(rt_alert_active_period, alertRow.alert_id, alertRow.version_id);
                    informedEntitiesTableAdapter.FillByAlertIdVersionId(rt_alert_informed_entity, alertRow.alert_id, alertRow.version_id);
                    alertsTimeTableAdapter.FillByNextFileTimeAfterAlert(rt_alert_times, alertRow.record_id);

                    if (alertIdsToClose == null || !alertIdsToClose.Any())
                    {
                        var nextAlertTime = rt_alert_times.FirstOrDefault();
                        if (nextAlertTime != null)
                            fileTime = (ulong)nextAlertTime.file_time;
                    }

                    var alert = alertRow.Alert;
                    alert.Closed = true;
                    alert.HeaderTimestamp = fileTime;

                    InsertAlert(alert, alertRow, closedAlertsDataTable, closedActivePeriodsDataTable, closedInformedEntitiesDataTable);

                    alertsSaved++;
                }

                using (var transaction = connection.BeginTransaction())
                {
                    using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transaction))
                    {
                        Utils.BulkInsert(closedAlertsDataTable, sqlBulkCopy, new List<string> { "record_id" });
                        Utils.BulkInsert(closedActivePeriodsDataTable, sqlBulkCopy);
                        Utils.BulkInsert(closedInformedEntitiesDataTable, sqlBulkCopy);

                        transaction.Commit();
                    }
                }
            }

            return alertsSaved;
        }

        private void UpdateAlert(AlertData alert, rt_alertRow existingAlert)
        {
            existingAlert.file_time = (int)alert.HeaderTimestamp;
            existingAlert.last_file_time = (int)alert.HeaderTimestamp;

            foreach (var activePeriod in alert.ActivePeriods)
            {
                var activePeriodRow = existingAlert.Getrt_alert_active_periodRows()
                    .FirstOrDefault(x => x.active_period_start == (int)activePeriod.ActivePeriodStart);

                if (activePeriodRow == null)
                    continue;

                if (activePeriod.ActivePeriodEnd > 0)
                    activePeriodRow.active_period_end = (int)activePeriod.ActivePeriodEnd;
                else if (!activePeriodRow.Isactive_period_endNull())
                    activePeriodRow.Setactive_period_endNull();
            }
        }

        private static void InsertAlert(AlertData alert,
                                        rt_alertRow existingAlert,
                                        rt_alertDataTable alertDataTable,
                                        rt_alert_active_periodDataTable activePeriodDataTable,
                                        rt_alert_informed_entityDataTable informedEntityDataTable)
        {
            var newAlertRow = alertDataTable.Newrt_alertRow();

            newAlertRow.file_time = (int)alert.HeaderTimestamp;
            newAlertRow.alert_id = alert.AlertId;
            newAlertRow.version_id = existingAlert?.version_id + 1 ?? 1;
            newAlertRow.cause = alert.Cause;
            newAlertRow.effect = alert.Effect;
            newAlertRow.header_text = alert.HeaderText;
            newAlertRow.description_text = alert.DescriptionText;
            newAlertRow.url = alert.Url;
            newAlertRow.closed = alert.Closed;
            newAlertRow.first_file_time = (int)alert.HeaderTimestamp;
            newAlertRow.last_file_time = (int)alert.HeaderTimestamp;

            alertDataTable.Rows.Add(newAlertRow);
            //Console.WriteLine("*Alert: " + newAlertRow.Alert);

            foreach (var activePeriod in alert.ActivePeriods)
            {
                var newActivePeriodRow = activePeriodDataTable.Newrt_alert_active_periodRow();

                newActivePeriodRow.version_id = newAlertRow.version_id;
                newActivePeriodRow.active_period_start = (int)activePeriod.ActivePeriodStart;
                if (activePeriod.ActivePeriodEnd > 0)
                    newActivePeriodRow.active_period_end = (int)activePeriod.ActivePeriodEnd;
                newActivePeriodRow.alert_id = activePeriod.AlertId;

                activePeriodDataTable.Rows.Add(newActivePeriodRow);
                //Console.WriteLine(" *Active Period: " + newActivePeriodRow.ActivePeriod);
            }

            foreach (var informedEntity in alert.InformedEntities)
            {
                var newInformedEntityRow = informedEntityDataTable.Newrt_alert_informed_entityRow();

                newInformedEntityRow.version_id = newAlertRow.version_id;
                newInformedEntityRow.alert_id = informedEntity.AlertId;
                newInformedEntityRow.agency_id = informedEntity.AgencyId;
                if (!string.IsNullOrEmpty(informedEntity.RouteId))
                    newInformedEntityRow.route_id = informedEntity.RouteId;
                newInformedEntityRow.route_type = informedEntity.RouteType;
                if (!string.IsNullOrEmpty(informedEntity.TripId))
                    newInformedEntityRow.trip_id = informedEntity.TripId;
                if (!string.IsNullOrEmpty(informedEntity.StopId))
                    newInformedEntityRow.stop_id = informedEntity.StopId;

                informedEntityDataTable.Rows.Add(newInformedEntityRow);
                //Console.WriteLine(" *Informed Entity: " + newInformedEntityRow.InformedEntity);
            }
        }

        private void InsertAlert(AlertData alert, rt_alertRow existingAlert)
        {
            var newAlertRow = rt_alert.Newrt_alertRow();

            newAlertRow.file_time = (int)alert.HeaderTimestamp;
            newAlertRow.alert_id = alert.AlertId;
            newAlertRow.version_id = existingAlert?.version_id + 1 ?? 1;
            newAlertRow.cause = alert.Cause;
            newAlertRow.effect = alert.Effect;
            newAlertRow.header_text = alert.HeaderText;
            newAlertRow.description_text = alert.DescriptionText;
            newAlertRow.url = alert.Url;
            newAlertRow.closed = alert.Closed;
            newAlertRow.first_file_time = (int)alert.HeaderTimestamp;
            newAlertRow.last_file_time = (int)alert.HeaderTimestamp;

            rt_alert.Rows.Add(newAlertRow);
            //Console.WriteLine("*Alert: " + newAlertRow.Alert);

            foreach (var activePeriod in alert.ActivePeriods)
            {
                var newActivePeriodRow = rt_alert_active_period.Newrt_alert_active_periodRow();

                newActivePeriodRow.version_id = newAlertRow.version_id;
                newActivePeriodRow.active_period_start = (int)activePeriod.ActivePeriodStart;
                if (activePeriod.ActivePeriodEnd > 0)
                    newActivePeriodRow.active_period_end = (int)activePeriod.ActivePeriodEnd;
                newActivePeriodRow.alert_id = activePeriod.AlertId;

                rt_alert_active_period.Rows.Add(newActivePeriodRow);
                //Console.WriteLine(" *Active Period: " + newActivePeriodRow.ActivePeriod);
            }

            foreach (var informedEntity in alert.InformedEntities)
            {
                var newInformedEntityRow = rt_alert_informed_entity.Newrt_alert_informed_entityRow();

                newInformedEntityRow.version_id = newAlertRow.version_id;
                newInformedEntityRow.alert_id = informedEntity.AlertId;
                newInformedEntityRow.agency_id = informedEntity.AgencyId;
                if (!string.IsNullOrEmpty(informedEntity.RouteId))
                    newInformedEntityRow.route_id = informedEntity.RouteId;
                newInformedEntityRow.route_type = informedEntity.RouteType;
                if (!string.IsNullOrEmpty(informedEntity.TripId))
                    newInformedEntityRow.trip_id = informedEntity.TripId;
                if (!string.IsNullOrEmpty(informedEntity.StopId))
                    newInformedEntityRow.stop_id = informedEntity.StopId;

                rt_alert_informed_entity.Rows.Add(newInformedEntityRow);
                //Console.WriteLine(" *Informed Entity: " + newInformedEntityRow.InformedEntity);
            }
        }
    }
}

namespace IBI.DataAccess.DataSets.AlertsDataSetTableAdapters
{
    partial class rt_alert_timesTableAdapter
    {
        internal void SetTempTable()
        {
            const string oldTable = "rt_alert";
            const string newTable = oldTable + "_temp";
            foreach (var sqlCommand in CommandCollection)
            {
                sqlCommand.CommandText = sqlCommand.CommandText.Replace(oldTable, newTable);
            }

            if (_adapter.UpdateCommand != null)
                _adapter.UpdateCommand.CommandText = _adapter.UpdateCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.DeleteCommand != null)
                _adapter.DeleteCommand.CommandText = _adapter.DeleteCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.InsertCommand != null)
                _adapter.InsertCommand.CommandText = _adapter.InsertCommand.CommandText.Replace(oldTable, newTable);
        }
    }

    partial class rt_alert_informed_entityTableAdapter
    {
        public SqlCommand SelectCommand => CommandCollection[0];
        public SqlCommand SelectByMaxVersionCommand => CommandCollection[3];

        internal void SetTempTable()
        {
            const string oldTable = "rt_alert_informed_entity";
            const string newTable = oldTable + "_temp";
            foreach (var sqlCommand in CommandCollection)
            {
                sqlCommand.CommandText = sqlCommand.CommandText.Replace(oldTable, newTable);
                sqlCommand.CommandText = sqlCommand.CommandText.Replace("rt_alert", "rt_alert_temp");
                sqlCommand.CommandText = sqlCommand.CommandText.Replace("rt_alert_temp_", "rt_alert_");
            }

            if (_adapter.UpdateCommand != null)
                _adapter.UpdateCommand.CommandText = _adapter.UpdateCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.DeleteCommand != null)
                _adapter.DeleteCommand.CommandText = _adapter.DeleteCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.InsertCommand != null)
                _adapter.InsertCommand.CommandText = _adapter.InsertCommand.CommandText.Replace(oldTable, newTable);
        }
    }

    partial class rt_alert_active_periodTableAdapter
    {
        public SqlCommand SelectCommand => CommandCollection[0];
        public SqlCommand SelectByMaxVersionCommand => CommandCollection[3];

        internal void SetTempTable()
        {
            const string oldTable = "rt_alert_active_period";
            const string newTable = oldTable + "_temp";
            foreach (var sqlCommand in CommandCollection)
            {
                sqlCommand.CommandText = sqlCommand.CommandText.Replace(oldTable, newTable);
                sqlCommand.CommandText = sqlCommand.CommandText.Replace("rt_alert", "rt_alert_temp");
                sqlCommand.CommandText = sqlCommand.CommandText.Replace("rt_alert_temp_", "rt_alert_");
            }

            if (_adapter.UpdateCommand != null)
                _adapter.UpdateCommand.CommandText = _adapter.UpdateCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.DeleteCommand != null)
                _adapter.DeleteCommand.CommandText = _adapter.DeleteCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.InsertCommand != null)
                _adapter.InsertCommand.CommandText = _adapter.InsertCommand.CommandText.Replace(oldTable, newTable);
        }
    }

    public partial class rt_alertTableAdapter
    {
        public SqlCommand SelectCommand => CommandCollection[0];
        public SqlCommand SelectByMaxVersionCommand => CommandCollection[2];
        public SqlCommand SelectOpenAlertsCommand => CommandCollection[3];

        internal void SetTempTable()
        {
            const string oldTable = "rt_alert";
            const string newTable = oldTable + "_temp";
            foreach (var sqlCommand in CommandCollection)
            {
                sqlCommand.CommandText = sqlCommand.CommandText.Replace(oldTable, newTable);
            }

            if (_adapter.UpdateCommand != null)
                _adapter.UpdateCommand.CommandText = _adapter.UpdateCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.DeleteCommand != null)
                _adapter.DeleteCommand.CommandText = _adapter.DeleteCommand.CommandText.Replace(oldTable, newTable);
            if (_adapter.InsertCommand != null)
                _adapter.InsertCommand.CommandText = _adapter.InsertCommand.CommandText.Replace(oldTable, newTable);
        }
    }
}
