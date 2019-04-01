using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

using GtfsRealtimeLib;

using IBI.DataAccess.DataSets;

using log4net;

namespace IBI.DataAccess.Models
{
    public static class Utils
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public static void BulkInsert(DataTable dataTable, string connectionString)
        {
            if (dataTable.Rows.Count <= 0)
                return;

            using (var connection = new SqlConnection(connectionString))
            {
                BulkInsert(dataTable, connection);
            }
        }

        public static void BulkInsert(DataTable dataTable, SqlConnection connection)
        {
            using (var sqlBulkCopy = new SqlBulkCopy(connection))
            {
                connection.Open();

                BulkInsert(dataTable, sqlBulkCopy);

                connection.Close();
            }
        }

        public static void BulkInsert(DataTable dataTable, SqlBulkCopy sqlBulkCopy)
        {
            BulkInsert(dataTable, sqlBulkCopy, new List<string>());
        }

        public static void BulkInsert(DataTable dataTable, SqlBulkCopy sqlBulkCopy, List<string> excludeColumns)
        {
            if (dataTable.Rows.Count == 0)
            {
                Log.Warn($"Table {dataTable.TableName} is empty...");
                return;
            }

            var insertedDataTable = dataTable.Rows.Cast<DataRow>().Where(x => x.RowState == DataRowState.Added).CopyToDataTable();

            sqlBulkCopy.DestinationTableName = dataTable.TableName;
            sqlBulkCopy.ColumnMappings.Clear();

            foreach (var column in dataTable.Columns.Cast<DataColumn>().Where(column => !excludeColumns.Contains(column.ColumnName)))
            {
                sqlBulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());
            }

            sqlBulkCopy.WriteToServer(insertedDataTable);
        }

        public static bool AreEqual(string a, string b)
        {
            return (string.IsNullOrEmpty(a) && string.IsNullOrEmpty(b)) || string.Equals(a, b);
        }

        public static DateTime? GetUtcTimeFromSeconds(ulong seconds)
        {
            return seconds > 0 ? DateTime.SpecifyKind(new DateTime(1970, 1, 1).AddSeconds(seconds), DateTimeKind.Utc) : (DateTime?)null;
        }

        public static ulong GetSecondsFromUtc(DateTime utcDateTime)
        {
            return (ulong)utcDateTime.Subtract(DateTime.SpecifyKind(new DateTime(1970, 1, 1), DateTimeKind.Utc)).Seconds;
        }

        public static List<AlertData> GetAlerts(FeedMessage feedMessage)
        {
            var alerts = new List<AlertData>();

            foreach (var entity in feedMessage.entity.Where(x => x.alert != null))
            {
                if (entity.alert.description_text == null)
                {
                    foreach (var headerTranslation in entity.alert.header_text.translation)
                    {
                        var alert = GetAlert(feedMessage, entity, null, headerTranslation);
                        alerts.Add(alert);
                    }
                }
                else
                    foreach (var translation in entity.alert.description_text.translation)
                    {
                        foreach (var headerTranslation in entity.alert.header_text.translation)
                        {
                            var alert = GetAlert(feedMessage, entity, translation, headerTranslation);
                            alerts.Add(alert);
                        }
                    }
            }

            return alerts;
        }

        private static AlertData GetAlert(FeedMessage feedMessage,
                                          FeedEntity entity,
                                          TranslatedString.Translation translation,
                                          TranslatedString.Translation headerTranslation)
        {
            return new AlertData
                   {
                       AlertId = entity.id,
                       Cause = entity.alert.cause.ToString(),
                       DescriptionLanguage = translation?.language,
                       DescriptionText = translation?.text,
                       Effect = entity.alert.effect.ToString(),
                       GtfsRealtimeVersion = feedMessage.header?.gtfs_realtime_version,
                       HeaderLanguage = headerTranslation.language,
                       HeaderText = headerTranslation.text,
                       HeaderTimestamp = feedMessage.header?.timestamp ?? 0,
                       Incrementality = feedMessage.header?.incrementality.ToString(),
                       Url = entity.alert.url?.translation.FirstOrDefault()?.text,
                       InformedEntities = entity.alert.informed_entity.Select(e => new AlertInformedEntityData
                                                                                   {
                                                                                       HeaderTimestamp = feedMessage.header.timestamp,
                                                                                       AlertId = entity.id,
                                                                                       AgencyId = e.agency_id,
                                                                                       RouteId = e.route_id,
                                                                                       RouteType = e.route_type,
                                                                                       StopId = e.stop_id,
                                                                                       TripId = e.trip?.trip_id
                                                                                   })
                                                .ToList(),
                       ActivePeriods = entity.alert.active_period.Select(a => new AlertActivePeriodData
                                                                              {
                                                                                  HeaderTimestamp = feedMessage.header.timestamp,
                                                                                  AlertId = entity.id,
                                                                                  ActivePeriodEnd = a.end,
                                                                                  ActivePeriodStart = a.start
                                                                              })
                                             .ToList()
                   };
        }

        public static void InsertAlertsRows(List<AlertData> alerts, ref List<string> previousAlertIds, bool useTemporaryTables)
        {
            var step = "start";

            try
            {
                var alertsUpdateDataSet = new AlertsDataSet();

                if (!alerts.Any())
                {
                    Log.Info("No alerts to save...");
                }
                else
                {
                    Log.Info($"Trying to check {alerts.Count} alert(s).");

                    //var alertsNoFirstTime = _alertsUpdateDataSet.CheckNoFirstTimeList();

                    var alertsSaved = alertsUpdateDataSet.SaveAlerts(alerts, useTemporaryTables);
                    Log.Info($"Saved successfully {alertsSaved} alert(s)");
                }

                step = "checkClosed";

                var currentAlertIds = alerts.Select(y => y.AlertId).ToList();

                var alertIdsToClose = previousAlertIds.Where(x => !currentAlertIds.Contains(x)).ToList();

                var moreToClose = 0;

                if (alertIdsToClose.Any() || !previousAlertIds.Any())
                {
                    var closedSaved = alertsUpdateDataSet.CheckClosedAlerts(alerts,
                                                                            alertIdsToClose.Any() ? alertIdsToClose : new List<string>(),
                                                                            out moreToClose,
                                                                            200,
                                                                            useTemporaryTables);

                    Log.Info(closedSaved > 0 ? $"Closed {closedSaved} alert(s); {moreToClose} more to close..." : "No alerts need to be closed");
                }
                else
                {
                    Log.Info("No alerts need to be closed");
                }

                if (moreToClose == 0)
                    previousAlertIds = alerts.Select(x => x.AlertId).ToList();
                else
                    previousAlertIds.Clear();
            }
            catch (Exception exception)
            {
                Log.Error($"Failed to save data ({step}): {exception.Message}");
                previousAlertIds.Clear();
            }
        }
    }
}
