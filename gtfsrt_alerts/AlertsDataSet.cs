using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;

using GtfsRealtimeLib;

namespace gtfsrt_alerts
{
    partial class AlertsDataSet
    {
        private readonly string SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();

        internal void SaveAlerts(List<AlertData> alerts, List<AlertActivePeriodData> activePeriods, List<AlertInformedEntityData> informedEntities)
        {
            var activePeriodsDataTable = new gtfsrt_alert_active_period_denormalizedDataTable {TableName = "dbo.gtfsrt_alert_active_period_denormalized"};
            var alertsDataTable = new gtfsrt_alert_denormalizedDataTable {TableName = "dbo.gtfsrt_alert_denormalized"};
            var informedEntitiesDataTable = new gtfsrt_alert_informed_entity_denormalizedDataTable {TableName = "dbo.gtfsrt_alert_informed_entity_denormalized"};

            foreach (var alert in alerts)
            {
                var newRow = alertsDataTable.Newgtfsrt_alert_denormalizedRow();

                newRow.gtfs_realtime_version = alert.GtfsRealtimeVersion;
                newRow.incrementality = alert.Incrementality;
                newRow.header_timestamp = (int) alert.HeaderTimestamp;
                newRow.alert_id = alert.AlertId;
                newRow.cause = alert.Cause;
                newRow.effect = alert.Effect;
                newRow.header_text = alert.HeaderText;
                newRow.header_language = alert.HeaderLanguage;
                newRow.description_text = alert.DescriptionText;
                newRow.description_language = alert.DescriptionLanguage;
                newRow.url = alert.Url;

                alertsDataTable.Rows.Add(newRow);
            }

            foreach (var activePeriod in activePeriods)
            {
                var newRow = activePeriodsDataTable.Newgtfsrt_alert_active_period_denormalizedRow();

                newRow.header_timestamp = (int) activePeriod.HeaderTimestamp;
                newRow.active_period_end = (int) activePeriod.ActivePeriodEnd;
                newRow.active_period_start = (int) activePeriod.ActivePeriodStart;
                newRow.alert_id = activePeriod.AlertId;

                activePeriodsDataTable.Rows.Add(newRow);
            }

            foreach (var informedEntity in informedEntities)
            {
                var newRow = informedEntitiesDataTable.Newgtfsrt_alert_informed_entity_denormalizedRow();

                newRow.header_timestamp = (int) informedEntity.HeaderTimestamp;
                newRow.alert_id = informedEntity.AlertId;
                newRow.agency_id = informedEntity.AgencyId;
                if (!string.IsNullOrEmpty(informedEntity.RouteId))
                {
                    newRow.route_id = informedEntity.RouteId;
                    newRow.route_type = informedEntity.RouteType;
                }
                if (!string.IsNullOrEmpty(informedEntity.TripId))
                    newRow.trip_id = informedEntity.TripId;
                if (!string.IsNullOrEmpty(informedEntity.StopId))
                    newRow.stop_id = informedEntity.StopId;

                informedEntitiesDataTable.Rows.Add(newRow);
            }

            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transaction))
                    {
                        GtfsData.BulkInsert(alertsDataTable, sqlBulkCopy);
                        GtfsData.BulkInsert(informedEntitiesDataTable, sqlBulkCopy);
                        GtfsData.BulkInsert(activePeriodsDataTable, sqlBulkCopy);

                        transaction.Commit();
                    }
                }
            }
        }
    }
}
