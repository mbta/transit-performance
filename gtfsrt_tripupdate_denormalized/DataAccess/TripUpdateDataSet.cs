using System.Collections.Generic;
using System.Configuration;

using IBI.DataAccess.Models;

namespace gtfsrt_tripupdate_denormalized.DataAccess
{
    partial class TripUpdateDataSet
    {
        private readonly string SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();

        internal void SaveTripUpdates(List<TripUpdateData> tripUpdates)
        {
            var dataTable = new GtfsRtTripUpdateDenormalizedDataTable();

            foreach (var tripUpdate in tripUpdates)
            {
                var newRow = dataTable.NewGtfsRtTripUpdateDenormalizedRow();

                newRow.gtfs_realtime_version = tripUpdate.GtfsRealtimeVersion;
                newRow.incrementality = tripUpdate.Incrementality;
                newRow.header_timestamp = (int)tripUpdate.HeaderTimestamp;
                newRow.feed_entity_id = tripUpdate.FeedEntityId;
                newRow.trip_id = tripUpdate.TripId;
                if (tripUpdate.TripDelay.HasValue)
                    newRow.trip_delay = tripUpdate.TripDelay.Value;
                newRow.route_id = tripUpdate.RouteId;
                if (tripUpdate.DirectionId.HasValue)
                    newRow.direction_id = (int)tripUpdate.DirectionId.Value;
                newRow.trip_start_date = tripUpdate.TripStartDate;
                if (!string.IsNullOrEmpty(tripUpdate.TripStartTime))
                    newRow.trip_start_time = tripUpdate.TripStartTime;
                newRow.trip_schedule_relationship = tripUpdate.TripScheduleRelationship;
                if (!string.IsNullOrEmpty(tripUpdate.VehicleId))
                    newRow.vehicle_id = tripUpdate.VehicleId;
                if (!string.IsNullOrEmpty(tripUpdate.VehicleLabel))
                    newRow.vehicle_label = tripUpdate.VehicleLabel;
                if (!string.IsNullOrEmpty(tripUpdate.VehicleLicensePlate))
                    newRow.vehicle_license_plate = tripUpdate.VehicleLicensePlate;
                if (tripUpdate.VehicleTimestamp.HasValue)
                    newRow.vehicle_timestamp = (int)tripUpdate.VehicleTimestamp.Value;
                newRow.stop_id = tripUpdate.StopId;
                newRow.stop_sequence = (int)tripUpdate.StopSequence;
                if (tripUpdate.PredictedArrivalTime.HasValue)
                    newRow.predicted_arrival_time = (int)tripUpdate.PredictedArrivalTime.Value;
                if (tripUpdate.PredictedArrivalDelay.HasValue)
                    newRow.predicted_arrival_delay = tripUpdate.PredictedArrivalDelay.Value;
                if (tripUpdate.PredictedArrivalUncertainty.HasValue)
                    newRow.predicted_arrival_uncertainty = tripUpdate.PredictedArrivalUncertainty.Value;
                if (tripUpdate.PredictedDepartureTime.HasValue)
                    newRow.predicted_departure_time = (int)tripUpdate.PredictedDepartureTime.Value;
                if (tripUpdate.PredictedDepartureDelay.HasValue)
                    newRow.predicted_departure_delay = tripUpdate.PredictedDepartureDelay.Value;
                if (tripUpdate.PredictedDepartureUncertainty.HasValue)
                    newRow.predicted_departure_uncertainty = tripUpdate.PredictedDepartureUncertainty.Value;
                newRow.stop_schedule_relationship = tripUpdate.StopScheduleRelationship;

                dataTable.Rows.Add(newRow);
            }

            dataTable.TableName = "dbo.gtfsrt_tripupdate_denormalized";
            if (dataTable.Rows.Count <= 0)
                return;

            Utils.BulkInsert(dataTable, SqlConnectionString);
        }
    }
}
