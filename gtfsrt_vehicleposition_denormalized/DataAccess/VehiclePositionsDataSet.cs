using System.Collections.Generic;
using System.Configuration;

using IBI.DataAccess.Models;

namespace gtfsrt_vehicleposition_denormalized.DataAccess
{
    partial class VehiclePositionsDataSet
    {
        private readonly string SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();

        internal void SaveData(List<VehiclePositionData> vehiclePositions)
        {
            var dataTable = new gtfsrt_vehicleposition_denormalizedDataTable();

            foreach (var vehiclePosition in vehiclePositions)
            {
                var newRow = dataTable.Newgtfsrt_vehicleposition_denormalizedRow();

                newRow.gtfs_realtime_version = vehiclePosition.gtfs_realtime_version;
                newRow.incrementality = vehiclePosition.incrementality;
                newRow.header_timestamp = (int)vehiclePosition.header_timestamp;
                newRow.feed_entity_id = vehiclePosition.feed_entity_id;
                newRow.trip_id = vehiclePosition.trip_id;
                newRow.route_id = vehiclePosition.route_id;
                if (vehiclePosition.direction_id.HasValue)
                    newRow.direction_id = (int)vehiclePosition.direction_id.Value;
                newRow.trip_start_date = vehiclePosition.trip_start_date;
                if (!string.IsNullOrEmpty(vehiclePosition.trip_start_time))
                    newRow.trip_start_time = vehiclePosition.trip_start_time;
                newRow.trip_schedule_relationship = vehiclePosition.trip_schedule_relationship;
                if (!string.IsNullOrEmpty(vehiclePosition.vehicle_id))
                    newRow.vehicle_id = vehiclePosition.vehicle_id;
                if (!string.IsNullOrEmpty(vehiclePosition.vehicle_label))
                    newRow.vehicle_label = vehiclePosition.vehicle_label;
                if (!string.IsNullOrEmpty(vehiclePosition.vehicle_license_plate))
                    newRow.vehicle_license_plate = vehiclePosition.vehicle_license_plate;
                if (vehiclePosition.vehicle_timestamp.HasValue)
                    newRow.vehicle_timestamp = (int)vehiclePosition.vehicle_timestamp.Value;
                if (vehiclePosition.current_stop_sequence.HasValue)
                    newRow.current_stop_sequence = (int)vehiclePosition.current_stop_sequence.Value;
                if (!string.IsNullOrEmpty(vehiclePosition.current_status))
                    newRow.current_status = vehiclePosition.current_status;
                if (!string.IsNullOrEmpty(vehiclePosition.stop_id))
                    newRow.stop_id = vehiclePosition.stop_id;
                if (!string.IsNullOrEmpty(vehiclePosition.congestion_level))
                    newRow.congestion_level = vehiclePosition.congestion_level;
                if (!string.IsNullOrEmpty(vehiclePosition.occupancy_status))
                    newRow.occupancy_status = vehiclePosition.occupancy_status;
                if (vehiclePosition.latitude.HasValue)
                    newRow.latitude = vehiclePosition.latitude.Value;
                if (vehiclePosition.longitude.HasValue)
                    newRow.longitude = vehiclePosition.longitude.Value;
                if (vehiclePosition.bearing.HasValue)
                    newRow.bearing = vehiclePosition.bearing.Value;
                if (vehiclePosition.odometer.HasValue)
                    newRow.odometer = vehiclePosition.odometer.Value;
                if (vehiclePosition.speed.HasValue)
                    newRow.speed = vehiclePosition.speed.Value;

                dataTable.Rows.Add(newRow);
            }

            dataTable.TableName = "dbo.gtfsrt_vehicleposition_denormalized";
            if (dataTable.Rows.Count <= 0)
                return;

            Utils.BulkInsert(dataTable, SqlConnectionString);
        }
    }
}
