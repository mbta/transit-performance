namespace gtfsrt_vehicleposition_denormalized
{
    internal class VehiclePositionData
    {
        public string gtfs_realtime_version { get; set; }
        public string incrementality { get; set; }
        public ulong header_timestamp { get; set; }
        public string feed_entity_id { get; set; }
        public string trip_id { get; set; }
        public string route_id { get; set; }
        public uint? direction_id { get; set; }
        public string trip_start_date { get; set; }
        public string trip_start_time { get; set; }
        public string trip_schedule_relationship { get; set; }
        public string vehicle_id { get; set; }
        public string vehicle_label { get; set; }
        public string vehicle_license_plate { get; set; }
        public ulong? vehicle_timestamp { get; set; }
        public uint? current_stop_sequence { get; set; }
        public string current_status { get; set; }
        public string stop_id { get; set; }
        public string congestion_level { get; set; }
        public string occupancy_status { get; set; }
        public double? latitude { get; set; }
        public double? longitude { get; set; }
        public double? bearing { get; set; }
        public double? odometer { get; set; }
        public double? speed { get; set; }
    }
}
