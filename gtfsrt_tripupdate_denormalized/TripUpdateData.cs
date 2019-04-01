namespace gtfsrt_tripupdate_denormalized
{
    internal class TripUpdateData
    {
        private string gtfs_realtime_version;
        private string incrementality;
        private ulong header_timestamp;
        private string feed_entity_id;
        private string trip_id;
        private int? trip_delay;
        private string route_id;
        private uint? direction_id;
        private string trip_start_date;
        private string trip_start_time;
        private string trip_schedule_relationship;
        private string vehicle_id;
        private string vehicle_label;
        private string vehicle_license_plate;
        private ulong? vehicle_timestamp;
        private string stop_id;
        private uint stop_sequence;
        private long? predicted_arrival_time;
        private int? predicted_arrival_delay;
        private int? predicted_arrival_uncertainty;
        private long? predicted_departure_time;
        private int? predicted_departure_delay;
        private int? predicted_departure_uncertainty;
        private string stop_schedule_relationship;

        public string GtfsRealtimeVersion
        {
            get { return gtfs_realtime_version; }
            set { gtfs_realtime_version = value; }
        }

        public string Incrementality
        {
            get { return incrementality; }
            set { incrementality = value; }
        }

        public ulong HeaderTimestamp
        {
            get { return header_timestamp; }
            set { header_timestamp = value; }
        }

        public string FeedEntityId
        {
            get { return feed_entity_id; }
            set { feed_entity_id = value; }
        }

        public string TripId
        {
            get { return trip_id; }
            set { trip_id = value; }
        }

        public int? TripDelay
        {
            get { return trip_delay; }
            set { trip_delay = value; }
        }

        public string RouteId
        {
            get { return route_id; }
            set { route_id = value; }
        }

        public uint? DirectionId
        {
            get { return direction_id; }
            set { direction_id = value; }
        }

        public string TripStartDate
        {
            get { return trip_start_date; }
            set { trip_start_date = value; }
        }

        public string TripStartTime
        {
            get { return trip_start_time; }
            set { trip_start_time = value; }
        }

        public string TripScheduleRelationship
        {
            get { return trip_schedule_relationship; }
            set { trip_schedule_relationship = value; }
        }

        public string VehicleId
        {
            get { return vehicle_id; }
            set { vehicle_id = value; }
        }

        public string VehicleLabel
        {
            get { return vehicle_label; }
            set { vehicle_label = value; }
        }

        public string VehicleLicensePlate
        {
            get { return vehicle_license_plate; }
            set { vehicle_license_plate = value; }
        }

        public ulong? VehicleTimestamp
        {
            get { return vehicle_timestamp; }
            set { vehicle_timestamp = value; }
        }

        public string StopId
        {
            get { return stop_id; }
            set { stop_id = value; }
        }

        public uint StopSequence
        {
            get { return stop_sequence; }
            set { stop_sequence = value; }
        }

        public long? PredictedArrivalTime
        {
            get { return predicted_arrival_time; }
            set { predicted_arrival_time = value; }
        }

        public int? PredictedArrivalDelay
        {
            get { return predicted_arrival_delay; }
            set { predicted_arrival_delay = value; }
        }

        public int? PredictedArrivalUncertainty
        {
            get { return predicted_arrival_uncertainty; }
            set { predicted_arrival_uncertainty = value; }
        }

        public long? PredictedDepartureTime
        {
            get { return predicted_departure_time; }
            set { predicted_departure_time = value; }
        }

        public int? PredictedDepartureDelay
        {
            get { return predicted_departure_delay; }
            set { predicted_departure_delay = value; }
        }

        public int? PredictedDepartureUncertainty
        {
            get { return predicted_departure_uncertainty; }
            set { predicted_departure_uncertainty = value; }
        }

        public string StopScheduleRelationship
        {
            get { return stop_schedule_relationship; }
            set { stop_schedule_relationship = value; }
        }
    }
}
