namespace gtfsrt_events_vp_current_status
{
    internal class Entity
    {
        public ulong fileTimestamp;
        public string tripId;
        public string routeId;
        public string stopId;
        public uint stopSequence;
        public string currentStopStatus;
        public ulong vehicletimeStamp;
        public int arrival;
        public int departure;
        public string startDate;
        public uint? directionId;

        public Entity(string tripId,
                      string routeId,
                      string stopId,
                      uint stopSequence,
                      string currentStopStatus,
                      ulong vehicletimeStamp,
                      ulong fileStamp,
                      string startDate,
                      uint? directionId)
        {
            this.tripId = tripId;
            this.routeId = routeId;
            this.stopId = stopId;
            this.stopSequence = stopSequence;
            this.currentStopStatus = currentStopStatus;
            this.vehicletimeStamp = vehicletimeStamp;
            arrival = 0;
            departure = 0;
            fileTimestamp = fileStamp;
            this.startDate = startDate;
            this.directionId = directionId;
        }

        public Entity()
        {
            // TODO: Complete member initialization
        }

        public Entity(Entity entity)
        {
            // TODO: Complete member initialization
            tripId = entity.tripId;
            routeId = entity.routeId;
            stopId = entity.stopId;
            stopSequence = entity.stopSequence;
            currentStopStatus = entity.currentStopStatus;
            vehicletimeStamp = entity.vehicletimeStamp;
            arrival = entity.arrival;
            departure = entity.departure;
            fileTimestamp = entity.fileTimestamp;
            startDate = entity.startDate;
            directionId = entity.directionId;
        }

        public override bool Equals(object obj)
        {
            var item = obj as Entity;
            if (item == null)
            {
                return false;
            }
            var tripIdFlag = item.tripId.Equals(tripId);
            var stopSequenceFlag = item.stopSequence == stopSequence;
            var currentStopStatusFlag = item.currentStopStatus.Equals(currentStopStatus);
            return (tripIdFlag && stopSequenceFlag && currentStopStatusFlag);
        }

        public override int GetHashCode()
        {
            return (tripId + stopSequence + currentStopStatus).GetHashCode();
        }
    }
}