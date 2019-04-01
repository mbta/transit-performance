using System;

namespace gtfsrt_events_tu_latest_prediction
{
    internal class EntityIdentifier
    {
        private readonly string TripId;
        private readonly uint StopSequence;

        private DateTime ServiceDate;
        private readonly EventType EventType;

        public EntityIdentifier(string tripId, uint stopSequence, DateTime serviceDate, EventType eventType)
        {
            // TODO: Complete member initialization
            TripId = tripId;
            StopSequence = stopSequence;
            ServiceDate = serviceDate;
            EventType = eventType;
        }

        /*
         * Two entities are said to be equal if their trip id, 
         * stop sequence, service date, and event type are the same
         * */

        public override bool Equals(object obj)
        {
            var item = obj as EntityIdentifier;
            if (item == null)
                return false;

            // Null check for trip id.
            // Ideally this condition never happens. In case it happens return false
            if (string.IsNullOrEmpty(TripId) || string.IsNullOrEmpty(item.TripId))
                return false;

            return TripId.Equals(item.TripId)
                   && StopSequence == item.StopSequence
                   && item.ServiceDate.Equals(ServiceDate)
                   && EventType.Equals(item.EventType);
        }

        /*
         * This method return hashcode for a given entity.
         * An entity key is composed of trip id, stop sequence, service date, and event type
         * */

        public override int GetHashCode()
        {
            var EventTypeString = EventType == EventType.PRA ? "PRA" : "PRD";
            var entityKey = TripId + StopSequence + ServiceDate.ToShortDateString() + EventTypeString;
            return entityKey.GetHashCode();
        }

        public override string ToString()
        {
            var EventTypeString = EventType == EventType.PRA ? "PRA" : "PRD";
            var entityString = TripId + "-" + StopSequence + "-" + ServiceDate.ToShortDateString() + "-" + EventTypeString;
            return entityString;
        }
    }
}
