using System;
using System.Text;

namespace gtfsrt_events_vp_current_status
{
    internal class Event
    {
        public DateTime serviceDate;
        public readonly string routeId;
        public readonly string tripId;
        public readonly string stopId;
        public readonly uint stopSequence;
        public readonly string vehicleId;
        public readonly string vehicleLabel;
        public readonly EventType eventType;
        public readonly ulong eventTime;
        public readonly ulong fileTimestamp;
        public uint? directionId;

        public override string ToString()
        {
            return $"{serviceDate.ToShortDateString()}|{routeId}|{tripId}|{stopId}|{stopSequence}|" +
                   $"{vehicleId}|{eventType}|{eventTime}|{fileTimestamp}|{directionId}";
        }

        public string ToSwtring()
        {
            var sbr = new StringBuilder();
            sbr.Append("Service Date: ");
            sbr.Append(serviceDate.ToShortDateString());
            sbr.Append(" Route ID: ");
            sbr.Append(routeId);
            sbr.Append(" Trip ID: ");
            sbr.Append(tripId);
            sbr.Append(" Stop ID: ");
            sbr.Append(stopId);
            sbr.Append(" Stop Sequence: ");
            sbr.Append(stopSequence);
            sbr.Append(" Vehicle ID: ");
            sbr.Append(vehicleId);
            sbr.Append(" Event Type: ");
            sbr.Append(eventType);
            sbr.Append(" Time: ");
            sbr.Append(eventTime);
            return sbr.ToString();
        }

        public Event(DateTime serviceDate,
                     string routeId,
                     string tripId,
                     string stopId,
                     uint stopSequence,
                     string vehicleId,
                     string vehicleLabel,
                     EventType eventType,
                     ulong actualTime,
                     ulong fileTimestamp1,
                     uint? directionId)
        {
            // TODO: Complete member initialization
            this.serviceDate = serviceDate;
            this.routeId = routeId;
            this.tripId = tripId;
            this.stopId = stopId;
            this.stopSequence = stopSequence;
            this.vehicleId = vehicleId;
            this.vehicleLabel = vehicleLabel;
            this.eventType = eventType;
            eventTime = actualTime;
            fileTimestamp = fileTimestamp1;
            this.directionId = directionId;
        }
    }

    public enum EventType
    {
        ARR = 0,
        DEP = 1,
    }
}