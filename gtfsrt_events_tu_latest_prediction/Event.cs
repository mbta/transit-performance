using System;

namespace gtfsrt_events_tu_latest_prediction
{
    /*
     * This class represent an trip event.
     * Class members have near correspondence to columns 
     * in database table.
     * */

    internal class Event
    {
        internal DateTime ServiceDate;
        internal readonly string RouteId;
        internal readonly string TripId;
        internal readonly string StopId;
        internal readonly uint StopSequence;
        internal readonly string VehicleId;
        internal readonly string VehicleLabel;
        internal readonly EventType _EventType;
        internal readonly long EventTime;
        internal readonly ulong FileTimestamp;
        internal uint? DirectionId;

        internal Event()
        {
            // Default Constructor
        }

        /*
         * Parameterized constructor.
         * */

        internal Event(DateTime serviceDate,
                       string routeId,
                       string tripId,
                       string stopId,
                       uint stopSequence,
                       string vehicleId,
                       EventType eventType,
                       long eventTime,
                       ulong fileTimestamp,
                       uint? directionId,
                       string vehicleLabel)
        {
            ServiceDate = serviceDate;
            RouteId = routeId;
            TripId = tripId;
            StopId = stopId;
            StopSequence = stopSequence;
            VehicleId = vehicleId;
            _EventType = eventType;
            EventTime = eventTime;
            FileTimestamp = fileTimestamp;
            DirectionId = directionId;
            VehicleLabel = vehicleLabel;
        }

        internal string GetEventIdentifier()
        {
            return TripId + "-" + StopSequence;
        }
    }

    public enum EventType
    {
        PRA = 0,
        PRD = 1,
    }
}