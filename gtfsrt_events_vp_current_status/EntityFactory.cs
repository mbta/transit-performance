using System.Collections.Generic;
using System.Configuration;

using GtfsRealtimeLib;

namespace gtfsrt_events_vp_current_status
{
    internal class EntityFactory
    {
        internal Dictionary<VehicleEntity, Entity> ProduceEntites(FeedMessage feedMessages)
        {
            var feedEntityList = feedMessages.entity;
            var vehicleEntitySet = new Dictionary<VehicleEntity, Entity>();

            var includeEntitiesWithoutTrip = ConfigurationManager.AppSettings["IncludeEntitiesWithoutTrip"].ToUpper();

            foreach (var feedEntity in feedEntityList)
            {
                //if a trip id exists for this entity or if config parameter says to include entities without a trip id 
                //then do the following...else skip (discard) this entity
                if (feedEntity.vehicle.trip == null && !"TRUE".Equals(includeEntitiesWithoutTrip))
                    continue;

                var currentStopStatus = feedEntity.vehicle.current_status.ToString();
                var tripId = feedEntity.vehicle?.trip?.trip_id;
                var routeId = feedEntity.vehicle?.trip?.route_id;
                var stopId = feedEntity.vehicle.stop_id;
                var stopSequence = feedEntity.vehicle.current_stop_sequence;
                var vehicletimeStamp = feedEntity.vehicle.timestamp;
                var VehicleId = feedEntity.vehicle.vehicle.id;
                var VehicleLabel = feedEntity.vehicle.vehicle.label;
                var fileStamp = feedMessages.header.timestamp;
                var startDate = feedEntity.vehicle?.trip?.start_date;
                var directionId = feedEntity.vehicle?.trip?.direction_id;
                var entity = new Entity(tripId, routeId, stopId, stopSequence, currentStopStatus, vehicletimeStamp, fileStamp, startDate, directionId);
                var vehicleEntity = new VehicleEntity
                                    {
                                        VehicleId = VehicleId,
                                        VehicleLabel = VehicleLabel,
                                        tripId = feedEntity.vehicle?.trip?.trip_id
                                    };
                if (vehicleEntitySet.ContainsKey(vehicleEntity))
                {
                    vehicleEntitySet.Remove(vehicleEntity);
                }
                vehicleEntitySet.Add(vehicleEntity, entity);
            }
            return vehicleEntitySet;
        }
    }
}