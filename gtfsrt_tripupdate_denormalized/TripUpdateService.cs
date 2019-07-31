using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;

using gtfsrt_tripupdate_denormalized.DataAccess;

using GtfsRealtimeLib;

using log4net;
using log4net.Config;

namespace gtfsrt_tripupdate_denormalized
{
    public class TripUpdateService
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly List<string> AcceptedRoutes;

        public TripUpdateService()
        {
            var acceptedRoutes = ConfigurationManager.AppSettings["ACCEPTROUTE"].Trim();
            AcceptedRoutes = string.IsNullOrEmpty(acceptedRoutes) ? new List<string>()
                : acceptedRoutes.Split(',').Where(x => !string.IsNullOrEmpty(x)).ToList();
        }

        public void Start()
        {
            try
            {
                XmlConfigurator.Configure();
                Log.Info("Program started");

                ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                Log.Info($"Enabled Protocols: {ServicePointManager.SecurityProtocol}");

                Thread.Sleep(1000);

                var data = new GtfsData(Log);
                data.NewFeedMessage += Data_NewFeedMessage;

                var feedMessageThread = new Thread(data.GetData);
                feedMessageThread.Start();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.InnerException);
                Log.Error(e.StackTrace);

                Thread.Sleep(1000);
                Environment.Exit(1);
            }
        }
        
        private void Data_NewFeedMessage(FeedMessage feedMessage)
        {
            var tripUpdates = new List<TripUpdateData>();

            foreach (var entity in feedMessage.entity.Where(x => !AcceptedRoutes.Any() || (!string.IsNullOrEmpty(x.trip_update?.trip?.route_id) &&
                                                                 AcceptedRoutes.Contains(x.trip_update?.trip?.route_id))))
            {
                tripUpdates.AddRange(entity.trip_update.stop_time_update
                                           .Select(stopTimeUpdate => new TripUpdateData
                                                                     {
                                                                         GtfsRealtimeVersion = feedMessage.header.gtfs_realtime_version,
                                                                         Incrementality = feedMessage.header.incrementality.ToString(),
                                                                         HeaderTimestamp = feedMessage.header.timestamp,
                                                                         FeedEntityId = entity.id,
                                                                         VehicleTimestamp = entity.trip_update?.timestamp,
                                                                         //TripDelay = ..., TripDelay not applicable
                                                                         TripId = entity.trip_update?.trip?.trip_id,
                                                                         TripStartDate = entity.trip_update?.trip?.start_date,
                                                                         TripStartTime = entity.trip_update?.trip?.start_time,
                                                                         RouteId = entity.trip_update?.trip?.route_id,
                                                                         DirectionId = entity.trip_update?.trip?.direction_id,
                                                                         TripScheduleRelationship =
                                                                             entity.trip_update?.trip?.schedule_relationship.ToString(),
                                                                         StopSequence = stopTimeUpdate.stop_sequence,
                                                                         StopId = stopTimeUpdate.stop_id,
                                                                         StopScheduleRelationship = stopTimeUpdate.schedule_relationship.ToString(),
                                                                         PredictedArrivalTime = stopTimeUpdate.arrival?.time,
                                                                         PredictedArrivalDelay = stopTimeUpdate.arrival?.delay,
                                                                         PredictedArrivalUncertainty = stopTimeUpdate.arrival?.uncertainty,
                                                                         PredictedDepartureTime = stopTimeUpdate.departure?.time,
                                                                         PredictedDepartureDelay = stopTimeUpdate.departure?.delay,
                                                                         PredictedDepartureUncertainty = stopTimeUpdate.departure?.uncertainty,
                                                                         VehicleId = entity.trip_update?.vehicle?.id,
                                                                         VehicleLabel = entity.trip_update?.vehicle?.label,
                                                                         VehicleLicensePlate = entity.trip_update?.vehicle?.license_plate
                                                                     }));
            }

            InsertTripUpdatesRows(tripUpdates);
        }

        public void Stop()
        {
            Thread.Sleep(1000);
            Environment.Exit(0);}

        readonly TripUpdateDataSet _tripUpdateDataSet = new TripUpdateDataSet();

        private void InsertTripUpdatesRows(List<TripUpdateData> tripUpdates)
        {
            if (!tripUpdates.Any())
            {
                Log.Debug("No trip updates to save...");
                return;
            }

            try
            {
                Log.Debug($"Trying to insert {tripUpdates.Count} trip update rows in database.");
                _tripUpdateDataSet.SaveTripUpdates(tripUpdates);
                Log.Debug($"Inserted {tripUpdates.Count} trip update rows in database.");
            }
            catch (Exception exception)
            {
                Log.Debug($"Failed to save data: {exception.Message}");
            }
        }
    }
}
