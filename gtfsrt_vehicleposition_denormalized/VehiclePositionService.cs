using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;

using gtfsrt_vehicleposition_denormalized.DataAccess;

using GtfsRealtimeLib;

using log4net;
using log4net.Config;

namespace gtfsrt_vehicleposition_denormalized
{
    internal class VehiclePositionService
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly List<string> AcceptedRoutes;

        public VehiclePositionService()
        {
            var acceptedRoutes = ConfigurationManager.AppSettings["ACCEPTROUTE"].Trim();
            AcceptedRoutes = acceptedRoutes.Split(',').Where(x => !string.IsNullOrEmpty(x)).ToList();
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
            var vehiclePositions = new List<VehiclePositionData>();

            foreach (var entity in feedMessage.entity.Where(x => !AcceptedRoutes.Any() || (!string.IsNullOrEmpty(x.vehicle?.trip?.route_id) &&
                                                                                           AcceptedRoutes.Contains(x.vehicle?.trip?.route_id))))
            {
                vehiclePositions.Add(new VehiclePositionData
                                     {
                                         gtfs_realtime_version = feedMessage.header.gtfs_realtime_version,
                                         incrementality = feedMessage.header.incrementality.ToString(),
                                         header_timestamp = feedMessage.header.timestamp,
                                         feed_entity_id = entity.id,
                                         trip_id = entity.vehicle?.trip?.trip_id,
                                         route_id = entity.vehicle?.trip?.route_id,
                                         direction_id = entity.vehicle?.trip?.direction_id,
                                         trip_start_date = entity.vehicle?.trip?.start_date,
                                         trip_start_time = entity.vehicle?.trip?.start_time,
                                         trip_schedule_relationship = entity.vehicle?.trip?.schedule_relationship.ToString(),
                                         vehicle_id = entity.vehicle?.vehicle?.id,
                                         vehicle_label = entity.vehicle?.vehicle?.label,
                                         vehicle_license_plate = entity.vehicle?.vehicle?.license_plate,
                                         vehicle_timestamp = entity.vehicle?.timestamp,
                                         current_stop_sequence = entity.vehicle?.current_stop_sequence,
                                         current_status = entity.vehicle?.current_status.ToString(),
                                         stop_id = entity.vehicle?.stop_id,
                                         congestion_level = entity.vehicle?.congestion_level.ToString(),
                                         occupancy_status = null, //where to get this from?
                                         latitude = entity.vehicle?.position?.latitude,
                                         longitude = entity.vehicle?.position?.longitude,
                                         bearing = entity.vehicle?.position?.bearing,
                                         odometer = entity.vehicle?.position?.odometer,
                                         speed = entity.vehicle?.position?.speed
                                     });
            }

            InsertVehiclePositionsRows(vehiclePositions);
        }

        readonly VehiclePositionsDataSet _dataSet = new VehiclePositionsDataSet();

        private void InsertVehiclePositionsRows(List<VehiclePositionData> vehiclePositions)
        {
            if (!vehiclePositions.Any())
            {
                Log.Debug("No vehicle positions to save...");
                return;
            }

            try
            {
                Log.Debug($"Trying to insert {vehiclePositions.Count} vehicle position rows in database.");
                _dataSet.SaveData(vehiclePositions);
                Log.Debug($"Inserted {vehiclePositions.Count} vehicle position rows in database.");
            }
            catch (Exception exception)
            {
                Log.Debug($"Failed to save data: {exception.Message}");
            }
        }
        public void Stop()
        {
            Thread.Sleep(1000);
            Environment.Exit(0);
        }
    }
}
