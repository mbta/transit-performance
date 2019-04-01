using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

using GtfsRealtimeLib;

using log4net;

using ProtoBuf;

namespace gtfsrt_events_vp_current_status
{
    internal class EventRecorder
    {
        private readonly ILog Log;

        private ulong previousFileTimestamp,
                      staleFileTimestampThreshold;

        private Dictionary<VehicleEntity, Entity> EternalEntitySet;
        private readonly EventQueue eventQueue;

        /// <summary>
        /// Begin recording the events
        /// </summary>
        internal void RecordEvents()
        {
            var outputFileName = ConfigurationManager.AppSettings["FilePath"];
            var url = ConfigurationManager.AppSettings["URL"];

            var staleFileTimestampThresholdStr = ConfigurationManager.AppSettings["StaleFileTimestampThreshold"];
            staleFileTimestampThreshold = string.IsNullOrEmpty(staleFileTimestampThresholdStr) ? 300 : ulong.Parse(staleFileTimestampThresholdStr);

            var updateCycleStr = ConfigurationManager.AppSettings["Frequency"];
            var updateCycle = string.IsNullOrEmpty(updateCycleStr) ? 15 : int.Parse(updateCycleStr);
            updateCycle = updateCycle * 1000; // convert seconds to miliseconds

            while (true)
            {
                Log.Info("Start RecordEvents iteration");
                try
                {
                    DownloadFile(outputFileName, url);
                    RecordAnyNewEvents(outputFileName);
                }
                catch (Exception e)
                {
                    Log.Error($"{e.Message} {e.InnerException?.Message}");
                    Log.Error(e.StackTrace);
                }
                Log.Info("End RecordEvents iteration");
                Thread.Sleep(updateCycle);
            }
        }

        private void RecordAnyNewEvents(string outputFileName)
        {
            var feedMessages = GetFeedMessages(outputFileName);
            /*
             * Check the file time stamp, if it is same as previous one,
             * do not process any further as there is no update
             */
            var currentFileTimestamp = feedMessages.header.timestamp;

            Log.Info("currentFileTimestamp: " + currentFileTimestamp + " previousFileTimestamp: " + previousFileTimestamp);

            if (currentFileTimestamp != previousFileTimestamp)
            {
                ProcessFeedMessages(feedMessages);
                previousFileTimestamp = currentFileTimestamp;
            }
            //else timestamps are equal...if currenteFileTimestamp is more than x seconds old, then exit and
            //service will be automatically restarted...restart may fix problem with old data
            else if (GetEpochTime() - currentFileTimestamp > staleFileTimestampThreshold)
            {
                Log.Error("currentFileTimestamp is stale. currentFileTimestamp: " + currentFileTimestamp + " currentTime: " + GetEpochTime() + " Exiting now...");
                Environment.Exit(2);
            }
        }

        private void ProcessFeedMessages(FeedMessage feedMessages)
        {
            var EphemeralEntitySet = GetEntitySet(feedMessages);
            if (EternalEntitySet == null)
            {
                InitializeEternalEntitySet(EphemeralEntitySet);
            }
            else
            {
                UpdateEternalEntitySet(EphemeralEntitySet);
            }
        }

        /// <summary>
        /// Update the Eternal Entity Set. Identify any arrival or departure events
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        private void UpdateEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet)
        {
            foreach (var entry in EphemeralEntitySet)
            {
                if (EternalEntitySet.ContainsKey(entry.Key))
                {
                    UpdateEternalEntitySet(EphemeralEntitySet, entry);
                }
                else
                {
                    AddNewEntityToEternalEntitySet(entry);
                }
            }
        }

        private void AddNewEntityToEternalEntitySet(KeyValuePair<VehicleEntity, Entity> entry)
        {
            if (VehicleCurrentStopStatus.STOPPED_AT.ToString().Equals(entry.Value.currentStopStatus))
            {
                EternalEntitySet.Add(entry.Key, entry.Value);
                GenerateArrivalEvent(entry.Key);
            }
        }

        /// <summary>
        /// Idnetify if any entity is updated and generate the appropriate events
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        /// <param name="entry"></param>
        private void UpdateEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, KeyValuePair<VehicleEntity, Entity> entry)
        {
            var parameterCode = IdentifyIfEntityChanged(EternalEntitySet[entry.Key], entry.Value);
            switch (parameterCode)
            {
                case 0:
                    ProcessStopSequenceChange(EphemeralEntitySet, entry.Key);
                    break;

                case 1:
                    ProcessStopStatusChange(EphemeralEntitySet, entry.Key);
                    break;

                case 2:
                    ProcessTripChange(EphemeralEntitySet, entry.Key);
                    break;
            }
        }

        private void ProcessTripChange(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 0);
        }

        private void ProcessStopStatusChange(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            if (EternalEntitySet[vehicleEntity].departure != 1)
            {
                GenerateDepartureEvent(EphemeralEntitySet, vehicleEntity);
                if (EternalEntitySet[vehicleEntity].departure == 1 && EternalEntitySet[vehicleEntity].arrival == 1)
                {
                    UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 1);
                }
            }
        }

        private void ProcessStopSequenceChange(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            var currentStatus = EphemeralEntitySet[vehicleEntity].currentStopStatus;
            GenerateDepartureEvent(EphemeralEntitySet, vehicleEntity);
            if (EternalEntitySet[vehicleEntity].departure == 1 && EternalEntitySet[vehicleEntity].arrival == 1)
            {
                UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 1);
            }
            if (VehicleCurrentStopStatus.STOPPED_AT.ToString().Equals(currentStatus))
            {
                UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 0);
                GenerateArrivalEvent(vehicleEntity);
            }
        }

        /// <summary>
        /// It updates the entities in the eternal eneity set with the new values.
        /// Update will depend on the update type that is supplied.
        /// If update type is 1, we completely remove that entity from the set.
        /// If update type is 0, we update that entity with new value.
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        /// <param name="vehicleEntity"></param>
        /// <param name="updateType"></param>
        private void UpdateEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity, int updateType)
        {
            switch (updateType)
            {
                case 1:
                    EternalEntitySet.Remove(vehicleEntity);
                    return;

                case 0:
                    var e = new Entity(EphemeralEntitySet[vehicleEntity]);
                    EternalEntitySet[vehicleEntity] = e;
                    break;
            }
        }

        private void GenerateDepartureEvent(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            var startDate = EternalEntitySet[vehicleEntity].startDate;
            var serviceDate = DateTime.ParseExact(startDate, "yyyyMMdd", CultureInfo.InvariantCulture);
            var routeId = EphemeralEntitySet[vehicleEntity].routeId;
            var tripId = EphemeralEntitySet[vehicleEntity].tripId;
            var stopId = EternalEntitySet[vehicleEntity].stopId;
            var stopSequence = EternalEntitySet[vehicleEntity].stopSequence;
            var vehicleId = vehicleEntity.VehicleId;
            var vehicleLabel = vehicleEntity.VehicleLabel;
            var eventType = EventType.DEP;
            var actualTime = EphemeralEntitySet[vehicleEntity].vehicletimeStamp;
            var fileTimestamp = EphemeralEntitySet[vehicleEntity].fileTimestamp;
            var directionId = EphemeralEntitySet[vehicleEntity].directionId;
            var newEvent = new Event(serviceDate,
                                     routeId,
                                     tripId,
                                     stopId,
                                     stopSequence,
                                     vehicleId,
                                     vehicleLabel,
                                     eventType,
                                     actualTime,
                                     fileTimestamp,
                                     directionId);
            var eventString = newEvent.ToString();
            EternalEntitySet[vehicleEntity].departure = 1;
            Log.Info(eventString);
            eventQueue.Enqueue(newEvent);
        }

        private int IdentifyIfEntityChanged(Entity entity1, Entity entity2)
        {
            if (entity1.stopSequence != (entity2.stopSequence))
            {
                return 0;
            }
            if (!entity1.currentStopStatus.Equals(entity2.currentStopStatus))
            {
                return 1;
            }
            if (!entity1.tripId.Equals(entity2.tripId))
            {
                return 2;
            }
            return -1;
        }

        /// <summary>
        /// Initialize the Eteranal Entity set for the first run.
        /// Also generate the arrival events
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        private void InitializeEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet)
        {
            EternalEntitySet = new Dictionary<VehicleEntity, Entity>();
            foreach (var entry in EphemeralEntitySet.Where(entry => VehicleCurrentStopStatus.STOPPED_AT.ToString().Equals(entry.Value.currentStopStatus)))
            {
                EternalEntitySet.Add(entry.Key, entry.Value);
                GenerateArrivalEvent(entry.Key);
                EternalEntitySet[entry.Key].arrival = 1;
            }
        }

        /// <summary>
        /// Returns a customized Entity set from the feed messages
        /// </summary>
        /// <param name="feedMessages"></param>
        /// <returns></returns>
        private Dictionary<VehicleEntity, Entity> GetEntitySet(FeedMessage feedMessages)
        {
            var entityFactory = new EntityFactory();
            var entitySet = entityFactory.ProduceEntites(feedMessages);
            return entitySet;
        }

        /// <summary>
        /// This method deserialize the .pb file to generate FeedMessage object
        /// </summary>
        /// <param name="outputFileName"></param>
        /// <returns></returns>
        private FeedMessage GetFeedMessages(string outputFileName)
        {
            FeedMessage feedMessages;
            using (var file = File.OpenRead(outputFileName))
            {
                feedMessages = Serializer.Deserialize<FeedMessage>(file);
            }
            return feedMessages;
        }

        /// <summary>
        /// Download the file from the url specified url to the output file.
        /// </summary>
        /// <param name="outputFileName"></param>
        /// <param name="Url"></param>
        private void DownloadFile(string outputFileName, string Url)
        {
            using (var Client = new WebClient())
            {
                Client.DownloadFile(Url, outputFileName);
            }
        }

        private void GenerateArrivalEvent(VehicleEntity vehicleEntity)
        {
            //DateTime serviceDate = DateTime.Now;
            var startDate = EternalEntitySet[vehicleEntity].startDate;
            var serviceDate = DateTime.ParseExact(startDate, "yyyyMMdd", CultureInfo.InvariantCulture);
            var routeId = EternalEntitySet[vehicleEntity].routeId;
            var tripId = EternalEntitySet[vehicleEntity].tripId;
            var stopId = EternalEntitySet[vehicleEntity].stopId;
            var stopSequence = EternalEntitySet[vehicleEntity].stopSequence;
            var vehicleId = vehicleEntity.VehicleId;
            var vehicleLabel = vehicleEntity.VehicleLabel;
            var eventType = EventType.ARR;
            var actualTime = EternalEntitySet[vehicleEntity].vehicletimeStamp;
            var fileTimestamp = EternalEntitySet[vehicleEntity].fileTimestamp;
            var directionId = EternalEntitySet[vehicleEntity].directionId;
            var newEvent = new Event(serviceDate,
                                     routeId,
                                     tripId,
                                     stopId,
                                     stopSequence,
                                     vehicleId,
                                     vehicleLabel,
                                     eventType,
                                     actualTime,
                                     fileTimestamp,
                                     directionId);
            var eventString = newEvent.ToString();
            EternalEntitySet[vehicleEntity].arrival = 1;
            Log.Info(eventString);
            eventQueue.Enqueue(newEvent);
        }

        private static ulong GetEpochTime()
        {
            var epochPoint = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var epoch = (ulong) DateTime.Now.ToUniversalTime().Subtract(epochPoint).TotalSeconds;
            return epoch;
        }

        public EventRecorder(EventQueue eventQueue, ILog Log)
        {
            // TODO: Complete member initialization
            this.eventQueue = eventQueue;
            this.Log = Log;
        }
    }
}