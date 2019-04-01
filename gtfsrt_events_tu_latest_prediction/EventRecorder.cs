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

using Newtonsoft.Json;

using ProtoBuf;

namespace gtfsrt_events_tu_latest_prediction
{
    /*
     * This class operates on entities and produces events.
     * */

    internal class EventRecorder
    {
        private Dictionary<EntityIdentifier, Entity> CurrentEntities;
        private readonly ILog Log;

        private readonly BlockingQueue<Event> UpdateQueue;
        private readonly BlockingQueue<Event> InsertQueue;

        private ulong FileTimestamp;

        private readonly List<string> AcceptList = new List<string>();

        public EventRecorder(BlockingQueue<Event> InsertEventQueue, BlockingQueue<Event> UpdateEventQueue, ILog Log)
        {
            // TODO: Complete member initialization
            InsertQueue = InsertEventQueue;
            UpdateQueue = UpdateEventQueue;

            this.Log = Log;
        }

        internal void RecordEvents()
        {
            var outputFileName = GetOutputFileName();
            var url = GetUrl();
            var cycleTime = GetCycleTime();
            CreateAcceptList();
            // Above part is executed only once when thread is started.

            while (true)
            {
                try
                {
                    DownloadFile(outputFileName, url);

                    var doReset = IsResetTime();
                    if (doReset)
                        DoReset();
                    else
                    {
                        RecordEvents(outputFileName);
                    }
                }

                catch (Exception e)
                {
                    Log.Debug("Something went wrong in Event recorder. Look below for message and stack trace.");
                    while (e != null)
                    {
                        Log.Error(e.Message);
                        e = e.InnerException;
                    }
                }

                // Pause this thread for cycleTime
                Thread.Sleep(cycleTime);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        private void CreateAcceptList()
        {
            var tem = ConfigurationManager.AppSettings["ACCEPTROUTE"];
            tem = tem.Trim();
            var arr = tem.Split(',');

            foreach (var x in arr)
            {
                AcceptList.Add(x);
            }
        }

        private void RecordEvents(string outputFileName)
        {
            Log.Debug("Begin RecordEvents");
            var feedMessage = GetFeedMesssages(outputFileName);
            if (feedMessage == null)
                return;
            WriteFeedMessageToFile(feedMessage);

            var fileTimestamp = feedMessage.header.timestamp;
            if (fileTimestamp == FileTimestamp)
            {
                Log.Info("Current file has same timestamp as previous one.");
                return;
            }

            ProcessFeedMessages(feedMessage);
            FileTimestamp = fileTimestamp;
            Log.Debug("End RecordEvents");
        }

        private void ProcessFeedMessages(FeedMessage feedMessage)
        {
            if (CurrentEntities == null)
            {
                CurrentEntities = GetEntites(feedMessage);
                GenerateEvents(CurrentEntities);
            }
            else
            {
                var newEntities = GetEntites(feedMessage);
                ProcessNewEvents(newEntities);
            }
        }

        /*
         * Here we do two things. First we find, which events will be updated.
         * Second update the events in Current Entities.
         * */

        private void ProcessNewEvents(Dictionary<EntityIdentifier, Entity> newEntities)
        {
            foreach (var entity in newEntities)
            {
                if (CurrentEntities.ContainsKey(entity.Key))
                {
                    // see if stop time has changed. if stop has change it means there is change in 
                    // prediction. we need to update it in database. so put that event in update 
                    // queue.
                    if (CurrentEntities[entity.Key].EventTime != entity.Value.EventTime)
                    {
                        UpdateEvent(entity.Value);
                    }
                    CurrentEntities[entity.Key] = entity.Value; // This is where magic happens
                }
                else
                {
                    // if current entities do not have new one, add that entity to current list
                    CurrentEntities.Add(entity.Key, entity.Value);
                    // also generate an insert event for new one.
                    InsertEvent(entity.Value);
                }
            }
        }

        private void InsertEvent(Entity entity)
        {
            var eventType = entity.GetEventType();
            if (eventType.Equals(EventType.PRA))
                InsertArrivalEvent(entity);
            else
                InsertDepartureEvent(entity);
        }

        private void UpdateEvent(Entity entity)
        {
            var eventType = entity.GetEventType();
            if (eventType.Equals(EventType.PRA))
                UpdateArrivalEvent(entity);
            else
                UpdateDepartureEvent(entity);
        }

        private void UpdateArrivalEvent(Entity entity)
        {
            var _event = CreateEvent(entity);
            UpdateQueue.Enqueue(_event);
        }

        private void UpdateDepartureEvent(Entity entity)
        {
            var _event = CreateEvent(entity);
            UpdateQueue.Enqueue(_event);
        }

        private Dictionary<EntityIdentifier, Entity> GetEntites(FeedMessage feedMessage)
        {
            var entities = new Dictionary<EntityIdentifier, Entity>();
            var feedEntities = feedMessage.entity;

            foreach (var feedEntity in feedEntities)
            {
                if (AcceptList.Any(x => !string.IsNullOrEmpty(x)) && !AcceptList.Contains(feedEntity.trip_update.trip.route_id))
                    continue;

                var tripId = feedEntity.trip_update.trip.trip_id;
                var vehicleId = feedEntity.trip_update.vehicle?.id;
                var vehicleLabel = feedEntity.trip_update.vehicle?.label;
                var routeId = feedEntity.trip_update.trip.route_id;
                var _serviceDate = feedEntity.trip_update.trip.start_date;
                var serviceDate = DateTime.ParseExact(_serviceDate, "yyyyMMdd", CultureInfo.InvariantCulture);
                var fileTimestamp = feedMessage.header.timestamp;
                var directionId = feedEntity.trip_update.trip.direction_id;

                foreach (var stop in feedEntity.trip_update.stop_time_update)
                {
                    var stopId = stop.stop_id;
                    var stopSequence = stop.stop_sequence;

                    //add arrival entity
                    if (stop.arrival != null)
                    {
                        var eventTimeArrival = stop.arrival.time;
                        const EventType eventType = EventType.PRA;
                        var entity = new Entity(serviceDate,
                                                routeId,
                                                tripId,
                                                stopId,
                                                stopSequence,
                                                vehicleId,
                                                eventType,
                                                eventTimeArrival,
                                                fileTimestamp,
                                                directionId,
                                                vehicleLabel);
                        var eId = new EntityIdentifier(tripId, stopSequence, serviceDate, eventType);

                        try
                        {

                            if (entities.ContainsKey(eId))
                                entities.Remove(eId);
                            entities.Add(eId, entity);
                        }
                        catch (Exception e)
                        {
                            Log.Debug(entities.ContainsKey(eId));
                            var et = entities[eId];
                            Log.Debug(et.TripId + "--" + et.StopSequence);
                            Log.Debug(eId.ToString());
                            Log.Error(e.Message);
                        }
                    }

                    //add departure entity
                    if (stop.departure == null)
                        continue;
                    {
                        var eventTimeDeparture = stop.departure.time;
                        const EventType eventType = EventType.PRD;
                        var entity = new Entity(serviceDate,
                                                routeId,
                                                tripId,
                                                stopId,
                                                stopSequence,
                                                vehicleId,
                                                eventType,
                                                eventTimeDeparture,
                                                fileTimestamp,
                                                directionId,
                                                vehicleLabel);
                        var eId = new EntityIdentifier(tripId, stopSequence, serviceDate, eventType);
                        try
                        {

                            if (entities.ContainsKey(eId))
                                entities.Remove(eId);
                            entities.Add(eId, entity);
                        }
                        catch (Exception e)
                        {
                            Log.Debug(entities.ContainsKey(eId));
                            var et = entities[eId];
                            Log.Debug(et.TripId + "--" + et.StopSequence);
                            Log.Debug(eId.ToString());
                            Log.Error(e.Message);
                        }
                    }
                }
            }
            return entities;
        }

        private void GenerateEvents(Dictionary<EntityIdentifier, Entity> Entities)
        {
            foreach (var entity in Entities)
            {
                var eventType = entity.Value.GetEventType();
                if (eventType.Equals(EventType.PRA))
                    InsertArrivalEvent(entity.Value);
                else
                    InsertDepartureEvent(entity.Value);
            }
        }

        private void InsertDepartureEvent(Entity entity)
        {
            var _event = CreateEvent(entity);
            InsertQueue.Enqueue(_event);
        }

        private void InsertArrivalEvent(Entity entity)
        {
            var _event = CreateEvent(entity);
            InsertQueue.Enqueue(_event);
        }

        private static Event CreateEvent(Entity entity)
        {
            var _event = new Event(entity.ServiceDate,
                                   entity.RouteId,
                                   entity.TripId,
                                   entity.StopId,
                                   entity.StopSequence,
                                   entity.VehicleId,
                                   entity._EventType,
                                   entity.EventTime,
                                   entity.FileTimestamp,
                                   entity.DirectionId,
                                   entity.VehicleLabel);
            return _event;
        }

        /*
         * It serializes feed message object to json like structure
         * and writes it to file named TRIPUPDATES.JSON.
         * */

        private static void WriteFeedMessageToFile(FeedMessage feedMessage)
        {
            var jsonFileName = ConfigurationManager.AppSettings["JSONPATH"];
            var text = JsonConvert.SerializeObject(feedMessage, Formatting.Indented);

            File.WriteAllText(jsonFileName, text);
        }

        /**
         * This method deserialise content of .pb file into FeedMessage 
         */

        private static FeedMessage GetFeedMesssages(string outputFileName)
        {
            FeedMessage feedMessage;
            using (var file = File.OpenRead(outputFileName))
            {
                feedMessage = Serializer.Deserialize<FeedMessage>(file);
            }
            return feedMessage;
        }

        /*
         * Return the pause time for while loop. If nothing specified
         * set it to 30 seconds.
         * */

        private static int GetCycleTime()
        {
            var temp = ConfigurationManager.AppSettings["FREQUENCY"];
            if (string.IsNullOrEmpty(temp))
                return 30000;
            return (int.Parse(temp)) * 1000;
        }

        private void DownloadFile(string outputFileName, string url)
        {
            Log.Debug("File downloaded");
            using (var client = new WebClient())
            {
                client.DownloadFile(url, outputFileName);
            }
        }

        private void DoReset()
        {
            Log.Debug("Doing reset.");
            var arm = new ArchiveManager();
            var archiveSuccesful = arm.ArchiveData(Log);

            if (!archiveSuccesful)
                return;

            Thread.Sleep(2000);
            Environment.Exit(1); // Not the best approach.
        }

        private static bool IsResetTime()
        {
            var nowTime = DateTime.Now.ToShortTimeString();
            var resetTime = ConfigurationManager.AppSettings["RESETTIME"];
            resetTime = string.IsNullOrEmpty(resetTime) ? "3:00 AM" : resetTime;
            return nowTime.Equals(resetTime);
        }

        private static string GetUrl()
        {
            var url = ConfigurationManager.AppSettings["URL"];
            return url;
        }

        /*
         * Return the file name of the output file.
         * This file name gives the location of the file 
         * by returning full path.
         * */

        private static string GetOutputFileName()
        {
            var outputFileName = ConfigurationManager.AppSettings["FILEPATH"];
            return outputFileName;
        }
    }
}
