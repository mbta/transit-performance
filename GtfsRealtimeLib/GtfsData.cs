using System;
using System.Configuration;
using System.IO;
using System.Net;
using System.Threading;

using log4net;

using Newtonsoft.Json;

using ProtoBuf;

namespace GtfsRealtimeLib
{
    public delegate void NewFeedMessageEventHandler(FeedMessage feedMessage);

    public class GtfsData
    {
        private readonly ILog Log;
        private ulong FileTimestamp;

        public static readonly string FILEPATH = ConfigurationManager.AppSettings["FILEPATH"];
        public static readonly string JSONPATH = ConfigurationManager.AppSettings["JSONPATH"];
        public static readonly string URL = ConfigurationManager.AppSettings["URL"];
        public static readonly string FREQUENCY = ConfigurationManager.AppSettings["FREQUENCY"];
        //public static string ACCEPTROUTE = ConfigurationManager.AppSettings["ACCEPTROUTE"];
        //public static string PERIOD_END_CHANGE_SECONDS = ConfigurationManager.AppSettings["PERIOD_END_CHANGE_SECONDS"];

        public event NewFeedMessageEventHandler NewFeedMessage;

        public GtfsData(ILog log)
        {
            Log = log;
        }

        public void GetData()
        {
            var cycleTime = GetCycleTime();
            // Above part is executed only once when thread is started.

            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11;

            while (true)
            {
                try
                {
                    DownloadFile();
                    CheckFeedMessage();
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

        private void DownloadFile()
        {
            using (var client = new WebClient())
            {
                var directory = Path.GetDirectoryName(FILEPATH);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                    Directory.CreateDirectory(directory);

                client.DownloadFile(URL, FILEPATH);

                Log.Debug("File downloaded...");
            }
        }

        private static int GetCycleTime()
        {
            if (string.IsNullOrEmpty(FREQUENCY))
                return 30000;
            return (int.Parse(FREQUENCY)) * 1000;
        }

        private void WriteFeedMessageToFile(FeedMessage feedMessage)
        {
            var text = JsonConvert.SerializeObject(feedMessage, Formatting.Indented);

            File.WriteAllText(JSONPATH, text);
        }

        private void CheckFeedMessage()
        {
            var feedMessage = GetFeedMessage(FILEPATH);
            if (feedMessage == null)
                return;

            WriteFeedMessageToFile(feedMessage);

            var fileTimestamp = feedMessage.header.timestamp;
            if (fileTimestamp == FileTimestamp)
            {
                Log.Info("Current file has same timestamp as previous one.");
                return;
            }

            Log.Info("New data to process...");

            NewFeedMessage?.Invoke(feedMessage);
            FileTimestamp = fileTimestamp;
        }

        private static FeedMessage GetFeedMessage(string outputFileName)
        {
            FeedMessage feedMessage;
            using (var file = File.OpenRead(outputFileName))
            {
                feedMessage = Serializer.Deserialize<FeedMessage>(file);
            }
            return feedMessage;
        }
    }
}
