using System;
using System.Net;
using System.Reflection;
using System.ServiceProcess;
using System.Threading;

using log4net;
using log4net.Config;

namespace gtfsrt_events_tu_latest_prediction
{
    public partial class gtfsrt_events_tu_latest_prediction_service : ServiceBase
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public gtfsrt_events_tu_latest_prediction_service()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            Start();
        }

        internal static void Start()
        {
            try
            {
                XmlConfigurator.Configure();
                Log.Info("Program started");

                ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                Log.Info($"Enabled Protocols: {ServicePointManager.SecurityProtocol}");

                var InsertEventQueue = new BlockingQueue<Event>();
                var UpdateEventQueue = new BlockingQueue<Event>();

                var databaseThread = new DatabaseThread(Log, InsertEventQueue, UpdateEventQueue);
                var dataThread = new Thread(databaseThread.ThreadRun);
                dataThread.Start();

                Thread.Sleep(1000);

                var eventRecorder = new EventRecorder(InsertEventQueue, UpdateEventQueue, Log);

                var eventThread = new Thread(eventRecorder.RecordEvents);
                eventThread.Start();
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

        protected override void OnStop()
        {
            Thread.Sleep(1000);
            Environment.Exit(0);
        }
    }
}
