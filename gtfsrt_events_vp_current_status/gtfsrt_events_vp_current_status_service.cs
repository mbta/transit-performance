using System;
using System.Net;
using System.Reflection;
using System.ServiceProcess;
using System.Threading;

using log4net;
using log4net.Config;

namespace gtfsrt_events_vp_current_status
{
    public partial class gtfsrt_events_vp_current_status_service : ServiceBase
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public gtfsrt_events_vp_current_status_service()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            StartGTFSRealtimeService();
        }

        internal static void StartGTFSRealtimeService()
        {
            try
            {
                XmlConfigurator.Configure();
                Log.Info("Start");

                ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                Log.Info($"Enabled Protocols: {ServicePointManager.SecurityProtocol}");

                var eventQueue = new EventQueue(Log);
                var databaseThread = new DatabaseThread(Log, eventQueue);
                var dataThread = new Thread(databaseThread.ThreadRun);
                dataThread.Start();
                Thread.Sleep(1000);
                var eventRecorder = new EventRecorder(eventQueue, Log);
                var eventThread = new Thread(eventRecorder.RecordEvents);
                eventThread.Start();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
                Environment.Exit(1);
            }
        }

        protected override void OnStop()
        {
            Log.Info("Stop");
            Environment.Exit(0);
        }
    }
}
