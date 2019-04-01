using System.ServiceProcess;

namespace gtfsrt_events_tu_latest_prediction
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
#if (!DEBUG)
            var ServicesToRun = new ServiceBase[]
                                {
                                    new gtfsrt_events_tu_latest_prediction_service()
                                };
            ServiceBase.Run(ServicesToRun);
#else
            gtfsrt_events_tu_latest_prediction_service.Start();
#endif
        }
    }
}
