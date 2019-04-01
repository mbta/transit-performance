using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;

using GtfsRealtimeLib;

using IBI.DataAccess.Models;

using log4net;
using log4net.Config;

namespace gtfsrt_alerts
{
    internal class AlertService
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private List<string> _previousAlertIds = new List<string>();
        public AlertService()
        {
        }

        public void Start()
        {
            try
            {
                XmlConfigurator.Configure();
                Log.Info("Program started");

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
           var alerts =  Utils.GetAlerts(feedMessage);

           Utils.InsertAlertsRows(alerts, ref _previousAlertIds, false);
       }

       public void Stop()
        {
            Thread.Sleep(1000);
            Environment.Exit(0);
        }
    }
}
