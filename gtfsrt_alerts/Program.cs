using System.Configuration;
using System.Reflection;

using log4net;
using log4net.Config;

using Topshelf;

namespace gtfsrt_alerts
{
    static class Program
    {
        internal static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        static void Main()
        {
            XmlConfigurator.Configure();
            var instanceName = ConfigurationManager.AppSettings["SERVICENAME"] ?? "gtfsrt_alerts";
            Log.Info($"***** START - Version {Assembly.GetExecutingAssembly().GetName().Version} *****");
            Log.Info(instanceName);

            HostFactory.Run(serviceConfig =>
            {
                serviceConfig.UseLog4Net();
                serviceConfig.Service<AlertService>(serviceInstance =>
                {
                    serviceInstance.ConstructUsing(() => new AlertService());
                    serviceInstance.WhenStarted(execute => execute.Start());
                    serviceInstance.WhenStopped(execute => execute.Stop());
                });

                serviceConfig.EnableServiceRecovery(recoveryOption =>
                {
                    recoveryOption.RestartService(1);
                    recoveryOption.RestartService(1);
                    recoveryOption.RestartService(1);
                });

                serviceConfig.SetServiceName(instanceName);
                serviceConfig.SetDisplayName(instanceName);
                serviceConfig.SetDescription(ConfigurationManager.AppSettings["SERVICEDESCRIPTION"] ?? "gtfsrt_alerts");
                //serviceConfig.RunAsPrompt();

                serviceConfig.StartAutomatically();
            });
        }
    }
}
