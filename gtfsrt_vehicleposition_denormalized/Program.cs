using System.Configuration;
using System.Reflection;

using log4net;
using log4net.Config;

using Topshelf;

namespace gtfsrt_vehicleposition_denormalized
{
    internal class Program
    {
        internal static ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static void Main()
        {
            XmlConfigurator.Configure();
            Log.Info($"***** START - Version {Assembly.GetExecutingAssembly().GetName().Version} *****");
            var instanceName = ConfigurationManager.AppSettings["SERVICENAME"] ?? "gtfsrt_vehicleposition_denormalized";
            Log.Info(instanceName);

            HostFactory.Run(serviceConfig =>
            {
                serviceConfig.UseLog4Net();
                serviceConfig.Service<VehiclePositionService>(serviceInstance =>
                {
                    serviceInstance.ConstructUsing(() => new VehiclePositionService());
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
                serviceConfig.SetDescription(ConfigurationManager.AppSettings["SERVICEDESCRIPTION"] ?? "gtfsrt_vehicleposition_denormalized");
                //serviceConfig.RunAsPrompt();

                serviceConfig.StartAutomatically();
            });
        }
    }
}
