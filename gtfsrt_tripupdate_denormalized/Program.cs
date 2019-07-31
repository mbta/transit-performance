using System.Net.Mime;
using System.Reflection;

using log4net;
using log4net.Config;

using Topshelf;

namespace gtfsrt_tripupdate_denormalized
{
    internal class Program
    {
        internal static ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static void Main()
        {
            XmlConfigurator.Configure();
            Log.Info($"***** START {Assembly.GetEntryAssembly()?.GetName().Version} *****");

            HostFactory.Run(serviceConfig =>
            {
                serviceConfig.UseLog4Net();
                serviceConfig.Service<TripUpdateService>(serviceInstance =>
                {
                    serviceInstance.ConstructUsing(() => new TripUpdateService());
                    serviceInstance.WhenStarted(execute => execute.Start());
                    serviceInstance.WhenStopped(execute => execute.Stop());
                });

                serviceConfig.EnableServiceRecovery(recoveryOption =>
                {
                    recoveryOption.RestartService(1);
                    recoveryOption.RestartService(1);
                    recoveryOption.RestartService(1);
                });

                serviceConfig.SetServiceName("gtfsrt_tripupdate_denormalized");
                serviceConfig.SetDisplayName("gtfsrt_tripupdate_denormalized");
                serviceConfig.SetDescription("Saves all trip updates for accepted routes");
                //serviceConfig.RunAsPrompt();

                serviceConfig.StartAutomatically();
            });
        }
    }
}
