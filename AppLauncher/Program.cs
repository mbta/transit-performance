using System;
using log4net;
using log4net.Config;
using System.Reflection;

namespace AppLauncher
{
    internal class Program
    {
        private static ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static void Main()
        {
            try
            {
                XmlConfigurator.Configure();
                Log.Info("Start");

                // Start work.
                var laucher = new Launcher(Log);
                laucher.StartAppLaucher();


            }
            catch (Exception ex)
            {
                Log.Error(ex.Message);
                Log.Error(ex.StackTrace);
            }
            finally
            {
                Log.Info("Stop");
            }
        }
    }
}
