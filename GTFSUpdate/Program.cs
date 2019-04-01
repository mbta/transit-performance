using log4net;
using log4net.Config;
using System;
using System.Reflection;

namespace GTFS
{
    internal class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private static int Main()
        {
            try
            {
                XmlConfigurator.Configure();

                Log.Info("\n\nGTFS schedule update program start.");

                var gtfsUpdate = new GTFSUpdate();

                Log.Info("Initializing GTFS update");
                var initialisationSuccessful = gtfsUpdate.InitialiseGTFSUpdate(Log);

                if (initialisationSuccessful)
                {
                    Log.Info("Initialisation of GTFS update successful.");
                    Log.Info("Running GTFS update");
                    var runningSuccessful = gtfsUpdate.RunGTFSUpdate();

                    switch (runningSuccessful)
                    {
                        case 0:
                            Log.Info("Begin Migrating process.");
                            var gtfsMigrateProcess = new GTFSMigrateProcess();
                            var migrationSuccessful = gtfsMigrateProcess.BeginMigration(Log);
                            if (migrationSuccessful)
                            {
                                Log.Info("GTFS migration successful");
                                Log.Info("GTFS Schedule update successful.");
                                Log.Info("GTFS schedule update program end.\n\n");
                                return 0;
                            }
                            Log.Info("GTFS migration failed");
                            Log.Info("GTFS Schedule update failed.");
                            Log.Info("GTFS schedule update program end.\n\n");
                            return 1;

                        case 1:
                            Log.Info("GTFS Schedule update failed.");
                            Log.Info("GTFS schedule update program end.\n\n");
                            break;
                    }
                    return 1;
                }
                Log.Info("Initialization of GTFS update failed.");
                Log.Info("GTFS schedule update program end.\n\n");
                return 1;

                //Log.Info("GTFS schedule update program end.\n\n");
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
                return 1;
            }
        }
    }
}
