using System;
using System.Configuration;
using log4net;
using Newtonsoft.Json;

namespace AppLauncher
{
    class Launcher
    {
        public Launcher(ILog log)
        {
            Log = log;
        }

        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        private static ILog Log { get; set; }

        internal void StartAppLaucher()
        {
            string configurationString = ReadConfigurationFile();
            TasksConfiguration tasksConfiguration = DeserializeConfiguration(configurationString);
            BeginTasks(tasksConfiguration);
        }

        private void BeginTasks(TasksConfiguration tasksConfiguration)
        {
            tasksConfiguration.SortTasks();

            TaskExecuter taskExecuter = new TaskExecuter(tasksConfiguration.databaseConfiguration);

            foreach (Task task in tasksConfiguration.tasks)
            {
                if (task.enable)
                {
                    Log.Info("Start executing " + task.name);
                    taskExecuter.DoTask(task);
                    Log.Info("Executed " + task.name);
                }
              
            }
        }   
        

        private static TasksConfiguration DeserializeConfiguration(string configurationString)
        {
            var tasksConfiguration = JsonConvert.DeserializeObject<TasksConfiguration>(configurationString);
            Log.Info("Converted json");
            return tasksConfiguration;
        }

        private static string ReadConfigurationFile()
        {
            // Read the file as one string.
            String tasksFile = ConfigurationManager.AppSettings["TasksFile"];
            string text = System.IO.File.ReadAllText(tasksFile);
            Log.Info("Read configuration file.");
            return text;
        }
    }
}
