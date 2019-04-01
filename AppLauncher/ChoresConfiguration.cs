using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;

namespace AppLauncher
{
    class TasksConfiguration
    {
        [JsonProperty("DatabaseConfiguration")]
        public DatabaseConfiguration databaseConfiguration { get; set; }

        [JsonProperty("Tasks")]
        internal List<Task> tasks { get; set; }

        internal void SortTasks()
        {
            this.tasks = new List<Task>(this.tasks.OrderBy(o => o.order));
        }
    }

    internal class Task
    {
        [JsonProperty("Name")]
        internal string name { get; set; }

        [JsonProperty("Location")]
        internal string location { get; set; }

        [JsonProperty("TaskType")]
        internal TaskType taskType { get; set; }

        [JsonProperty("Order")]
        internal int order { get; set; }

        [JsonProperty("Arguments")]
        internal List<Argument> arguments { get; set; }

        [JsonProperty("Enable")]
        internal bool enable { get; set; }
    }

    internal class Argument
    {
        [JsonProperty("Name")]
        internal string name { get; set; }

        [JsonProperty("Value")]
        internal string value { get; set; }

        [JsonProperty("Datatype")]
        internal DataType datatype { get; set; }
    }

    enum TaskType
    {
        Executable, StoredProcedure
    }

    enum DataType
    {
        Integer, Bit, DateTime, Varchar, NVarchar,
        Date,
        Float
    }

    class DatabaseConfiguration
    {
        [JsonProperty("Datasource")]
        internal string dataSource { get; set; }

        [JsonProperty("DataBase")]
        internal string databaseName { get; set; }

        [JsonProperty("User")]
        internal string user { get; set; }

        [JsonProperty("Password")]
        internal string password { get; set; }

        public string GetConnectionString()
        {
            var sbr = new StringBuilder();
            sbr.Append("Data Source = " + this.dataSource);
            sbr.Append(";Initial Catalog = " + this.databaseName);
            sbr.Append(";User ID = " + this.user);
            sbr.Append(";Password = " + this.password);
            return sbr.ToString();
        }
    }
}
