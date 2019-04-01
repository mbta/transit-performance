using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace AppLauncher
{
    class TaskExecuter
    {
        private static DatabaseConfiguration _databaseConfiguration;

        public TaskExecuter(DatabaseConfiguration tasksConfiguration)
        {
            _databaseConfiguration = tasksConfiguration;
        }

        internal void DoTask(Task task)
        {
            switch (task.taskType)
            {
                case TaskType.Executable:
                    ExecutableTask(task);
                    break;
                case TaskType.StoredProcedure:
                    StoredProcedureTask(task);
                    break;
            }
        }

        private static void StoredProcedureTask(Task task)
        {
            string connectionString = _databaseConfiguration.GetConnectionString();
            using (var conn = new SqlConnection(connectionString))
            using (var command = new SqlCommand(task.name, conn)
            {
                CommandType = CommandType.StoredProcedure
            })
            {
                command.CommandTimeout = 7200;
                if (task.arguments != null && task.arguments.Count > 0)
                {
                    foreach (var argument in task.arguments)
                    {
                        SqlParameter parameter = new SqlParameter();
                        parameter.ParameterName = argument.name;
                        parameter.SqlDbType = GetDbType(argument.datatype);
                        if (argument.datatype.Equals(DataType.Date) || argument.datatype.Equals(DataType.DateTime))
                        {
                            GetParameterValue(parameter,argument);
                        }
                        else
                        {
                            parameter.Value = argument.value;
                        }
                        parameter.Direction = ParameterDirection.Input;
                       

                        // Add the parameter to the Parameters collection. 
                        command.Parameters.Add(value: parameter);   
                    }
                }
                conn.Open();
                command.ExecuteNonQuery();
                conn.Close();
            }
        }

        private static void GetParameterValue(SqlParameter parameter, Argument argument)
        {
            if ("Today".Equals(argument.value,StringComparison.InvariantCultureIgnoreCase))
            {
                parameter.Value = DateTime.Now.ToString("yyyy-MM-dd");
            }
            if("Yesterday".Equals(argument.value,StringComparison.InvariantCultureIgnoreCase))
            {
                parameter.Value = DateTime.Now.AddDays(-1);
            }
        }

        private static SqlDbType GetDbType(DataType dataType)
        {
            switch (dataType)
            {
                case DataType.Varchar:
                    return SqlDbType.NVarChar;
                case DataType.DateTime:
                    return SqlDbType.DateTime;
                case DataType.Integer:
                    return SqlDbType.Int;
                case DataType.Bit:
                    return SqlDbType.Bit;
                case DataType.NVarchar:
                    return SqlDbType.NVarChar;
                case DataType.Date:
                    return SqlDbType.Date;
                case DataType.Float:
                    return SqlDbType.Float;
            }
            throw new Exception("unkown datatype supplied");
        }

        private static void ExecutableTask(Task task)
        {
            ProcessStartInfo startInfo = ExecutableTaskHelper(task);
            //throw  new Exception(startInfo.WorkingDirectory + "\n"+startInfo.FileName);
            // Start the process with the info we specified.
            // Call WaitForExit and then the using statement will close.
            using (Process exeProcess = Process.Start(startInfo))
            {
                if (exeProcess != null) 
                    exeProcess.WaitForExit();
            }
        }

        private static ProcessStartInfo ExecutableTaskHelper(Task task)
        {
            // Use ProcessStartInfo class
            var startInfo = new ProcessStartInfo
            {
                CreateNoWindow = false,
                UseShellExecute = false,
                FileName = task.name,
                WindowStyle = ProcessWindowStyle.Hidden,
                WorkingDirectory = task.location
            };
            return startInfo;
        }
    }
}
