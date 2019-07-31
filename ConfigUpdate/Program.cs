using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace ConfigUpdate
{
    internal class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly string sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ConnectionString;

        private static int Main()
        {
            try
            {
                XmlConfigurator.Configure();

                Log.Info($"*********START**********");
                var savedDataFile = ConfigurationManager.AppSettings["LastModifiedDateFile"];

                var configFilePath = ConfigurationManager.AppSettings["ConfigStructureFile"];

                var configFilesDirectory = ConfigurationManager.AppSettings["ConfigFilesPath"];

                Log.Info($"LastModifiedDateFile: {savedDataFile}");
                Log.Info($"ConfigStructureFile: {configFilePath}");
                Log.Info($"ConfigFilesPath: {configFilesDirectory}");

                if (!File.Exists(savedDataFile))
                {
                    RunFirstTime(configFilePath, configFilesDirectory);
                    CreateLastModifiedDateFile(configFilePath, savedDataFile, configFilesDirectory);
                    Log.Info($"ConfigUpdate successful - run first time!");
                    Log.Info($"########################");
                    return 0;
                }

                Log.Info($"Get previous times...");
                var previousWriteTimes = GetPreviousWriteTime(savedDataFile);

                Log.Info($"Get table collection...");
                var tableCollection = ConvertConfigFileToJsonObject(configFilePath);

                Log.Info($"Get current times...");
                var currentWriteTimes = GetCurrentWriteTime(tableCollection, configFilesDirectory);

                var structureChange = CheckStructureInformation(previousWriteTimes, configFilePath);

                Log.Info($"Structure changed: {structureChange}");

                if (structureChange)
                {
                    // execute the extra steps
                    RecreateDatabaseTables(configFilePath);
                }

                foreach (var file in currentWriteTimes.Keys)
                {
                    var currentFileWriteTime = currentWriteTimes[file];

                    if (previousWriteTimes.ContainsKey(file))
                    {
                        if (!previousWriteTimes[file].Equals(currentFileWriteTime))
                        {
                            Log.Info($"File for table {file} might have changed - updating columns and data...");
                            UpdateAnyNewColumn(file, configFilesDirectory);
                            PopulateDatabase(file, configFilesDirectory, true);
                        }
                        else
                        {
                            Log.Info($"File for table {file} unchanged");
                        }
                    }
                    else
                    {
                        Log.Info($"File for table {file} - newly added - updating columns and data...");
                        UpdateAnyNewColumn(file, configFilesDirectory);
                        PopulateDatabase(file, configFilesDirectory);
                    }
                }

                Log.Info($"Update last write time");
                UpdateLastWriteTime(savedDataFile, currentWriteTimes, configFilePath, configFilesDirectory);

                Log.Info($"ConfigUpdate successful!");
                Log.Info($"########################");
                return 0;
            }
            catch (Exception e)
            {
                Log.Fatal($"Error: {e.Message}");
                return 1;
            }
        }

        private static void UpdateLastWriteTime(string lastModifiedFilePath, Dictionary<string, string> currentWriteTimes, string configFilePath, string configFilesPath)
        {
            File.Delete(lastModifiedFilePath);

            var tableNames = currentWriteTimes.Keys.ToList();

            WriteLastWriteTimeFile(configFilePath, lastModifiedFilePath, configFilesPath, tableNames);
        }

        private static void CreateLastModifiedDateFile(string configFilePath, string lastModifiedFilePath, string configFilesPath)
        {
            var tableCollection = ConvertConfigFileToJsonObject(configFilePath);
            var tableNames = tableCollection.Select(x => x.name);

            WriteLastWriteTimeFile(configFilePath, lastModifiedFilePath, configFilesPath, tableNames);

            Log.Info($"Last modified file created: {lastModifiedFilePath}");
        }

        private static void WriteLastWriteTimeFile(string configFilePath, string lastModifiedFilePath, string configFilesPath, IEnumerable<string> tableNames)
        {
            using (var file = new StreamWriter(lastModifiedFilePath))
            {
                foreach (var tableName in tableNames.OrderBy(x => x))
                {
                    var fileForTable = Path.Combine(configFilesPath, tableName) + ".csv";

                    if (File.Exists(fileForTable))
                        file.Write(tableName + "@" + File.GetLastWriteTime(fileForTable).ToString(CultureInfo.InvariantCulture) + "$");
                }

                file.Write(Path.GetFileNameWithoutExtension(configFilePath) + "@" + File.GetLastWriteTime(configFilePath).ToString(CultureInfo.InvariantCulture));
            }
        }

        private static void RunFirstTime(string configFilePath, string configFilesDirectory)
        {
            var tableCollection = RecreateDatabaseTables(configFilePath);
            //RecreateDatabaseTables(configFilePath);

            foreach (var table in tableCollection)
            {
                var filePath = Path.Combine(configFilesDirectory, table.name) + ".csv";
                if (!File.Exists(filePath))
                {
                    Log.Warn($"File {filePath} does not exist - cannot check for new columns or populate the table");
                    continue;
                }
                UpdateAnyNewColumn(table.name, configFilesDirectory);
                PopulateDatabase(table.name, configFilesDirectory);
            }
        }

        private static ConfigTableCollection RecreateDatabaseTables(string path)
        {
            var tableCollection = ConvertConfigFileToJsonObject(path);
            foreach (var table in tableCollection)
            {
                ExecuteDropTableQuery(table);
                ExecuteCreateTableQuery(table);
            }
            return tableCollection;
        }

        private static Dictionary<string, string> GetCurrentWriteTime(ConfigTableCollection tableCollection, string configFilesPath)
        {
            var currentWriteTime = new Dictionary<string, string>();
            var files = Directory.GetFiles(configFilesPath, "*.csv");

            var tableList = new List<string>();

            foreach (var table in tableCollection)
            {
                tableList.Add(table.name);
            }

            foreach (var file in files)
            {
                var s1 = File.GetLastWriteTime(file).ToString(CultureInfo.InvariantCulture);
                var s2 = Path.GetFileNameWithoutExtension(file);

                if (tableList.Contains(s2))
                {
                    currentWriteTime[s2] = s1;
                }
            }

            return currentWriteTime;
        }

        /*
     * If a column in present in the data file but not in the corresponfding 
     * database table, add the column to the table in the database.
     */
        private static void UpdateAnyNewColumn(string tableName, string filepath)
        {
            var fileColumnList = GetColumnList(Path.Combine(filepath, tableName) + ".csv");
            var sqlColumnList = GetColumnListFromDatabase(tableName);
            foreach (var column in fileColumnList)
            {
                if (!sqlColumnList.Contains(column))
                {
                    AddColumnToTable(column, tableName);
                }
            }
        }

        /*
         *  Get a list of column for a particular table from the database
         *  
         */
        private static List<string> GetColumnListFromDatabase(string tableName)
        {
            var sqlQuery = @"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('" + "dbo" + "." + tableName + "')";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            var reader = cmd.ExecuteReader();
            var sqlColumnList = new List<string>();
            while (reader.Read())
            {
                var columnName = reader.GetString(0);
                sqlColumnList.Add(columnName);
            }
            reader.Close();
            sqlConnection.Close();
            return sqlColumnList;
        }

        /*
 *  Add a column to an existing table. 
 */
        private static void AddColumnToTable(string columnName, string file)
        {
            var sqlQuery = @"ALTER TABLE dbo" + "." + file + " ADD " + columnName + " VARCHAR(MAX) ";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteReader();
            sqlConnection.Close();
        }

        private static bool CheckStructureInformation(Dictionary<string, string> previousWriteTime, string path)
        {
            var currentWriteTime = File.GetLastWriteTime(path);
            var fileName = Path.GetFileNameWithoutExtension(path);

            var previousTime = previousWriteTime[fileName];

            return !currentWriteTime.ToString(CultureInfo.InvariantCulture).Equals(previousTime);
        }

        private static Dictionary<string, string> GetPreviousWriteTime(string savedDataFile)
        {
            var previousWriteTime = new Dictionary<string, string>();

            using (var sr = new StreamReader(savedDataFile))
            {
                var line = sr.ReadToEnd();

                var fileTimestamps = line.Split('$');

                foreach (var file in fileTimestamps)
                {
                    if (string.IsNullOrEmpty(file))
                        continue;
                    var fileInformation = file.Split('@');
                    var fileName = fileInformation[0];
                    var fileTime = fileInformation[1];
                    previousWriteTime.Add(fileName, fileTime);
                }
            }

            return previousWriteTime;
        }

        /*
            *  This method returns the list of column from the file
            *  Column name are present in the first line of GTFS data file
            */
        private static List<string> GetColumnList(string file)
        {
            string line;
            using (var sr = new StreamReader(file))
            {
                line = sr.ReadLine();
            }
            return ParseLine(line);
        }
        
        /*
         *  Parse the comma seperated line, and return the list of strings 
         */
        private static List<string> ParseLine(string line)
        {
            var feedValues = string.IsNullOrEmpty(line) ? null : new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))")
                .Matches(line).Cast<Match>().Select(match => match.Groups[4].Success ? match.Groups[4].Value
                                                        : (match.Groups[2].Success ? match.Groups[2].Value : "") +
                                                          (match.Groups[3].Success ? match.Groups[3].Value : "")).ToArray();
            return feedValues?.ToList() ?? new List<string>();
        }

        private static void CopyDataIntoTable(DataTable datatable, string fileName)
        {
            var columnList = GetColumnList(fileName + ".csv");

            Log.Info($"Saving data into {datatable.TableName}...");
            using (var sr = new StreamReader(fileName + ".csv"))
            {
                sr.ReadLine();
                while (sr.Peek() > -1)
                {
                    var line = sr.ReadLine();
                    var dataRowValues = ParseLine(line);
                    AddDataRow(columnList, datatable, dataRowValues);
                }
            }

            BulkInsertIntoDatabase(datatable);
        }

        /*
      * Copy the datatable to the database. 
      */
        private static void BulkInsertIntoDatabase(DataTable datatable)
        {
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            using (var s = new SqlBulkCopy(sqlConnection))
            {
                s.DestinationTableName = datatable.TableName;
                foreach (var column in datatable.Columns)
                    s.ColumnMappings.Add(column.ToString(), column.ToString());
                s.WriteToServer(datatable);
            }
            sqlConnection.Close();
        }

        /*
       *  Add a new data row.
       */
        private static void AddDataRow(List<string> columnList, DataTable datatable, List<string> dataRowValues)
        {
            var newDataRow = datatable.NewRow();
            var i = 0;

            foreach (var data in dataRowValues)
            {
                var flag = false;
                var columnName = columnList[i];
                var columnType = datatable.Columns[columnName].DataType;

                if (columnType == typeof(DateTime))
                {
                    DateTime dateTime;
                    if (!DateTime.TryParse(data, out dateTime))
                    {
                        if (!DateTime.TryParseExact(data, "yyyyMMdd", new CultureInfo("en-US"), DateTimeStyles.None, out dateTime))
                        {
                            throw new Exception(data + " is not in a valid date format.");
                        }
                    }
                    newDataRow[columnName] = dateTime;
                    flag = true;
                }

                if (columnType == typeof(bool))
                {
                    if (data.Equals("1"))
                        newDataRow[columnName] = true;
                    if (data.Equals("0"))
                        newDataRow[columnName] = false;
                    flag = true;

                }

                if (!flag)
                {
                    newDataRow[columnName] = string.IsNullOrEmpty(data) ? null : data;
                }
                i++;
            }

            datatable.Rows.Add(newDataRow);
        }

        private static void PopulateDatabase(string tableName, string configFilepath, bool cleanTable = false)
        {
            if(cleanTable)
                CleanTable(tableName);
            var datatable = GetCorrespondingTable(tableName);
            CopyDataIntoTable(datatable, Path.Combine(configFilepath, tableName));
        }

        private static DataTable GetCorrespondingTable(string requiredFile)
        {
            var datatable = new DataTable($"dbo.{requiredFile}");
            var sqlQuery = $"select * from dbo.{requiredFile}";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            datatable.Load(cmd.ExecuteReader());
            sqlConnection.Close();
            return datatable;
        }

        private static void CleanTable(string tableName)
        {
            Log.Info($"Deleting data from dbo.{tableName}...");
            var sqlQuery = $"delete from dbo.{tableName}";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
        }

        /*
         *  Check if the table already exists in the database.
         *  If it exists drop it from the database.
         */
        private static void ExecuteDropTableQuery(ConfigTable ConfigTable)
        {
            var tableName = ConfigTable.name;
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var sqlQuery = @"IF OBJECT_ID ('dbo." + tableName + @"', 'U') IS NOT NULL DROP TABLE dbo." + tableName + ";";
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Dropped table dbo" + "." + tableName + " from database.");
        }

        /*
            *  Create the table in the database 
         */
        private static void ExecuteCreateTableQuery(ConfigTable ConfigTable)
        {
            var tableName = ConfigTable.name;
            var columnsList = GetColumns(ConfigTable.columns);
            var primaryKeys = GetPrimaryKeys(ConfigTable.columns);
            var sqlQuery = @"CREATE TABLE dbo" + "." +
                           tableName +
                           " ( " +
                           string.Join(" , ", columnsList) +
                           (primaryKeys.Count > 0 ? @" PRIMARY KEY (" + string.Join(", ", primaryKeys) + " )" : "") + @");";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Created table dbo" + "." + tableName + " in database.");
        }

        private static List<string> GetPrimaryKeys(ConfigColumnSet ConfigColumnSet)
        {
            return (from column in ConfigColumnSet
                    where column.primaryKey
                    select column.name).ToList();
        }

        private static List<string> GetColumns(ConfigColumnSet ConfigColumnSet)
        {
            return ConfigColumnSet.Select(column => column.name + " " + column.type + " " + (column.primaryKey | !column.allowNull ? " NOT NULL " : " NULL"))
                                  .ToList();
        }

        private static ConfigTableCollection ConvertConfigFileToJsonObject(string path)
        {
            Log.Info("Parse the Config_file_structure and identify the schema tables.");
            string jsonString;
            using (var sr = new StreamReader(path))
            {
                jsonString = sr.ReadToEnd();
            }
            var tableCollection = SchemaContainer.GetTables(jsonString).tables;

            Log.Info($"Tables: {tableCollection.Count}: {string.Join(Environment.NewLine, tableCollection.Select(x => x.name))}");

            return tableCollection;
        }
    }
}
