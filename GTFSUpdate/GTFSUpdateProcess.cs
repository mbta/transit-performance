using log4net;

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GTFS
{
    internal class GTFSUpdateProcess
    {
        private ILog Log;
        private string sqlConnectionString;
        private string secondarySchema;

        internal bool BeginGTFSUpdateProcess(ILog _log)
        {
            Log = _log;
            sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ConnectionString;
            secondarySchema = ConfigurationManager.AppSettings["SecondarySchemaName"];
            var updateGTFSSuccessful = false;

            try
            {
                var tableCollection = ConvertGTFSFileToJsonObject();
                CreateGTFSTables(tableCollection);
                var PopulateGTFSTableSuccessful = PopulateGTFSTable(tableCollection);

                if (PopulateGTFSTableSuccessful)
                {
                    Parallel.ForEach(tableCollection, ExecuteCreateIndex);
                    updateGTFSSuccessful = true;
                }
            }
            catch (AggregateException ex)
            {
                Log.Error("Exception in GTFS update process. \n" + ex.StackTrace);
                foreach(var e in ex.Flatten().InnerExceptions)
                { Log.Error(e.Message); }
                Log.Error(ex.Message);
            }
            return updateGTFSSuccessful;
        }

        private bool PopulateGTFSTable(GTFSTableCollection tableCollection)
        {
            Log.Info("Start populating gtfs_next schema tables.");
            var requiredFileExists = CheckIfAllRequiredFilesExists(tableCollection);
            if (requiredFileExists)
            {
                var requiredFileList = GetRequiredFileList(tableCollection);
                UpdateAnyNewColumn(requiredFileList);

                foreach (var requiredFile in requiredFileList)
                {
                    UploadData(requiredFile);
                }
                //Parallel.ForEach(requiredFileList, requiredFile => UploadData(requiredFile));
                return true;
            }
            return false;
        }

        private void UploadData(string requiredFile)
        {
            var datatable = GetCorrespondingTable(requiredFile);
            CopyDataIntoTable(datatable, requiredFile);
        }

        protected void UploadData(List<string> requiredFileList)
        {
            foreach (var requiredFile in requiredFileList)
            {
                var datatable = GetCorrespondingTable(requiredFile);
                CopyDataIntoTable(datatable, requiredFile);
            }
        }

        private void CopyDataIntoTable(DataTable datatable, string fileName)
        {
            var GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            var columnList = GetColumnList(GTFSPath + "/" + fileName + ".txt");

            Log.Info(datatable.TableName);

            using (var sr = new StreamReader(GTFSPath + "/" + fileName + ".txt"))
            {
                sr.ReadLine();
                var batchSize = 0;
                while (sr.Peek() > -1)
                {
                    var line = sr.ReadLine();
                    var dataRowValues = ParseLine(line);
                    AddDataRow(columnList, datatable, dataRowValues);
                    batchSize++;
                    if (batchSize == 10000)
                    {
                        BulkInsertIntoDatabase(datatable);
                        batchSize = 0;
                        datatable.Clear();
                    }
                }
            }

            BulkInsertIntoDatabase(datatable);
        }

        /*
         * Copy the datatable to the database. 
         */
        private void BulkInsertIntoDatabase(DataTable datatable)
        {
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            using (var s = new SqlBulkCopy(sqlConnection))
            {
                s.DestinationTableName = datatable.TableName;
                foreach (var column in datatable.Columns)
                    s.ColumnMappings.Add(column.ToString(), column.ToString());

                Log.Info("BulkInsert for " + datatable.TableName);

                s.BulkCopyTimeout = 180;
                s.WriteToServer(datatable);
            }
            sqlConnection.Close();
        }

        /*
         *  Add a new data row.
         */
        private void AddDataRow(List<string> columnList, DataTable datatable, List<string> dataRowValues)
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
                    if(string.IsNullOrEmpty(data))
                    { newDataRow[columnName] = DBNull.Value; }
                    else
                    { newDataRow[columnName] = data; }
                }
                i++;
            }


            datatable.Rows.Add(newDataRow);
        }

        /*
         *  Given a file name, return a corresponding database table. 
         */
        private DataTable GetCorrespondingTable(string requiredFile)
        {
            var datatable = new DataTable(secondarySchema + "." + requiredFile);
            var sqlQuery = @"select * from " + secondarySchema + "." + requiredFile;
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            datatable.Load(cmd.ExecuteReader());
            sqlConnection.Close();
            return datatable;

        }

        /*
         *  Get a list of column for a particular table from the database
         *  
         */
        private List<string> GetColumnListFromDatabase(string tableName)
        {
            var sqlQuery = @"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('" + secondarySchema + "." + tableName + "')";
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
         *  This method returns the list of column from the file
         *  Column name are present in the first line of GTFS data file
         */
        private List<string> GetColumnList(string file)
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
        private List<string> ParseLine(string line)
        {
            var feedValues = string.IsNullOrEmpty(line) ? null :
                new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))")
                    .Matches(line)
                    .Cast<Match>()
                    .Select(match => match.Groups[4].Success ? match.Groups[4].Value :
                                (match.Groups[2].Success ? match.Groups[2].Value : "") +
                                (match.Groups[3].Success ? match.Groups[3].Value : "")).ToArray();
            return feedValues?.ToList() ?? new List<string>();
        }

        /*
         * If a column in present in the data file but not in the corresponfding 
         * database table, add the column to the table in the database.
         */
        private void UpdateAnyNewColumn(List<string> requiredFileList)
        {
            var GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            foreach (var file in requiredFileList)
            {
                var fileColumnList = GetColumnList(GTFSPath + "/" + file + ".txt");
                var sqlColumnList = GetColumnListFromDatabase(file);
                foreach (var column in fileColumnList)
                {
                    if (!sqlColumnList.Contains(column))
                    {
                        AddColumnToTable(column, file);
                    }
                }
            }
        }

        /*
         *  Add a column to an existing table. 
         */
        private void AddColumnToTable(string columnName, string file)
        {
            var sqlQuery = @"ALTER TABLE " + secondarySchema + "." + file + " ADD " + columnName + " VARCHAR(MAX) ";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteReader();
            sqlConnection.Close();
        }

        /*
         *  From the GTFS table collection return the list of the 
         *  rewquired tables ie files.
         */
        private static List<string> GetRequiredFileList(GTFSTableCollection tableCollection)
        {
            return (from gtfsTable in tableCollection
                    where gtfsTable.required
                    select gtfsTable.name).ToList();
        }

        /*
         *  From the downloaded folder, check if all the required files, mentioned in
         *  the gtfs_file_structure are present.
         */
        private bool CheckIfAllRequiredFilesExists(GTFSTableCollection tableCollection)
        {
            var GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            var fileNames = GetFileNames(GTFSPath, "*.txt");
            var requiredFileExists = false;

            foreach (var gtfsTable in tableCollection)
            {
                if (!gtfsTable.required)
                    continue;

                if (fileNames.Contains(gtfsTable.name))
                {
                    requiredFileExists = true;
                }
                else
                {
                    Log.Info("Required file " + gtfsTable.name + " does not exist.");
                    return false;
                }
            }
            return requiredFileExists;
        }

        /*
         * This method returns a list of file names based on filter.
         * If the filter is .txt, it will return list of filenames of type text.
         * The file names are stripped off from their extension.
         * */
        private List<string> GetFileNames(string path, string filter)
        {
            var d = new DirectoryInfo(path);
            var fileInfoList = d.GetFiles(filter);
            var fileNames = new List<string>();

            foreach (var fileInfo in fileInfoList)
            {
                var fileName = Path.GetFileNameWithoutExtension(fileInfo.FullName);
                fileNames.Add(fileName);
            }

            return fileNames;
        }

        private void CreateGTFSTables(GTFSTableCollection tableCollection)
        {
            CreateTablesFromCollection(tableCollection);
            Log.Info("Table creation successful in database.");
        }

        private GTFSTableCollection ConvertGTFSFileToJsonObject()
        {
            Log.Info("Parse the gtfs_file_structure and identify the schema tables.");
            var gtfs_file_structure = ConfigurationManager.AppSettings["GTFSFileStructure"];
            string jsonString;
            using (var sr = new StreamReader(gtfs_file_structure))
            {
                jsonString = sr.ReadToEnd();
            }
            var tableCollection = SchemaContainer.GetTables(jsonString).tables;
            return tableCollection;
        }

        /*
        *  Create schema if it does not exists.
        *  For each table, drop the table from the database.
        *  Create a new table in the database
        *  Create indices on the column of tables as required.
        */
        private void CreateTablesFromCollection(GTFSTableCollection tableCollection)
        {
            CreateScehma();
            foreach (var gtfsTable in tableCollection)
            {
                ExecuteDropTableQuery(gtfsTable);
                ExecuteCreateTableQuery(gtfsTable);
            }
        }

        /*
        * Check if the scehma exists in the database.
        * If it does not exist, create one.
        */
        private void CreateScehma()
        {
            var sqlQuery = $"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{secondarySchema}') EXEC('CREATE SCHEMA {secondarySchema}')";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
        }

        /*
        *  Create the indexes 
        */
        private void ExecuteCreateIndex(GTFSTable gtfsTable)
        {
            var tableName = gtfsTable.name;

            foreach (var column in gtfsTable.columns)
            {
                var columnName = column.name;
                if (!column.index)
                    continue;

                var sqlQuery = $"CREATE NONCLUSTERED INDEX IX_{tableName}_{columnName} ON {secondarySchema}.{tableName} ({columnName})";
                var sqlConnection = new SqlConnection(sqlConnectionString);
                sqlConnection.Open();
                var cmd = new SqlCommand(sqlQuery, sqlConnection) {CommandTimeout = 120};
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
                Log.Info($"Created NONCLUSTERED INDEX IX_{tableName}_{columnName} ON {secondarySchema}.{tableName} ({columnName})");
            }
        }

        /*
         *  Create the table in the database 
         */
        private void ExecuteCreateTableQuery(GTFSTable gtfsTable)
        {
            var tableName = gtfsTable.name;
            var columnsList = GetColumns(gtfsTable.columns);
            var primaryKeys = GetPrimaryKeys(gtfsTable.columns);
            var sqlQuery = @"CREATE TABLE " + secondarySchema + "." +
                tableName +
                " ( " +
                string.Join(" , ", columnsList) +
                (primaryKeys.Count > 0 ? @" PRIMARY KEY (" + string.Join(", ", primaryKeys) + " )" : "") + @");";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Created table " + secondarySchema + "." + tableName + " in database.");
        }

        private List<string> GetPrimaryKeys(GTFSColumnSet gtfsColumnSet)
        {
            var primaryKeys = new List<string>();
            foreach (var column in gtfsColumnSet)
            {
                if (column.primaryKey)
                {
                    primaryKeys.Add(column.name);
                }
            }
            return primaryKeys;
        }

        /*
         *  Given a set of GTFS column of a table,
         *  returns a list of column from the set with their
         *  specification for the database. 
         *  Specification include: column datatype and can have null or not.
         */
        private List<string> GetColumns(GTFSColumnSet gtfsColumnSet)
        {
            return gtfsColumnSet
                .Select(column => $"{column.name} {column.type} " +
                                  $"{(column.primaryKey | !column.allowNull ? " NOT NULL " : " NULL")}")
                .ToList();
        }

        /*
         *  Check if the table already exists in the database.
         *  If it exists drop it from the database.
         */
        private void ExecuteDropTableQuery(GTFSTable gtfsTable)
        {
            var tableName = gtfsTable.name;
            var schemaName = secondarySchema;
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var sqlQuery = $"IF OBJECT_ID ('{secondarySchema}.{tableName}', 'U') IS NOT NULL DROP TABLE {schemaName}.{tableName};";
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Dropped table " + schemaName + "." + tableName + " from database.");
        }

        protected List<string> GetListOfPrimaryKeys(GTFSTable gtfsTable)
        {
            return (from column in gtfsTable.columns
                    where column.primaryKey
                    select column.name).ToList();
        }
    }
}
