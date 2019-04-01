using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Text;

namespace GTFS
{
    internal class GTFSMigrateProcess
    {
        private readonly string sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ConnectionString;
        private readonly string primarySchema = ConfigurationManager.AppSettings["PrimarySchemaName"];
        private readonly string secondarySchema = ConfigurationManager.AppSettings["SecondarySchemaName"];
        private ILog Log;

        internal bool BeginMigration(ILog _Log)
        {
            Log = _Log;

            RunStoredProc();
            CreateScehma();
            var tableNames = GetTableNames(primarySchema);

            if (!ExecuteDropTableQuery(tableNames))
                return false;

            var migrationSuccessful = MigrateTables();

            /*
                 * Once the database has been updated, then only change the local feed info file.
                 * */
            var downloadAndCompareFeedInfo = ConfigurationManager.AppSettings["DownloadAndCompareFeedInfo"].ToUpper();
            var compareExtractedFeedInfo = ConfigurationManager.AppSettings["CompareExtractedFeedInfo"].ToUpper();

            if (!migrationSuccessful || (!"TRUE".Equals(downloadAndCompareFeedInfo) && !"TRUE".Equals(compareExtractedFeedInfo)))
                return migrationSuccessful;

            File.Copy("feed_info_temp.txt", "feed_info.txt", true);
            Log.Info("Updated feed info file with latest version");

            return true;
        }

        private void RunStoredProc()
        {

            var connectionString1 = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();
            //using (var conn = new SqlConnection(connectionString1))
            //using (var command = new SqlCommand("dbo.UpdateGTFSNextStep1", conn)
            //{
            //    CommandType = CommandType.Text
            //})
            //{
            //    conn.Open();

            //    command.CommandTimeout = 3600;
            //    command.ExecuteNonQuery();
            //    conn.Close();
            //}
            //Log.Info("UpdateGTFSNextStep1 procedure completed");

            using (var conn = new SqlConnection(connectionString1))
                using (var command = new SqlCommand("dbo.UpdateGTFSNext", conn)
                                     {
                                         CommandType = CommandType.Text
                                     })
                {
                    conn.Open();
                    command.CommandTimeout = 3600;
                    command.ExecuteNonQuery();
                    conn.Close();
                }
            Log.Info("UpdateGTFSNext procedure completed");


        }

        protected void DownloadFile(string outputFileName, string Url)
        {
            using (var Client = new WebClient())
            {
                Client.DownloadFile(Url, outputFileName);
            }
            Log.Info("Download of file " + outputFileName + " successful.");
        }

        private bool ExecuteDropTableQuery(List<string> tableNames)
        {
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var trans = sqlConnection.BeginTransaction();

            try
            {
                foreach (var table in tableNames)
                {
                    var tableName = table;
                    var schemaName = primarySchema;
                    var sqlQuery = @"IF OBJECT_ID ('" + primarySchema + "." + tableName + @"', 'U') IS NOT NULL DROP TABLE " + schemaName + "." + tableName +
                                   ";";
                    var cmd = new SqlCommand(sqlQuery, sqlConnection, trans);
                    cmd.ExecuteNonQuery();
                    Log.Info("Dropped table " + schemaName + "." + tableName + " from database.");
                }
                trans.Commit();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                trans.Rollback();
                return false;
            }
            sqlConnection.Close();

            return true;
        }

        private bool MigrateTables()
        {
            var queryList = new List<string>();
            var tableNames = GetTableNames(secondarySchema);
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();

            foreach (var table in tableNames)
            {
                var sbr = new StringBuilder();
                sbr.Append("ALTER SCHEMA ");
                sbr.Append(primarySchema);
                sbr.Append(" TRANSFER ");
                sbr.Append(secondarySchema);
                sbr.Append(".");
                sbr.Append(table);
                queryList.Add(sbr.ToString());
            }

            var trans = sqlConnection.BeginTransaction();
            try
            {
                foreach (var sqlquery in queryList)
                {
                    var cmd1 = new SqlCommand(sqlquery, sqlConnection, trans);
                    cmd1.ExecuteNonQuery();
                }
                trans.Commit();
            }
            catch (Exception e)
            {
                trans.Rollback();
                Log.Error(e.Message);
                return false;
            }

            sqlConnection.Close();
            Log.Info("Migration Successful");
            return true;
        }

        private List<string> GetTableNames(string schemaName)
        {
            var sqlQuery = @"SELECT TABLE_NAME 
                                FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + schemaName + "'";
            var tableNames = new List<string>();
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            var reader = cmd.ExecuteReader();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    tableNames.Add(reader.GetString(0));
                }
            }
            reader.Close();
            sqlConnection.Close();
            return tableNames;
        }

        /*
         * Check if the scehma exists in the database.
         * If it does not exist, create one.
         */
        private void CreateScehma()
        {
            var sqlQuery = @"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + primarySchema + "') EXEC( 'CREATE SCHEMA " + primarySchema + "' )";
            var sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            var cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();

            sqlConnection.Close();
        }
    }
}
