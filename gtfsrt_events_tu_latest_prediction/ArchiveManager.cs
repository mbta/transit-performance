using System.Configuration;
using System.Data.SqlClient;

using log4net;

namespace gtfsrt_events_tu_latest_prediction
{
    internal class ArchiveManager
    {
        internal bool ArchiveData(ILog Log)
        {
            var SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();
            using (var connection = new SqlConnection(SqlConnectionString))
            {
                connection.Open();
                
                const string query1 = @"INSERT INTO dbo.event_rt_trip_archive SELECT * FROM dbo.event_rt_trip";
                var cmd = new SqlCommand
                          {
                              Connection = connection,
                              CommandText = query1,
                              CommandTimeout = 300
                          };
                var rowsInserted = cmd.ExecuteNonQuery();
                var rowsDeleted = -2;

                if (rowsInserted > 0)
                {
                    const string query2 = "DELETE FROM dbo.event_rt_trip";
                    cmd.CommandText = query2;
                    //cmd.CommandTimeout = 30;
                    rowsDeleted = cmd.ExecuteNonQuery();
                }

                if (rowsInserted != rowsDeleted)
                    return false;

                Log.Debug("Moved " + rowsInserted + " events into archive table.");
                Log.Debug("Archiving successful");
                return true;
            }
        }
    }
}
