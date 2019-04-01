using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace GTFS
{
    internal class GTFSUpdate
    {
        private ILog Log;

        internal bool InitialiseGTFSUpdate(ILog log)
        {
            Log = log;
            return true;
        }

        internal int RunGTFSUpdate()
        {
            int updateSuccessful;
            var downloadAndCompareFeedInfo = ConfigurationManager.AppSettings["DownloadAndCompareFeedInfo"].ToUpper();
            var feedInfoFileUrl = ConfigurationManager.AppSettings["FeedInfoFileUrl"];

            try
            {
                var feedInfoUpdated = false;

                if ("TRUE".Equals(downloadAndCompareFeedInfo))
                {
                    DownloadFile("feed_info_temp.txt", feedInfoFileUrl);
                    feedInfoUpdated = CompareFeedInfoFile();
                }

                if (feedInfoUpdated || "FALSE".Equals(downloadAndCompareFeedInfo))
                {
                    Log.Info("Run GTFS schedule database update.");

                    updateSuccessful = UpdateGTFSDatabase();
                }
                else
                {
                    Log.Info("GTFS feed schedule dataset has remain unchanged.");
                    updateSuccessful = 2;
                    return updateSuccessful;
                }
            }
            catch (Exception ex)
            {
                Log.Error("Error in GTFS run.", ex);
                updateSuccessful = 1;
            }

            return updateSuccessful;
        }

        private int UpdateGTFSDatabase()
        {
            /*
             * Download the .zip file
             * Extract files from zip archive
             * Process data from the files 
             * Update database with new dataset
             */
            int updateGtfsSuccessful; //default to indicate not successful
            var feedInfoUpdated = false;

            var downloadGTFS = ConfigurationManager.AppSettings["DownloadGTFS"].ToUpper();
            if ("TRUE".Equals(downloadGTFS))
            {
                var GTFSDataSetUrl = ConfigurationManager.AppSettings["GTFSDataSetUrl"];
                var GTFSZipPath = ConfigurationManager.AppSettings["GTFSZipPath"];
                DownloadFile(GTFSZipPath, GTFSDataSetUrl);
                ExtractZipArchive(GTFSZipPath);
            }

            var compareExtractedFeedInfo = ConfigurationManager.AppSettings["CompareExtractedFeedInfo"].ToUpper();
            if ("TRUE".Equals(compareExtractedFeedInfo))
            {
                var GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];
                var extractedFeedInfoPath = GTFSPath + "\\feed_info.txt";

                File.Copy(extractedFeedInfoPath, "feed_info_temp.txt", true);
                feedInfoUpdated = CompareFeedInfoFile();
                if (feedInfoUpdated)
                { Log.Error("feed_info.txt has been updated"); } //logged as error to trigger email...should be changed to something else if/when logging logic changed to allow emails for items other than errors
            }

            if (feedInfoUpdated || "FALSE".Equals(compareExtractedFeedInfo))
            {
                var gtfsUpdateProcess = new GTFSUpdateProcess();
                var updateGtfsSuccessfulBool = gtfsUpdateProcess.BeginGTFSUpdateProcess(Log);
                updateGtfsSuccessful = updateGtfsSuccessfulBool ? 0 : 1;
            }
            else
            {
                Log.Info("feed_info.txt has not been updated. GTFS feed schedule dataset has remain unchanged.");
                updateGtfsSuccessful = 2;
            }

            return updateGtfsSuccessful;
        }

        private void ExtractZipArchive(string path)
        {
            var GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            var dir = new DirectoryInfo(GTFSPath);

            if (dir.Exists)
            {
                dir.Delete(true);
                Log.Info("Delete GTFS folder");
            }

            ZipFile.ExtractToDirectory(path, GTFSPath);
            Log.Info("GTFS extraction successful.");
        }

        private bool CompareFeedInfoFile()
        {
            if (!File.Exists("feed_info.txt"))
            {
                File.Copy("feed_info_temp.txt", "feed_info.txt");
                return true;
            }
            var feedInfoUpdated = CompareFields();
            return feedInfoUpdated;
        }

        private bool CompareFields()
        {
            var feedInfoUpdated = false;

            string local_line_1;
            string local_line_2;

            using (var sr = new StreamReader("feed_info.txt"))
            {
                local_line_1 = sr.ReadLine();
                local_line_2 = sr.ReadLine();
            }

            var local_feedKeys = GetFeedKeys(local_line_1);
            var local_feedValues = GetFeedValues(local_line_2);

            var local_feed_info_fields = new Dictionary<string, object>();

            if (local_feedKeys.Length == local_feedValues.Length)
            {
                for (var i = 0; i < local_feedKeys.Length; i++)
                {
                    local_feed_info_fields[local_feedKeys[i]] = local_feedValues[i];
                }
            }

            string downloaded_line_1;
            string downloaded_line_2;

            using (var sr = new StreamReader("feed_info_temp.txt"))
            {
                downloaded_line_1 = sr.ReadLine();
                downloaded_line_2 = sr.ReadLine();
            }

            var downloaded_feedKeys = GetFeedKeys(downloaded_line_1);
            var downloaded_feedValues = GetFeedValues(downloaded_line_2);

            var downloaded__feed_info_fields = new Dictionary<string, object>();

            if (downloaded_feedKeys.Length == downloaded_feedValues.Length)
            {
                for (var i = 0; i < downloaded_feedKeys.Length; i++)
                {
                    downloaded__feed_info_fields[downloaded_feedKeys[i]] = downloaded_feedValues[i];
                }
            }

            foreach (var entry in downloaded__feed_info_fields)
            {
                var key = entry.Key;

                var local_value = local_feed_info_fields[key];
                var downloaded_value = downloaded__feed_info_fields[key];

                if (downloaded_value.Equals(local_value))
                    continue;

                Log.Info("Field: " + key + " has changed. Value " + local_value + " is changed to " + downloaded_value);
                feedInfoUpdated = true;
            }
            return feedInfoUpdated;

        }

        private object[] GetFeedValues(string line_2)
        {
            object[] feedValues = string.IsNullOrEmpty(line_2) ? null
                : new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))")
                    .Matches(line_2)
                    .Cast<Match>()
                    .Select(match => match.Groups[4].Success ? match.Groups[4].Value :
                                (match.Groups[2].Success ? match.Groups[2].Value : "") +
                                (match.Groups[3].Success ? match.Groups[3].Value : ""))
                    .ToArray();

            return feedValues;
        }

        private string[] GetFeedKeys(string line_2)
        {
            var feedValues = string.IsNullOrEmpty(line_2) ? null
                : new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))")
                    .Matches(line_2)
                    .Cast<Match>()
                    .Select(match => match.Groups[4].Success ? match.Groups[4].Value :
                                (match.Groups[2].Success ? match.Groups[2].Value : "") + 
                                (match.Groups[3].Success ? match.Groups[3].Value : ""))
                    .ToArray();
            return feedValues;
        }

        private void DownloadFile(string outputFileName, string Url)
        {
            using (var Client = new WebClient())
            {
                Client.DownloadFile(Url, outputFileName);
            }
            Log.Info("Download of file " + outputFileName + " successful.");
        }
    }
}
