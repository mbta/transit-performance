using Newtonsoft.Json;

namespace GTFS
{
    internal class GTFSTable
    {
        public GTFSTable()
        {
            required = true;
        }

        [JsonProperty("columns", NullValueHandling = NullValueHandling.Ignore)]
        public GTFSColumnSet columns { get; set; }

        [JsonProperty("required", NullValueHandling = NullValueHandling.Ignore)]
        public bool required { get; set; }

        [JsonProperty("name", NullValueHandling = NullValueHandling.Ignore)]
        public string name { get; set; }

        public override string ToString()
        {
            return $"{name}: {columns.Count} columns; {required}";
        }
    }
}
