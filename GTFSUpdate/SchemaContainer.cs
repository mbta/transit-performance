using Newtonsoft.Json;

namespace GTFS
{
    internal class SchemaContainer
    {
        internal static SchemaContainer GetTables(string jsonString)
        {
            return JsonConvert.DeserializeObject<SchemaContainer>(jsonString);
        }

        [JsonProperty("tables")]
        public GTFSTableCollection tables
        {
            get;
            set;
        }
    }
}
