using Newtonsoft.Json;

namespace ConfigUpdate
{
    internal class SchemaContainer
    {
        internal static SchemaContainer GetTables(string jsonString)
        {
            return JsonConvert.DeserializeObject<SchemaContainer>(jsonString);
        }

        [JsonProperty("tables")]
        public ConfigTableCollection tables { get; set; }
    }
}
