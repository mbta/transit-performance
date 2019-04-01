using Newtonsoft.Json;
using System.Linq;

namespace ConfigUpdate
{
    internal class ConfigTable
    {
        public ConfigTable()
        {
            required = true;
        }

        [JsonProperty("columns", NullValueHandling = NullValueHandling.Ignore)]
        public ConfigColumnSet columns { get; set; }

        [JsonProperty("required", NullValueHandling = NullValueHandling.Ignore)]
        public bool required { get; set; }

        [JsonProperty("name", NullValueHandling = NullValueHandling.Ignore)]
        public string name { get; set; }

        public override string ToString()
        {
            return $"{name}|{required}|Columns: {string.Join(";", columns.Select(x => x.ToString()))}";
        }
    }
}
