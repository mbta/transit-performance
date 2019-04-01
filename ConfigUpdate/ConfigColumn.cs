using Newtonsoft.Json;

using System.ComponentModel;

namespace ConfigUpdate
{
    internal class ConfigColumn
    {
        public ConfigColumn()
        {
            allowNull = true;
            required = true;
        }

        [DefaultValue(true)]
        [JsonProperty("create", NullValueHandling = NullValueHandling.Ignore)]
        public bool create { get; set; }

        [DefaultValue(false)]
        [JsonProperty("index", NullValueHandling = NullValueHandling.Ignore)]
        public bool index { get; set; }
        
        [JsonProperty("null", NullValueHandling = NullValueHandling.Ignore)]
        [DefaultValue(true)]
        public bool allowNull { get; set; }

        [JsonProperty("type", NullValueHandling = NullValueHandling.Ignore)]
        public string type { get; set; }

        [DefaultValue(false)]
        [JsonProperty("primaryKey", NullValueHandling = NullValueHandling.Ignore)]
        public bool primaryKey { get; set; }

        [DefaultValue(true)]
        [JsonProperty("required", NullValueHandling = NullValueHandling.Ignore)]
        public bool required { get; set; }

        [JsonProperty("name", NullValueHandling = NullValueHandling.Ignore)]
        public string name { get; set; }

        public override string ToString()
        {
            return $"{name}|{primaryKey}|{type}|{required}|{allowNull}";
        }
    }
}