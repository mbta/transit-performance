using Newtonsoft.Json;

using System.Collections.Generic;

namespace ConfigUpdate
{
    [JsonConverter(typeof(ColumnSetConverter))]
    internal class ConfigColumnSet : List<ConfigColumn>
    {
    }
}

