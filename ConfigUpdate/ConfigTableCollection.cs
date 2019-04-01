using Newtonsoft.Json;

using System.Collections.Generic;

namespace ConfigUpdate
{
    [JsonConverter(typeof(TableCollectionConverter))]
    internal class ConfigTableCollection : List<ConfigTable>
    {
    }
}
