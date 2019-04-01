using Newtonsoft.Json;

using System.Collections.Generic;

namespace GTFS
{
    [JsonConverter(typeof(TableCollectionConverter))]
    internal class GTFSTableCollection : List<GTFSTable>
    {
    }
}
