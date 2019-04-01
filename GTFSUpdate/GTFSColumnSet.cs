using Newtonsoft.Json;

using System.Collections.Generic;

namespace GTFS
{
    [JsonConverter(typeof(ColumnSetConverter))]
    internal class GTFSColumnSet : List<GTFSColumn>
    {
    }
}
