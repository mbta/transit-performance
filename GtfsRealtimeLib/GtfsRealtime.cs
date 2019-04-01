using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    using System.Net;
    using ProtoBuf;
    
    public class GtfsRealtime
    {
        public FeedMessage GetFeedMessages(string outputFileName)
        {
            FeedMessage feedMessages = null;
            using (var file = File.OpenRead(outputFileName))
            {
                feedMessages = Serializer.Deserialize<FeedMessage>(file);
            }
            return feedMessages;   
        }
    }
}
