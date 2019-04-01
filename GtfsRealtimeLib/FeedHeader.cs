using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"FeedHeader")]
    public partial class FeedHeader : global::ProtoBuf.IExtensible
    {
        public FeedHeader()
        {
        }

        private string _gtfs_realtime_version;
        [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name = @"gtfs_realtime_version", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public string gtfs_realtime_version
        {
            get { return _gtfs_realtime_version; }
            set { _gtfs_realtime_version = value; }
        }

        private FeedHeader.Incrementality _incrementality = FeedHeader.Incrementality.FULL_DATASET;
        [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"incrementality", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(FeedHeader.Incrementality.FULL_DATASET)]
        public FeedHeader.Incrementality incrementality
        {
            get { return _incrementality; }
            set { _incrementality = value; }
        }

        private ulong _timestamp = default(ulong);
        [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"timestamp", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(default(ulong))]
        public ulong timestamp
        {
            get { return _timestamp; }
            set { _timestamp = value; }
        }

        [global::ProtoBuf.ProtoContract(Name = @"Incrementality")]
        public enum Incrementality
        {

            [global::ProtoBuf.ProtoEnum(Name = @"FULL_DATASET", Value = 0)]
            FULL_DATASET = 0,

            [global::ProtoBuf.ProtoEnum(Name = @"DIFFERENTIAL", Value = 1)]
            DIFFERENTIAL = 1
        }

        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
