using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"FeedMessage")]
    public partial class FeedMessage : global::ProtoBuf.IExtensible
    {
        public FeedMessage()
        {
            // Default Constructor
        }

        private FeedHeader _header;
        [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name = @"header", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public FeedHeader header
        {
            get { return _header; }
            set { _header = value; }
        }

        private readonly global::System.Collections.Generic.List<FeedEntity> _entity = new global::System.Collections.Generic.List<FeedEntity>();
        [global::ProtoBuf.ProtoMember(2, Name = @"entity", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public global::System.Collections.Generic.List<FeedEntity> entity
        {
            get { return _entity; }
        }

        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
