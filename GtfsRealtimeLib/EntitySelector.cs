using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    
        [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"EntitySelector")]
        public partial class EntitySelector : global::ProtoBuf.IExtensible
        {
            public EntitySelector() { }

            private string _agency_id = "";
            [global::ProtoBuf.ProtoMember(1, IsRequired = false, Name = @"agency_id", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue("")]
            public string agency_id
            {
                get { return _agency_id; }
                set { _agency_id = value; }
            }
            private string _route_id = "";
            [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"route_id", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue("")]
            public string route_id
            {
                get { return _route_id; }
                set { _route_id = value; }
            }
            private int _route_type = default(int);
            [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"route_type", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
            [global::System.ComponentModel.DefaultValue(default(int))]
            public int route_type
            {
                get { return _route_type; }
                set { _route_type = value; }
            }
            private TripDescriptor _trip = null;
            [global::ProtoBuf.ProtoMember(4, IsRequired = false, Name = @"trip", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue(null)]
            public TripDescriptor trip
            {
                get { return _trip; }
                set { _trip = value; }
            }
            private string _stop_id = "";
            [global::ProtoBuf.ProtoMember(5, IsRequired = false, Name = @"stop_id", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue("")]
            public string stop_id
            {
                get { return _stop_id; }
                set { _stop_id = value; }
            }
            private global::ProtoBuf.IExtension extensionObject;
            global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
        }
    
}
