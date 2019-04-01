using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"FeedEntity")]
    public partial class FeedEntity : global::ProtoBuf.IExtensible
    {
        public FeedEntity()
        {
            // default constructor
        }

        private string _id;
        [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name = @"id", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public string id
        {
            get { return _id; }
            set { _id = value; }
        }


        private bool _is_deleted = false;
        [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"is_deleted", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue((bool)false)]
        public bool is_deleted
        {
            get { return _is_deleted; }
            set { _is_deleted = value; }
        }

        private TripUpdate _trip_update = null;
        [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"trip_update", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue(null)]
        public TripUpdate trip_update
        {
            get { return _trip_update; }
            set { _trip_update = value; }
        }

        private VehiclePosition _vehicle = null;
        [global::ProtoBuf.ProtoMember(4, IsRequired = false, Name = @"vehicle", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue(null)]
        public VehiclePosition vehicle
        {
            get { return _vehicle; }
            set { _vehicle = value; }
        }

        private Alert _alert = null;
        [global::ProtoBuf.ProtoMember(5, IsRequired = false, Name = @"alert", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue(null)]
        public Alert alert
        {
            get { return _alert; }
            set { _alert = value; }
        }

        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
