using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"VehicleDescriptor")]
    public partial class VehicleDescriptor : global::ProtoBuf.IExtensible
    {
        public VehicleDescriptor() { }

        private string _id = "";
        [global::ProtoBuf.ProtoMember(1, IsRequired = false, Name = @"id", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue("")]
        public string id
        {
            get { return _id; }
            set { _id = value; }
        }
        private string _label = "";
        [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"label", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue("")]
        public string label
        {
            get { return _label; }
            set { _label = value; }
        }
        private string _license_plate = "";
        [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"license_plate", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue("")]
        public string license_plate
        {
            get { return _license_plate; }
            set { _license_plate = value; }
        }
        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
