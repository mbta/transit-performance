using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"Position")]
    public partial class Position : global::ProtoBuf.IExtensible
    {
        public Position() { }

        private float _latitude;
        [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name = @"latitude", DataFormat = global::ProtoBuf.DataFormat.FixedSize)]
        public float latitude
        {
            get { return _latitude; }
            set { _latitude = value; }
        }
        private float _longitude;
        [global::ProtoBuf.ProtoMember(2, IsRequired = true, Name = @"longitude", DataFormat = global::ProtoBuf.DataFormat.FixedSize)]
        public float longitude
        {
            get { return _longitude; }
            set { _longitude = value; }
        }
        private float _bearing = default(float);
        [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"bearing", DataFormat = global::ProtoBuf.DataFormat.FixedSize)]
        [global::System.ComponentModel.DefaultValue(default(float))]
        public float bearing
        {
            get { return _bearing; }
            set { _bearing = value; }
        }
        private double _odometer = default(double);
        [global::ProtoBuf.ProtoMember(4, IsRequired = false, Name = @"odometer", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(default(double))]
        public double odometer
        {
            get { return _odometer; }
            set { _odometer = value; }
        }
        private float _speed = default(float);
        [global::ProtoBuf.ProtoMember(5, IsRequired = false, Name = @"speed", DataFormat = global::ProtoBuf.DataFormat.FixedSize)]
        [global::System.ComponentModel.DefaultValue(default(float))]
        public float speed
        {
            get { return _speed; }
            set { _speed = value; }
        }
        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
