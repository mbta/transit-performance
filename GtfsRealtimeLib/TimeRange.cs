using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"TimeRange")]
    public partial class TimeRange : global::ProtoBuf.IExtensible
    {
        public TimeRange() { }

        private ulong _start = default(ulong);
        [global::ProtoBuf.ProtoMember(1, IsRequired = false, Name = @"start", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(default(ulong))]
        public ulong start
        {
            get { return _start; }
            set { _start = value; }
        }
        private ulong _end = default(ulong);
        [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"end", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(default(ulong))]
        public ulong end
        {
            get { return _end; }
            set { _end = value; }
        }
        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
