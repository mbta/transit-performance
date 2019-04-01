using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"Alert")]
    public partial class Alert : global::ProtoBuf.IExtensible
    {
        public Alert()
        {

        }

        private readonly global::System.Collections.Generic.List<TimeRange> _active_period = new global::System.Collections.Generic.List<TimeRange>();
        [global::ProtoBuf.ProtoMember(1, Name = @"active_period", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public global::System.Collections.Generic.List<TimeRange> active_period
        {
            get { return _active_period; }
        }

        private readonly global::System.Collections.Generic.List<EntitySelector> _informed_entity = new global::System.Collections.Generic.List<EntitySelector>();
        [global::ProtoBuf.ProtoMember(5, Name = @"informed_entity", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public global::System.Collections.Generic.List<EntitySelector> informed_entity
        {
            get { return _informed_entity; }
        }

        private Alert.Cause _cause = Alert.Cause.UNKNOWN_CAUSE;
        [global::ProtoBuf.ProtoMember(6, IsRequired = false, Name = @"cause", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(Alert.Cause.UNKNOWN_CAUSE)]
        public Alert.Cause cause
        {
            get { return _cause; }
            set { _cause = value; }
        }
        private Alert.Effect _effect = Alert.Effect.UNKNOWN_EFFECT;
        [global::ProtoBuf.ProtoMember(7, IsRequired = false, Name = @"effect", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(Alert.Effect.UNKNOWN_EFFECT)]
        public Alert.Effect effect
        {
            get { return _effect; }
            set { _effect = value; }
        }
        private TranslatedString _url = null;
        [global::ProtoBuf.ProtoMember(8, IsRequired = false, Name = @"url", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue(null)]
        public TranslatedString url
        {
            get { return _url; }
            set { _url = value; }
        }
        private TranslatedString _header_text = null;
        [global::ProtoBuf.ProtoMember(10, IsRequired = false, Name = @"header_text", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue(null)]
        public TranslatedString header_text
        {
            get { return _header_text; }
            set { _header_text = value; }
        }
        private TranslatedString _description_text = null;
        [global::ProtoBuf.ProtoMember(11, IsRequired = false, Name = @"description_text", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue(null)]
        public TranslatedString description_text
        {
            get { return _description_text; }
            set { _description_text = value; }
        }
        [global::ProtoBuf.ProtoContract(Name = @"Cause")]
        public enum Cause
        {

            [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN_CAUSE", Value = 1)]
            UNKNOWN_CAUSE = 1,

            [global::ProtoBuf.ProtoEnum(Name = @"OTHER_CAUSE", Value = 2)]
            OTHER_CAUSE = 2,

            [global::ProtoBuf.ProtoEnum(Name = @"TECHNICAL_PROBLEM", Value = 3)]
            TECHNICAL_PROBLEM = 3,

            [global::ProtoBuf.ProtoEnum(Name = @"STRIKE", Value = 4)]
            STRIKE = 4,

            [global::ProtoBuf.ProtoEnum(Name = @"DEMONSTRATION", Value = 5)]
            DEMONSTRATION = 5,

            [global::ProtoBuf.ProtoEnum(Name = @"ACCIDENT", Value = 6)]
            ACCIDENT = 6,

            [global::ProtoBuf.ProtoEnum(Name = @"HOLIDAY", Value = 7)]
            HOLIDAY = 7,

            [global::ProtoBuf.ProtoEnum(Name = @"WEATHER", Value = 8)]
            WEATHER = 8,

            [global::ProtoBuf.ProtoEnum(Name = @"MAINTENANCE", Value = 9)]
            MAINTENANCE = 9,

            [global::ProtoBuf.ProtoEnum(Name = @"CONSTRUCTION", Value = 10)]
            CONSTRUCTION = 10,

            [global::ProtoBuf.ProtoEnum(Name = @"POLICE_ACTIVITY", Value = 11)]
            POLICE_ACTIVITY = 11,

            [global::ProtoBuf.ProtoEnum(Name = @"MEDICAL_EMERGENCY", Value = 12)]
            MEDICAL_EMERGENCY = 12
        }

        [global::ProtoBuf.ProtoContract(Name = @"Effect")]
        public enum Effect
        {

            [global::ProtoBuf.ProtoEnum(Name = @"NO_SERVICE", Value = 1)]
            NO_SERVICE = 1,

            [global::ProtoBuf.ProtoEnum(Name = @"REDUCED_SERVICE", Value = 2)]
            REDUCED_SERVICE = 2,

            [global::ProtoBuf.ProtoEnum(Name = @"SIGNIFICANT_DELAYS", Value = 3)]
            SIGNIFICANT_DELAYS = 3,

            [global::ProtoBuf.ProtoEnum(Name = @"DETOUR", Value = 4)]
            DETOUR = 4,

            [global::ProtoBuf.ProtoEnum(Name = @"ADDITIONAL_SERVICE", Value = 5)]
            ADDITIONAL_SERVICE = 5,

            [global::ProtoBuf.ProtoEnum(Name = @"MODIFIED_SERVICE", Value = 6)]
            MODIFIED_SERVICE = 6,

            [global::ProtoBuf.ProtoEnum(Name = @"OTHER_EFFECT", Value = 7)]
            OTHER_EFFECT = 7,

            [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN_EFFECT", Value = 8)]
            UNKNOWN_EFFECT = 8,

            [global::ProtoBuf.ProtoEnum(Name = @"STOP_MOVED", Value = 9)]
            STOP_MOVED = 9
        }

        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
