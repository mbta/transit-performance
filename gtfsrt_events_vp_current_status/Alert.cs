using System;
using System.Collections.Generic;
using System.ComponentModel;

using GtfsRealtimeLib;

using ProtoBuf;

namespace gtfsrt_events_vp_current_status
{
    [Serializable, ProtoContract(Name = @"Alert")]
    public partial class Alert : IExtensible
    {
        public Alert()
        {

        }

        private readonly List<TimeRange> _active_period = new List<TimeRange>();
        [ProtoMember(1, Name = @"active_period", DataFormat = DataFormat.Default)]
        public List<TimeRange> active_period
        {
            get { return _active_period; }
        }

        private readonly List<EntitySelector> _informed_entity = new List<EntitySelector>();
        [ProtoMember(5, Name = @"informed_entity", DataFormat = DataFormat.Default)]
        public List<EntitySelector> informed_entity
        {
            get { return _informed_entity; }
        }

        private Cause _cause = Cause.UNKNOWN_CAUSE;
        [ProtoMember(6, IsRequired = false, Name = @"cause", DataFormat = DataFormat.TwosComplement)]
        [DefaultValue(Cause.UNKNOWN_CAUSE)]
        public Cause cause
        {
            get { return _cause; }
            set { _cause = value; }
        }
        private Effect _effect = Effect.UNKNOWN_EFFECT;
        [ProtoMember(7, IsRequired = false, Name = @"effect", DataFormat = DataFormat.TwosComplement)]
        [DefaultValue(Effect.UNKNOWN_EFFECT)]
        public Effect effect
        {
            get { return _effect; }
            set { _effect = value; }
        }
        private TranslatedString _url = null;
        [ProtoMember(8, IsRequired = false, Name = @"url", DataFormat = DataFormat.Default)]
        [DefaultValue(null)]
        public TranslatedString url
        {
            get { return _url; }
            set { _url = value; }
        }
        private TranslatedString _header_text = null;
        [ProtoMember(10, IsRequired = false, Name = @"header_text", DataFormat = DataFormat.Default)]
        [DefaultValue(null)]
        public TranslatedString header_text
        {
            get { return _header_text; }
            set { _header_text = value; }
        }
        private TranslatedString _description_text = null;
        [ProtoMember(11, IsRequired = false, Name = @"description_text", DataFormat = DataFormat.Default)]
        [DefaultValue(null)]
        public TranslatedString description_text
        {
            get { return _description_text; }
            set { _description_text = value; }
        }
        [ProtoContract(Name = @"Cause")]
        public enum Cause
        {

            [ProtoEnum(Name = @"UNKNOWN_CAUSE", Value = 1)]
            UNKNOWN_CAUSE = 1,

            [ProtoEnum(Name = @"OTHER_CAUSE", Value = 2)]
            OTHER_CAUSE = 2,

            [ProtoEnum(Name = @"TECHNICAL_PROBLEM", Value = 3)]
            TECHNICAL_PROBLEM = 3,

            [ProtoEnum(Name = @"STRIKE", Value = 4)]
            STRIKE = 4,

            [ProtoEnum(Name = @"DEMONSTRATION", Value = 5)]
            DEMONSTRATION = 5,

            [ProtoEnum(Name = @"ACCIDENT", Value = 6)]
            ACCIDENT = 6,

            [ProtoEnum(Name = @"HOLIDAY", Value = 7)]
            HOLIDAY = 7,

            [ProtoEnum(Name = @"WEATHER", Value = 8)]
            WEATHER = 8,

            [ProtoEnum(Name = @"MAINTENANCE", Value = 9)]
            MAINTENANCE = 9,

            [ProtoEnum(Name = @"CONSTRUCTION", Value = 10)]
            CONSTRUCTION = 10,

            [ProtoEnum(Name = @"POLICE_ACTIVITY", Value = 11)]
            POLICE_ACTIVITY = 11,

            [ProtoEnum(Name = @"MEDICAL_EMERGENCY", Value = 12)]
            MEDICAL_EMERGENCY = 12
        }

        [ProtoContract(Name = @"Effect")]
        public enum Effect
        {

            [ProtoEnum(Name = @"NO_SERVICE", Value = 1)]
            NO_SERVICE = 1,

            [ProtoEnum(Name = @"REDUCED_SERVICE", Value = 2)]
            REDUCED_SERVICE = 2,

            [ProtoEnum(Name = @"SIGNIFICANT_DELAYS", Value = 3)]
            SIGNIFICANT_DELAYS = 3,

            [ProtoEnum(Name = @"DETOUR", Value = 4)]
            DETOUR = 4,

            [ProtoEnum(Name = @"ADDITIONAL_SERVICE", Value = 5)]
            ADDITIONAL_SERVICE = 5,

            [ProtoEnum(Name = @"MODIFIED_SERVICE", Value = 6)]
            MODIFIED_SERVICE = 6,

            [ProtoEnum(Name = @"OTHER_EFFECT", Value = 7)]
            OTHER_EFFECT = 7,

            [ProtoEnum(Name = @"UNKNOWN_EFFECT", Value = 8)]
            UNKNOWN_EFFECT = 8,

            [ProtoEnum(Name = @"STOP_MOVED", Value = 9)]
            STOP_MOVED = 9
        }

        private IExtension extensionObject;
        IExtension IExtensible.GetExtensionObject(bool createIfMissing)
        { return Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
