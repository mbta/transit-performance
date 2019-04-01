using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"TripUpdate")]
    public partial class TripUpdate : global::ProtoBuf.IExtensible
    {
        public TripUpdate() { }

        private TripDescriptor _trip;
        [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name = @"trip", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public TripDescriptor trip
        {
            get { return _trip; }
            set { _trip = value; }
        }
        private VehicleDescriptor _vehicle = null;
        [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"vehicle", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue(null)]
        public VehicleDescriptor vehicle
        {
            get { return _vehicle; }
            set { _vehicle = value; }
        }

        private readonly global::System.Collections.Generic.List<TripUpdate.StopTimeUpdate> _stop_time_update = new global::System.Collections.Generic.List<TripUpdate.StopTimeUpdate>();
        [global::ProtoBuf.ProtoMember(2, Name = @"stop_time_update", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public global::System.Collections.Generic.List<TripUpdate.StopTimeUpdate> stop_time_update
        {
            get { return _stop_time_update; }
        }

        private ulong? _timestamp = null;
        [global::ProtoBuf.ProtoMember(4, IsRequired = false, Name = @"timestamp", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(null)]
        public ulong? timestamp
        {
            get { return _timestamp; }
            set { _timestamp = value; }
        }
        [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"StopTimeEvent")]
        public partial class StopTimeEvent : global::ProtoBuf.IExtensible
        {
            public StopTimeEvent() { }

            private int _delay = default(int);
            [global::ProtoBuf.ProtoMember(1, IsRequired = false, Name = @"delay", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
            [global::System.ComponentModel.DefaultValue(default(int))]
            public int delay
            {
                get { return _delay; }
                set { _delay = value; }
            }
            private long _time = default(long);
            [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"time", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
            [global::System.ComponentModel.DefaultValue(default(long))]
            public long time
            {
                get { return _time; }
                set { _time = value; }
            }
            private int? _uncertainty = null;
            [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"uncertainty", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
            [global::System.ComponentModel.DefaultValue(null)]
            public int? uncertainty
            {
                get { return _uncertainty; }
                set { _uncertainty = value; }
            }
            private global::ProtoBuf.IExtension extensionObject;
            global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
        }

        [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"StopTimeUpdate")]
        public partial class StopTimeUpdate : global::ProtoBuf.IExtensible
        {
            public StopTimeUpdate() { }

            private uint _stop_sequence = default(uint);
            [global::ProtoBuf.ProtoMember(1, IsRequired = false, Name = @"stop_sequence", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
            [global::System.ComponentModel.DefaultValue(default(uint))]
            public uint stop_sequence
            {
                get { return _stop_sequence; }
                set { _stop_sequence = value; }
            }
            private string _stop_id = "";
            [global::ProtoBuf.ProtoMember(4, IsRequired = false, Name = @"stop_id", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue("")]
            public string stop_id
            {
                get { return _stop_id; }
                set { _stop_id = value; }
            }
            private TripUpdate.StopTimeEvent _arrival = null;
            [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"arrival", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue(null)]
            public TripUpdate.StopTimeEvent arrival
            {
                get { return _arrival; }
                set { _arrival = value; }
            }
            private TripUpdate.StopTimeEvent _departure = null;
            [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"departure", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue(null)]
            public TripUpdate.StopTimeEvent departure
            {
                get { return _departure; }
                set { _departure = value; }
            }
            private TripUpdate.StopTimeUpdate.ScheduleRelationship _schedule_relationship = TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED;
            [global::ProtoBuf.ProtoMember(5, IsRequired = false, Name = @"schedule_relationship", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
            [global::System.ComponentModel.DefaultValue(TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)]
            public TripUpdate.StopTimeUpdate.ScheduleRelationship schedule_relationship
            {
                get { return _schedule_relationship; }
                set { _schedule_relationship = value; }
            }
            [global::ProtoBuf.ProtoContract(Name = @"ScheduleRelationship")]
            public enum ScheduleRelationship
            {

                [global::ProtoBuf.ProtoEnum(Name = @"SCHEDULED", Value = 0)]
                SCHEDULED = 0,

                [global::ProtoBuf.ProtoEnum(Name = @"SKIPPED", Value = 1)]
                SKIPPED = 1,

                [global::ProtoBuf.ProtoEnum(Name = @"NO_DATA", Value = 2)]
                NO_DATA = 2
            }

            private global::ProtoBuf.IExtension extensionObject;
            global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
        }

        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
