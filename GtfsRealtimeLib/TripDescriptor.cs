using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{
    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"TripDescriptor")]
    public partial class TripDescriptor : global::ProtoBuf.IExtensible
    {
        public TripDescriptor() { }

        private string _trip_id = "";
        [global::ProtoBuf.ProtoMember(1, IsRequired = false, Name = @"trip_id", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue("")]
        public string trip_id
        {
            get { return _trip_id; }
            set { _trip_id = value; }
        }


        private uint? _direction_id = default(uint?);
        [global::ProtoBuf.ProtoMember(6, IsRequired = false, Name = @"direction_id", DataFormat = global::ProtoBuf.DataFormat.Default)]
        //[global::System.ComponentModel.DefaultValue(999)]
        public uint? direction_id
        {
            get { return _direction_id; }
            set { _direction_id = value; }
        }
        

        private string _route_id = "";
        [global::ProtoBuf.ProtoMember(5, IsRequired = false, Name = @"route_id", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue("")]
        public string route_id
        {
            get { return _route_id; }
            set { _route_id = value; }
        }
        private string _start_time = "";
        [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"start_time", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue("")]
        public string start_time
        {
            get { return _start_time; }
            set { _start_time = value; }
        }
        private string _start_date = "";
        [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name = @"start_date", DataFormat = global::ProtoBuf.DataFormat.Default)]
        [global::System.ComponentModel.DefaultValue("")]
        public string start_date
        {
            get { return _start_date; }
            set { _start_date = value; }
        }
        private TripDescriptor.ScheduleRelationship _schedule_relationship = TripDescriptor.ScheduleRelationship.SCHEDULED;
        [global::ProtoBuf.ProtoMember(4, IsRequired = false, Name = @"schedule_relationship", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
        [global::System.ComponentModel.DefaultValue(TripDescriptor.ScheduleRelationship.SCHEDULED)]
        public TripDescriptor.ScheduleRelationship schedule_relationship
        {
            get { return _schedule_relationship; }
            set { _schedule_relationship = value; }
        }
        [global::ProtoBuf.ProtoContract(Name = @"ScheduleRelationship")]
        public enum ScheduleRelationship
        {

            [global::ProtoBuf.ProtoEnum(Name = @"SCHEDULED", Value = 0)]
            SCHEDULED = 0,

            [global::ProtoBuf.ProtoEnum(Name = @"ADDED", Value = 1)]
            ADDED = 1,

            [global::ProtoBuf.ProtoEnum(Name = @"UNSCHEDULED", Value = 2)]
            UNSCHEDULED = 2,

            [global::ProtoBuf.ProtoEnum(Name = @"CANCELED", Value = 3)]
            CANCELED = 3
        }

        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
