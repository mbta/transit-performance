namespace gtfsrt_events_vp_current_status
{
    internal class VehicleEntity
    {
        public string VehicleId;
        public string VehicleLabel;
        public string tripId;

        public override bool Equals(object obj)
        {
            var item = obj as VehicleEntity;
            if (item == null)
            {
                return false;
            }

            return (VehicleId.Equals(item.VehicleId) && tripId.Equals(item.tripId));
        }

        public override int GetHashCode()
        {
            return (VehicleId + tripId).GetHashCode();
        }
    }
}