using System;
using System.Collections.Generic;
using System.Linq;

using IBI.DataAccess.DataSets;

namespace IBI.DataAccess.Models
{
    public class AlertData
    {
        public AlertData()
        {
            ActivePeriods = new List<AlertActivePeriodData>();
            InformedEntities = new List<AlertInformedEntityData>();
        }

        public string GtfsRealtimeVersion { get; set; }
        public string Incrementality { get; set; }
        public ulong HeaderTimestamp { get; set; }
        public string AlertId { get; set; }
        public string Cause { get; set; }
        public string Effect { get; set; }
        public string HeaderText { get; set; }
        public string HeaderLanguage { get; set; }
        public string DescriptionText { get; set; }
        public string DescriptionLanguage { get; set; }
        public string Url { get; set; }
        public List<AlertInformedEntityData> InformedEntities { get; set; }
        public List<AlertActivePeriodData> ActivePeriods { get; set; }
        public bool CheckPeriodEndChange { get; set; }
        public bool Closed { get; set; }

        //public DateTime? FileTime => Utils.GetUtcTimeFromSeconds(HeaderTimestamp);

        public override bool Equals(object obj)
        {
            var other = obj as AlertData;

            return other != null && Equals(other);
        }

        protected bool Equals(AlertData other)
        {
            return EqualsExceptActivePeriods(other) && EqualsActivePeriods(other);
        }

        internal bool EqualsActivePeriods(AlertData other)
        {
            var equals = true;

            if (!CheckPeriodEndChange)
            {
                if (ActivePeriods.Any(activePeriod => !other.ActivePeriods.Any(x => x.Equals(activePeriod))))
                {
                    equals = false;
                }
            }
            else
            {
                foreach (var activePeriod in ActivePeriods)
                {
                    var otherActivePeriod = other.ActivePeriods.FirstOrDefault(x => x.ActivePeriodStart == activePeriod.ActivePeriodStart);
                    var seconds = otherActivePeriod != null ? Math.Abs((int) activePeriod.ActivePeriodEnd - (int) otherActivePeriod.ActivePeriodEnd) : 0;

                    equals = otherActivePeriod != null && seconds <= AlertsDataSet.ActivePeriodEndChangeSeconds;

                    if (!equals)
                        break;
                }
            }

            return equals;
        }

        internal bool EqualsExceptActivePeriods(AlertData other)
        {
            var equals = Utils.AreEqual(Cause, other.Cause) &&
                         Utils.AreEqual(Effect, other.Effect) &&
                         Utils.AreEqual(HeaderText, other.HeaderText) &&
                         Utils.AreEqual(DescriptionText, other.DescriptionText) &&
                         Utils.AreEqual(Url, other.Url) &&
                         InformedEntities.Count == other.InformedEntities.Count &&
                         ActivePeriods.Count == other.ActivePeriods.Count &&
                         Closed == other.Closed;

            if (!equals)
                return false;

            if (InformedEntities.Any(informedEntity => !other.InformedEntities.Any(x => x.Equals(informedEntity))))
            {
                equals = false;
            }

            return equals;
        }

        public override int GetHashCode()
        {
            var hashCode = Cause.GetHashCode();
            hashCode = (hashCode * 397) ^ Effect.GetHashCode();
            hashCode = (hashCode * 397) ^ HeaderText.GetHashCode();
            hashCode = (hashCode * 397) ^ DescriptionText.GetHashCode();
            hashCode = (hashCode * 397) ^ Url.GetHashCode();

            return hashCode;
        }

        public override string ToString()
        {
            return $"{AlertId}|{Cause}|{Effect}|{DescriptionText}|{HeaderText}|{Url}|{HeaderTimestamp}";
        }
    }

    public class AlertInformedEntityData
    {
        public ulong HeaderTimestamp { get; set; }
        public string AlertId { get; set; }
        public string AgencyId { get; set; }
        public string RouteId { get; set; }
        public int RouteType { get; set; }
        public string TripId { get; set; }
        public string StopId { get; set; }

        public override bool Equals(object obj)
        {
            var other = obj as AlertInformedEntityData;

            return other != null && Equals(other);
        }

        protected bool Equals(AlertInformedEntityData other)
        {
            return Utils.AreEqual(AgencyId, other.AgencyId) &&
                   Utils.AreEqual(RouteId, other.RouteId) &&
                   Equals(RouteType, other.RouteType) &&
                   Utils.AreEqual(TripId, other.TripId) &&
                   Utils.AreEqual(StopId, other.StopId);
        }

        public override int GetHashCode()
        {
            var hashCode = AgencyId.GetHashCode();
            hashCode = (hashCode * 397) ^ RouteId.GetHashCode();
            hashCode = (hashCode * 397) ^ RouteType.GetHashCode();
            hashCode = (hashCode * 397) ^ TripId.GetHashCode();
            hashCode = (hashCode * 397) ^ StopId.GetHashCode();

            return hashCode;
        }

        public override string ToString()
        {
            return $"{AlertId}|{AgencyId}|{RouteId}|{RouteType}|{TripId}|{StopId}|{HeaderTimestamp}";
        }
    }

    public class AlertActivePeriodData
    {
        public ulong HeaderTimestamp { get; set; }
        public string AlertId { get; set; }
        public ulong ActivePeriodStart { get; set; }
        public ulong ActivePeriodEnd { get; set; }

        //public DateTime? StartTime => Utils.GetUtcTimeFromSeconds(ActivePeriodStart);
        //public DateTime? EndTime => Utils.GetUtcTimeFromSeconds(ActivePeriodEnd);

        public override bool Equals(object obj)
        {
            var other = obj as AlertActivePeriodData;

            return other != null && Equals(other);
        }

        protected bool Equals(AlertActivePeriodData other)
        {
            return Equals(ActivePeriodStart, other.ActivePeriodStart) &&
                   Equals(ActivePeriodEnd, other.ActivePeriodEnd);
        }

        public override int GetHashCode()
        {
            var hashCode = ActivePeriodStart.GetHashCode();
            hashCode = (hashCode * 397) ^ ActivePeriodEnd.GetHashCode();

            return hashCode;
        }

        public override string ToString()
        {
            return $"{AlertId}|{ActivePeriodStart}|{ActivePeriodEnd}|{HeaderTimestamp}";
        }
    }
}
