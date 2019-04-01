using System;
using System.Collections.Concurrent;
using System.Threading;

using log4net;

namespace gtfsrt_events_vp_current_status
{
    internal class EventQueue
    {
        private readonly BlockingCollection<Event> eventQueue = new BlockingCollection<Event>();
        private readonly AutoResetEvent queueNotifier = new AutoResetEvent(false);
        private readonly ILog Log;

        internal EventQueue(ILog log)
        {
            Log = log;
        }

        internal EventQueue()
        {
        }

        public void Enqueue(Event _event)
        {
            try
            {
                eventQueue.Add(_event);
                queueNotifier.Set();
            }
            catch (Exception e)
            {
                Log.Error(e.StackTrace);
            }
        }

        public Event Dequeue()
        {
            if (eventQueue.Count == 0)
            {
                queueNotifier.WaitOne();
                queueNotifier.Reset();
            }
            Event _event = null;
            try
            {
                _event = eventQueue.Take();
            }
            catch (Exception e)
            {
                Log.Error(e.StackTrace);
            }
            queueNotifier.Reset();
            return _event;
        }

        internal int GetCount()
        {
            return eventQueue.Count;
        }
    }
}