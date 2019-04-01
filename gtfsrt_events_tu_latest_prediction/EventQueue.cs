using System.Collections.Concurrent;
using System.Threading;

namespace gtfsrt_events_tu_latest_prediction
{
    /*
     * This queue is used for queuing up the items in blocking
     * collection manner. Useful for multithreading.
     * 
     * */

    internal class BlockingQueue<T>
    {
        private readonly BlockingCollection<T> _BlockingQueue = new BlockingCollection<T>();
        private readonly AutoResetEvent QueueNotifier = new AutoResetEvent(false);
        private readonly string QueueName;

        internal BlockingQueue(string queueName)
        {
            QueueName = queueName;
        }

        public BlockingQueue()
        {
            // TODO: Complete member initialization
        }

        internal string GetQueueName()
        {
            return QueueName;
        }

        public void Enqueue(T item)
        {
            _BlockingQueue.Add(item);
            QueueNotifier.Set();
        }

        public T Dequeue()
        {
            if (_BlockingQueue.Count == 0)
            {
                QueueNotifier.WaitOne();
                QueueNotifier.Reset();
            }
            var item = _BlockingQueue.Take();
            QueueNotifier.Reset();
            return item;
        }

        internal int GetCount()
        {
            return _BlockingQueue.Count;
        }
    }
}
