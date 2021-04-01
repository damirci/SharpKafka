using System.Threading;

namespace SharpKafka.Consumer
{
    public interface IKafkaConsumer<TKey, TValue>
    {
        void Dispose();
        void StartConsumerLoop(CancellationToken cancellationToken);
    }
}