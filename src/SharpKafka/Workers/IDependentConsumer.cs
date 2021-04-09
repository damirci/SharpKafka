using Confluent.Kafka;

namespace SharpKafka.Workers
{
    public interface IDependentConsumer<TKey,TValue>
    {
        IConsumer<TKey, TValue> Consumer { get; }
    }
}
