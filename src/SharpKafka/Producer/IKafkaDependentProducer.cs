using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace SharpKafka.Producer
{
    public interface IKafkaDependentProducer<TKey, TValue>
    {
        void Flush(TimeSpan timeout);
        void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);
        Task ProduceAsync(string topic, Message<TKey, TValue> message);
        Task ProduceAsync(TKey key, TValue value);
        void Produce(TKey key, TValue value, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);
    }
}
