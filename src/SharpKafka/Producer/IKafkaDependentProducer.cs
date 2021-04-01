using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace SharpKafka.Producer
{
    public interface IKafkaDependentProducer<K, V>
    {
        void Flush(TimeSpan timeout);
        void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null);
        Task ProduceAsync(string topic, Message<K, V> message);
    }
}
