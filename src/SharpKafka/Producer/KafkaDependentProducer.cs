using Confluent.Kafka;
using System;
using System.Threading.Tasks;


namespace SharpKafka.Producer
{
    public class KafkaDependentProducer<K, V> : IKafkaDependentProducer<K, V>
       
    {
        private readonly IProducer<K, V> handler;

        public KafkaDependentProducer(ProducerClientHandler handle, ISerializer<K> keySerializer, ISerializer<V> valueSerializer)
        {
            handler = new DependentProducerBuilder<K, V>(handle.Handle)
                .SetKeySerializer(keySerializer)
                .SetValueSerializer(valueSerializer)
                .Build();
        }

        public Task ProduceAsync(string topic, Message<K, V> message)
            => handler.ProduceAsync(topic, message);

        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
            => handler.Produce(topic, message, deliveryHandler);

        public void Flush(TimeSpan timeout)
            => handler.Flush(timeout);
    }
}
