using Confluent.Kafka;
using SharpKafka.Message;
using System;
using System.Reflection;
using System.Threading.Tasks;


namespace SharpKafka.Producer
{
    public class KafkaDependentProducer<K, V> : IKafkaDependentProducer<K, V>

    {
        private readonly IProducer<K, V> handler;
        private readonly string _topic;
        public KafkaDependentProducer(ProducerClientHandler handle, ISerializer<K> keySerializer, ISerializer<V> valueSerializer)
        {
            handler = new DependentProducerBuilder<K, V>(handle.Handle)
                .SetKeySerializer(keySerializer)
                .SetValueSerializer(valueSerializer)
                .Build();
            var topic = typeof(V).GetCustomAttribute<TopicAttribute>();
            _topic = topic?.Name;
        }

        public Task ProduceAsync(string topic, Message<K, V> message)
            => handler.ProduceAsync(topic, message);

        public Task ProduceAsync(K key, V value)
        {
            if (_topic == null)
            {
                throw new ArgumentException("Topic attribute is not defined for this type of value");
            }

            return handler.ProduceAsync(_topic, new Message<K, V> { Key = key, Value = value });
        }

        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
            => handler.Produce(topic, message, deliveryHandler);


        public void Produce(K key, V value, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            if (_topic == null)
            {
                throw new ArgumentException("Topic attribute is not defined for this type of value");
            }

            handler.Produce(_topic, new Message<K, V> { Key = key, Value = value }, deliveryHandler);
        }

        public void Flush(TimeSpan timeout)
            => handler.Flush(timeout);
    }
}
