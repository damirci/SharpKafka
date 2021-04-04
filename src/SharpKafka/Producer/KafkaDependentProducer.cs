using Confluent.Kafka;
using SharpKafka.Message;
using System;
using System.Reflection;
using System.Threading.Tasks;


namespace SharpKafka.Producer
{
    public class KafkaDependentProducer<TKey, TValue> : IKafkaDependentProducer<TKey, TValue>

    {
        private readonly IProducer<TKey, TValue> handler;
        private readonly string _topic;
        public KafkaDependentProducer(ProducerClientHandler handle, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            handler = new DependentProducerBuilder<TKey, TValue>(handle.Handle)
                .SetKeySerializer(keySerializer)
                .SetValueSerializer(valueSerializer)
                .Build();
            var topic = typeof(TValue).GetCustomAttribute<TopicAttribute>();
            _topic = topic?.Name;
        }

        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message)
            => handler.ProduceAsync(topic, message);

        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(TKey key, TValue value)
        {
            if (_topic == null)
            {
                throw new ArgumentException("Topic attribute is not defined for this type of value");
            }

            return handler.ProduceAsync(_topic, new Message<TKey, TValue> { Key = key, Value = value });
        }

        public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
            => handler.Produce(topic, message, deliveryHandler);


        public void Produce(TKey key, TValue value, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            if (_topic == null)
            {
                throw new ArgumentException("Topic attribute is not defined for this type of value");
            }

            handler.Produce(_topic, new Message<TKey, TValue> { Key = key, Value = value }, deliveryHandler);
        }

        public void Flush(TimeSpan timeout)
            => handler.Flush(timeout);
    }
}
