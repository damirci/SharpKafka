using Confluent.Kafka;

namespace SharpKafka.Workers
{
    public class DependentConsumer<TKey, TValue> : IDependentConsumer<TKey, TValue>
    {
        public IConsumer<TKey, TValue> Consumer { get;}
        public DependentConsumer(KafkaConfig config, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            Consumer = new ConsumerBuilder<TKey, TValue>(config.Consumer)
                .SetKeyDeserializer(keyDeserializer)
                .SetValueDeserializer(valueDeserializer)
                .Build();
        }
    }
}
