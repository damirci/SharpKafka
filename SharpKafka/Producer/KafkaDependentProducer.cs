using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Hadoop.Avro;

namespace SharpKafka.Producer
{
    public class KafkaDependentProducer<K, V> : IKafkaDependentProducer<K, V>
       
    {
        private readonly IProducer<K, V> handler;

        public KafkaDependentProducer(ProducerClientHandler handle)
        {
            handler = new DependentProducerBuilder<K, V>(handle.Handle)
                .SetValueSerializer(new JsonSerializer<V>().SerializeAsync())
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
