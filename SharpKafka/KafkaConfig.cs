using Confluent.Kafka;
using System;

namespace SharpKafka
{
    public class KafkaConfig
    {
        public ProducerConfig Producer { get; set; }
        public ConsumerConfig Consumer { get; set; }
    }
}
