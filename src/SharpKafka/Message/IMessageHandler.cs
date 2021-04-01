using Confluent.Kafka;

namespace SharpKafka.Message
{
    public interface IMessageHandler<TKey, TValue>
    {
        public bool Handle(Message<TKey, TValue> message);
    }
}
