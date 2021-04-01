using Confluent.Kafka;

namespace SharpKafka.Message
{
    public interface IMessageHandler<TKey, TValue>
    {
        bool Handle(Message<TKey, TValue> message);
    }
}
