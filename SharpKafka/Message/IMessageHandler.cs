using Confluent.Kafka;

namespace SharpKafka
{
    public interface IMessageHandler<TKey,TValue>
    {
        public bool Handle(Message<TKey, TValue> message);
    }
}
