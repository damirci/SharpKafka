using Confluent.Kafka;

namespace SharpKafka
{
    public interface IMessageHandler<TKey,TValue>: IMessageHandler
    {
        public bool Handle(Message<TKey, TValue> message);
    }

    public interface IMessageHandler
    {
    }
}
