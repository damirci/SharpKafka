using Confluent.Kafka;

namespace SharpKafka.Message
{
    public interface IMessageHandler<TKey, TValue>
    {
        /// <summary>
        /// message handler
        /// </summary>
        /// <param name="message">Kafka message</param>
        /// <returns>true if message handling was successful otherwise false</returns>
        bool Handle(Message<TKey, TValue> message);
    }
}
