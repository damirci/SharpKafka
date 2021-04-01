namespace SharpKafka.Consumer
{
    public interface IKafkaRetryConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
    }
}
