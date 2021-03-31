using System.Threading;

namespace SharpKafka.Consumer
{
    public interface IKafkaConsumer
    {
        void Dispose();
        void StartConsumerLoop(string topic, CancellationToken cancellationToken);
    }
}