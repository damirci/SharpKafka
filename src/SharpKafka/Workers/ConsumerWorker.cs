using Microsoft.Extensions.Hosting;
using SharpKafka.Consumer;
using SharpKafka.Message;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace SharpKafka.Workers
{
    public class ConsumerWorker<TKey, TValue> : BackgroundService, IConsumerWorker<TKey, TValue>
    {
        private readonly IMessageHandler<TKey, TValue> _messageHandler;
        private readonly IKafkaConsumer<TKey, TValue> _consumer;

        public ConsumerWorker(IMessageHandler<TKey, TValue> messageHandler, IKafkaConsumer<TKey, TValue> consumer)
        {
            _messageHandler = messageHandler;
            _consumer = consumer;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => _consumer.StartConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        public override void Dispose()
        {
            _consumer.Dispose();
            base.Dispose();
        }
    }
}
