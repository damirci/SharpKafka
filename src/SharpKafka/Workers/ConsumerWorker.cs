using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SharpKafka.Message;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace SharpKafka.Workers
{
    public class ConsumerWorker<TKey, TValue> : BackgroundService, IConsumerWorker<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ILogger<ConsumerWorker<TKey, TValue>> _logger;
        private readonly IMessageHandler<TKey, TValue> _messageHandler;
        private readonly string _topic;

        public ConsumerWorker(KafkaConfig option,
            ILogger<ConsumerWorker<TKey, TValue>> logger,
            IMessageHandler<TKey, TValue> messageHandler,
            IDeserializer<TKey> keyDersializer,
            IDeserializer<TValue> valueDersializer)
        {
            var config = option.Consumer;
            _consumer = new ConsumerBuilder<TKey, TValue>(config)
                .SetKeyDeserializer(keyDersializer)
                .SetValueDeserializer(valueDersializer)
                .Build();
            _logger = logger;
            _messageHandler = messageHandler;
            var topic = _messageHandler.GetType().GetCustomAttribute<TopicAttribute>();
            _topic = topic.Name;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        public void StartConsumerLoop(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topic);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);

                    // Handle message...
                    if (consumeResult == null || consumeResult.Message == null)
                    {
                        continue;
                    }
                    var message = consumeResult.Message;
                    _ = _messageHandler.Handle(message);

                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    _logger.LogError($"Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError($"Unexpected error: {e}");
                    break;
                }
            }
        }

        public override void Dispose()
        {
            GC.SuppressFinalize(this);
            _consumer.Close(); // Commit offsets and leave the group cleanly.
            _consumer.Dispose();
            base.Dispose();
        }
    }
}
