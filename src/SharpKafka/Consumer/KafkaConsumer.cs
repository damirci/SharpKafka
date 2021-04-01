using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SharpKafka.Message;
using System;
using System.Reflection;
using System.Threading;

namespace SharpKafka.Consumer
{
    public class KafkaConsumer<TKey, TValue> : IDisposable, IKafkaConsumer<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ILogger<KafkaConsumer<TKey, TValue>> _logger;
        private readonly IMessageHandler<TKey, TValue> _messageHandler;
        private readonly string _topic;

        public KafkaConsumer(KafkaConfig option,
            ILogger<KafkaConsumer<TKey, TValue>> logger,
            IMessageHandler<TKey, TValue> messageHandler,
            IDeserializer<TKey> keyDersializer,
            IDeserializer<TValue> valueDersializer
)
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
                    _messageHandler.Handle(message);

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

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            _consumer.Close(); // Commit offsets and leave the group cleanly.
            _consumer.Dispose();
        }
    }
}
