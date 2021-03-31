using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;

namespace SharpKafka.Consumer
{
    public class KafkaConsumer<TKey, TValue> : IDisposable, IKafkaConsumer
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ILogger<KafkaConsumer<TKey, TValue>> _logger;
        private readonly IMessageHandler<TKey, TValue> messageHandler;

        public KafkaConsumer(IOptions<KafkaConfig> option,
            ILogger<KafkaConsumer<TKey, TValue>> logger, IMessageHandler<TKey, TValue> messageHandler
)
        {
            var config = option.Value.Consumer;
            _consumer = new ConsumerBuilder<TKey, TValue>(config).Build();
            _logger = logger;
            this.messageHandler = messageHandler;
        }

        public void StartConsumerLoop(string topic, CancellationToken cancellationToken)
        {
            _consumer.Subscribe(topic);

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
                    messageHandler.Handle(message);

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
