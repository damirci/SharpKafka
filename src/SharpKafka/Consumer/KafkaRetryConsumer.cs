using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SharpKafka.Message;
using SharpKafka.Producer;
using System;
using System.Reflection;
using System.Threading;

namespace SharpKafka.Consumer
{
    public class KafkaRetryConsumer<TKey, TValue> : KafkaConsumer<TKey, TValue>, IKafkaRetryConsumer<TKey, TValue>
    {
        private readonly IKafkaDependentProducer<TKey, TValue> _producer;
        public readonly ConsumerConfig _config;
        private readonly int _maxRetry;
        private readonly long _retryWait;
        private readonly string _dlqTopic;
        private readonly string _retryTopic;

        public KafkaRetryConsumer(KafkaConfig option,
            ILogger<KafkaConsumer<TKey, TValue>> logger,
            IMessageHandler<TKey, TValue> messageHandler,
            IKafkaDependentProducer<TKey, TValue> producer,
            IDeserializer<TKey> keyDersializer,
            IDeserializer<TValue> valueDersializer) : base(option, logger, messageHandler, keyDersializer, valueDersializer)
        {
            _config = option.Consumer;
            _producer = producer;
            var retry = _messageHandler.GetType().GetCustomAttribute<RetryAttribute>();

            _maxRetry = retry.MaxRetry;
            _retryWait = retry.Wait;
            _dlqTopic = $"{_topic}__{option.Consumer.GroupId}__{retry.DlqPostfix}";
            _retryTopic = $"{_topic}__{option.Consumer.GroupId}__{retry.TopicPostfix}";
        }

        public override void StartConsumerLoop(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topic);
            _consumer.Subscribe(_retryTopic);

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

        public override void Dispose()
        {
            GC.SuppressFinalize(this);
            _producer.Flush(TimeSpan.FromSeconds(10));
            base.Dispose();
        }
    }
}
