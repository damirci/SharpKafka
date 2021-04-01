using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SharpKafka.Extentions;
using SharpKafka.Message;
using SharpKafka.Producer;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace SharpKafka.Workers
{
    public class RetryConsumerWorker<TKey, TValue> : BackgroundService, IConsumerWorker<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ILogger<RetryConsumerWorker<TKey, TValue>> _logger;
        private readonly IMessageHandler<TKey, TValue> _messageHandler;
        private readonly string _topic;
        private readonly IKafkaDependentProducer<TKey, TValue> _producer;
        public readonly ConsumerConfig _config;
        private readonly int _maxRetry;
        private readonly long _retryWait;
        private readonly string _dlqTopic;
        private readonly string _retryTopic;

        public RetryConsumerWorker(KafkaConfig option,
            ILogger<RetryConsumerWorker<TKey, TValue>> logger,
            IMessageHandler<TKey, TValue> messageHandler,
            IDeserializer<TKey> keyDersializer,
            IDeserializer<TValue> valueDersializer,
            IKafkaDependentProducer<TKey, TValue> producer)
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

            _producer = producer;
            var retry = _messageHandler.GetType().GetCustomAttribute<RetryAttribute>();

            _maxRetry = retry.MaxRetry;
            _retryWait = retry.Wait;

            _dlqTopic = $"{_topic}__{option.Consumer.GroupId}__{retry.DlqPostfix}";
            _retryTopic = $"{_topic}__{option.Consumer.GroupId}__{retry.TopicPostfix}";
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        private void StartConsumerLoop(CancellationToken cancellationToken)
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

                    var isHandled = false;
                    var retryCounter = message.Headers.GetRetryCounter();

                    if (retryCounter <= 0)//first try
                    {
                        isHandled = _messageHandler.Handle(message);
                    }
                    else
                    {
                        var waitUntil = message.Timestamp.UtcDateTime.AddMilliseconds(_retryWait);
                        if (waitUntil <= DateTimeOffset.UtcNow)
                        {
                            isHandled = _messageHandler.Handle(message);
                        }
                        else
                        {
                            _producer.Produce(_retryTopic, message);
                            continue;
                        }
                    }

                    if (isHandled)
                    {
                        continue;
                    }

                    var failedMessage = new Message<TKey, TValue> { Key = message.Key, Headers = message.Headers, Value = message.Value };

                    if (retryCounter + 1 < _maxRetry)
                    {
                        failedMessage.Headers.SetRetryCounter(retryCounter + 1);
                        _producer.Produce(_retryTopic, failedMessage);
                    }
                    else
                    {
                        _producer.Produce(_dlqTopic, failedMessage);
                    }
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
