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
    public class RetryConsumerWorker<TKey, TValue> : ConsumerWorkerBase<TKey, TValue>, IConsumerWorker<TKey, TValue>
    {
        private readonly IKafkaDependentProducer<TKey, TValue> _producer;
        private readonly int _maxRetry;
        private readonly long _retryWait;
        private readonly string _dlqTopic;
        private readonly string _retryTopic;

        public RetryConsumerWorker(KafkaConfig option,
            ILogger<RetryConsumerWorker<TKey, TValue>> logger,
            IMessageHandler<TKey, TValue> messageHandler,
            IDependentConsumer<TKey, TValue> consumer,
            IKafkaDependentProducer<TKey, TValue> producer) : base(option, logger, messageHandler, consumer)
        {
            _producer = producer;
            var retry = MessageHandler.GetType().GetCustomAttribute<RetryAttribute>();

            _maxRetry = retry.MaxRetry;
            _retryWait = retry.Wait;

            _dlqTopic = $"{Topic}__{option.Consumer.GroupId}__{retry.DlqPostfix}";
            _retryTopic = $"{Topic}__{option.Consumer.GroupId}__{retry.RetryPostfix}";
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        private void StartConsumerLoop(CancellationToken cancellationToken)
        {
            Consumer.Subscribe(Topic);
            Consumer.Subscribe(_retryTopic);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = Consumer.Consume(cancellationToken);

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
                        isHandled = MessageHandler.Handle(message);
                    }
                    else
                    {
                        var waitUntil = message.Timestamp.UtcDateTime.AddMilliseconds(_retryWait);
                        if (waitUntil <= DateTimeOffset.UtcNow)
                        {
                            isHandled = MessageHandler.Handle(message);
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

                    if (retryCounter + 1 <= _maxRetry)
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
                    Logger.LogError($"Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    Logger.LogError($"Unexpected error: {e}");
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
