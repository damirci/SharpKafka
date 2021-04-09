using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SharpKafka.Message;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SharpKafka.Workers
{
    public class ConsumerWorker<TKey, TValue> : ConsumerWorkerBase<TKey, TValue>, IConsumerWorker<TKey, TValue>
    {
        public ConsumerWorker(KafkaConfig option,
            ILogger<ConsumerWorker<TKey, TValue>> logger,
            IMessageHandler<TKey, TValue> messageHandler,
            IDependentConsumer<TKey, TValue> consumer) : base(option, logger, messageHandler, consumer)
        {
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        public void StartConsumerLoop(CancellationToken cancellationToken)
        {
            Consumer.Subscribe(Topic);

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
                    _ = MessageHandler.Handle(message);

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


    }
}
