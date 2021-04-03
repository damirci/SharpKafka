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
    public abstract class ConsumerWorkerBase<TKey, TValue> : BackgroundService, IConsumerWorker<TKey, TValue>
    {
        protected readonly string Topic;
        public ConsumerWorkerBase(KafkaConfig option,
        ILogger<IConsumerWorker<TKey, TValue>> logger,
        IMessageHandler<TKey, TValue> messageHandler,
        IDeserializer<TKey> keyDersializer,
        IDeserializer<TValue> valueDersializer)
        {
            Config = option.Consumer;
            Consumer = new ConsumerBuilder<TKey, TValue>(Config)
                .SetKeyDeserializer(keyDersializer)
                .SetValueDeserializer(valueDersializer)
                .Build();
            MessageHandler = messageHandler;
            var topic = messageHandler.GetType().GetCustomAttribute<TopicAttribute>();
            Topic = topic.Name;
            Logger = logger;
            MessageHandler = messageHandler;
        }

        public ILogger<IConsumerWorker<TKey, TValue>> Logger { get; }
        public IMessageHandler<TKey, TValue> MessageHandler { get; }
        public IConsumer<TKey, TValue> Consumer { get; }
        public ConsumerConfig Config { get; }

        public override void Dispose()
        {
            GC.SuppressFinalize(this);
            Consumer.Close(); // Commit offsets and leave the group cleanly.
            Consumer.Dispose();
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            base.StartAsync(cancellationToken);
            Logger.LogInformation($"{Topic}: Kafka consumer for started");
            return Task.CompletedTask;
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            base.StopAsync(cancellationToken);
            Logger.LogInformation($"{Topic}: Kafka consumer for started");
            return Task.CompletedTask;
        }
    }
}
