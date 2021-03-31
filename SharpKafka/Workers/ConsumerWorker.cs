using Microsoft.Extensions.Hosting;
using SharpKafka.Consumer;
using SharpKafka.Message;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SharpKafka.Workers
{
    public class ConsumerWorker<TKey,TValue> : BackgroundService
    {
        private readonly string _topic;
        private readonly IKafkaConsumer<TKey, TValue> _consumer;

        public ConsumerWorker(IKafkaConsumer<TKey, TValue> consumer)
        {
            var topic = typeof(TValue).GetCustomAttribute<TopicAttribute>();
            _topic = topic.Name;
            _consumer = consumer;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => _consumer.StartConsumerLoop(_topic,  stoppingToken)).Start();
            return Task.CompletedTask;
        }

        public override void Dispose()
        {
            _consumer.Dispose();
            base.Dispose();
        }
    }
}
