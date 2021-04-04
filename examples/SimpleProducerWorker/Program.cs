using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpKafka;
using SharpKafka.Extentions;
using SharpKafka.Producer;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleProducerWorker
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    var kafkaConfig = hostContext.Configuration.GetSection("Kafka").Get<KafkaConfig>();
                    services.AddSharpKafka(kafkaConfig, typeof(Program));

                    services.AddHostedService<Worker>();
                });
    }

    public class Worker : BackgroundService
    {
        private readonly IKafkaDependentProducer<Null, string> _stringProduer;

        public Worker(IKafkaDependentProducer<Null, string> stringProduer)
        {
            _stringProduer = stringProduer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _stringProduer.Produce("simple-message-topic",
                    new Message<Null, string> { Value = $"producer running at: {DateTimeOffset.Now}" });
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
