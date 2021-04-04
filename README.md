# SharpKafka
[![License: Apache](https://img.shields.io/github/license/damirci/SharpKafka)](https://opensource.org/licenses/Apache-2.0)
[![Last Tag](https://img.shields.io/github/v/tag/damirci/SharpKafka)](https://github.com/damirci/SharpKafka/tags)
[![Nuget Download](https://img.shields.io/nuget/dt/SharpKafka)](https://www.nuget.org/packages/SharpKafka/)


**SharpKafka** is a simple library to work with [Apache Kafka](http://kafka.apache.org/) actually a wrapper on top confluent-kafka-dotnet

## Usage

Take a look in the [examples](examples) directory for usage.

For an overview of configuration properties, refer to the [librdkafka documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

### Simple Producer Examples


```csharp
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
```

### Simple Consumer Example


```csharp
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SharpKafka;
using SharpKafka.Extentions;
using SharpKafka.Message;

namespace SimpleConsumerWorker
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
                });
    }

    [Topic(name: "simple-message-topic")]
    public class ConsumerMessageHandler : IMessageHandler<Null, string>
    {
        private readonly ILogger<ConsumerMessageHandler> _logger;

        public ConsumerMessageHandler(ILogger<ConsumerMessageHandler> logger)
        {
            _logger = logger;
        }
        public bool Handle(Message<Null, string> message)
        {
            _logger.LogInformation("message consumed: {message}",message.Value);
            //if message consumtion is succesful return true
            return true;
        }
    }
}

```
