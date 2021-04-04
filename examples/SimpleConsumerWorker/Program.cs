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
