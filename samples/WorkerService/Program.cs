using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpKafka;
using SharpKafka.Extentions;
using WorkerService.Messages;

namespace WorkerService
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
                    IConfiguration configuration = hostContext.Configuration;
                    var kafkaConfig = configuration.GetSection("Kafka").Get<KafkaConfig>();
                    services.AddSharpKafka(kafkaConfig, typeof(Program));
                    services.AddTransient((sp) => new JsonDeserializer<DummyMessage>().AsSyncOverAsync());

                });
    }
}
