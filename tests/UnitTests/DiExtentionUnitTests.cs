using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpKafka;
using SharpKafka.Extentions;
using SharpKafka.Message;
using SharpKafka.Producer;
using SharpKafka.Workers;
using UnitTests.Dummies;
using Xunit;

namespace UnitTests
{
    public class DiExtentionUnitTests
    {
        [Fact]
        public void Should_Inject_producer_for_Message()
        {
            //arrange
            var services = new ServiceCollection();
            var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = "localhost"
            });
            services.AddTransient((sp) => new JsonSerializer<TestMessage>(schemaRegistry).AsSyncOverAsync());
            
            //act
            services.AddSharpKafka(new KafkaConfig { Producer = new ProducerConfig() { BootstrapServers = "localhost" } }, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var producer = provider.GetRequiredService<IKafkaDependentProducer<Null, TestMessage>>();

            //assert
            Assert.NotNull(producer);
        }

        [Fact]
        public void Should_Inject_TestMessageHandler()
        {
            //arrange
            var services = new ServiceCollection();

            //act
            services.AddSharpKafka(new KafkaConfig(), typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var messageHandler = provider.GetRequiredService<IMessageHandler<Null, string>>();

            //assert
            Assert.NotNull(messageHandler);
        }

        [Fact]
        public void Should_Inject_Consumer_Worker_for_MessageHandler()
        {
            //arrange
            var services = new ServiceCollection();
            services.AddLogging();
            var expected = typeof(ConsumerWorker<Null, string>);

            //act
            services.AddSharpKafka(new KafkaConfig { Consumer = new ConsumerConfig() { BootstrapServers = "localhost", GroupId = "test" } }, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var hostedServices = provider.GetServices<IHostedService>();

            //assert
            Assert.NotEmpty(hostedServices);
            Assert.Contains(hostedServices, t => t.GetType() == expected);
        }

        [Fact]
        public void Should_Inject_Object_Consumer_Worker_for_MessageHandler()
        {
            //arrange
            var services = new ServiceCollection();
            services.AddTransient((sp) => new JsonDeserializer<TestMessage>().AsSyncOverAsync());
            services.AddLogging();
            var expected = typeof(ConsumerWorker<string, TestMessage>);

            //act
            services.AddSharpKafka(new KafkaConfig { Consumer = new ConsumerConfig() { BootstrapServers = "localhost", GroupId = "test" } }, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var hostedServices = provider.GetServices<IHostedService>();

            //assert
            Assert.NotEmpty(hostedServices);
            Assert.Contains(hostedServices, t => t.GetType() == expected);
        }
    }
}
