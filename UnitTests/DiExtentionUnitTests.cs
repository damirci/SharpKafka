using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using SharpKafka;
using SharpKafka.Consumer;
using SharpKafka.Extentions;
using SharpKafka.Producer;
using SharpKafka.Workers;
using System;
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
            services.AddTransient((sp)=> Serializers.Null);
            var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = "localhost"
            });
            services.AddTransient((sp) => new JsonSerializer<TestMessage>(schemaRegistry).AsSyncOverAsync());

            //act
            services.AddSharpKafka(new SharpKafka.KafkaConfig { Producer = new ProducerConfig() { BootstrapServers= "localhost" } }, typeof(DiExtentionUnitTests));
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
        public void Should_Inject_Consumer_Worker_for_Message()
        {
            //arrange
            var services = new ServiceCollection();
            //services.AddTransient((sp) => new JsonDeserializer<TestMessage>().AsSyncOverAsync());
            services.AddScoped(sp=> new Mock<ILogger<ConsumerWorker<Null, string>>>().Object);
            services.AddScoped(sp => new Mock<ILogger<KafkaConsumer<Null, string>>>().Object);
            //act
            services.AddSharpKafka(new KafkaConfig { Consumer = new ConsumerConfig() { BootstrapServers = "localhost" ,GroupId="test"} }, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var worker = provider.GetRequiredService<IConsumerWorker<Null, string>>();

            //assert
            Assert.NotNull(worker);
        }
    }
}
