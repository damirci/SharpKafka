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
        const string _KafkaAddress = "localhost";
        readonly KafkaConfig kafkaConfig = new()
        {
            Consumer = new ConsumerConfig()
            {
                BootstrapServers = _KafkaAddress,
                GroupId = "test-cg"
            },
            Producer = new ProducerConfig()
            {
                BootstrapServers = _KafkaAddress
            }
        };

        readonly CachedSchemaRegistryClient schemaRegistry = new(new SchemaRegistryConfig
        {
            Url = _KafkaAddress
        });

        readonly ServiceCollection services = new();

        //pre arrange and setup manual injections
        public DiExtentionUnitTests()
        {
            services.AddTransient((sp) => new JsonDeserializer<TestMessage>().AsSyncOverAsync());
            services.AddTransient((sp) => new JsonSerializer<TestMessage>(schemaRegistry).AsSyncOverAsync());
            services.AddTransient((sp) => new JsonDeserializer<DummyMessage>().AsSyncOverAsync());
            services.AddTransient((sp) => new JsonSerializer<DummyMessage>(schemaRegistry).AsSyncOverAsync());
            services.AddLogging();
        }

        [Fact]
        public void Should_Inject_producer_for_Message()
        {
            //arrange

            //act
            services.AddSharpKafka(kafkaConfig, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var producer = provider.GetRequiredService<IKafkaDependentProducer<Null, TestMessage>>();

            //assert
            Assert.NotNull(producer);
        }

        [Fact]
        public void Should_Inject_TestMessageHandler()
        {
            //arrange

            //act
            services.AddSharpKafka(kafkaConfig, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var messageHandler = provider.GetRequiredService<IMessageHandler<Null, string>>();

            //assert
            Assert.NotNull(messageHandler);
        }

        [Fact]
        public void Should_Inject_Consumer_Worker_for_MessageHandler()
        {
            //arrange
            var expected = typeof(ConsumerWorker<Null, string>);

            //act
            services.AddSharpKafka(kafkaConfig, typeof(DiExtentionUnitTests));
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
            var expected = typeof(ConsumerWorker<string, TestMessage>);

            //act
            services.AddSharpKafka(kafkaConfig, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var hostedServices = provider.GetServices<IHostedService>();

            //assert
            Assert.NotEmpty(hostedServices);
            Assert.Contains(hostedServices, t => t.GetType() == expected);
        }

        [Fact]
        public void Should_Inject_Retry_Consumer_Worker_for_MessageHandler()
        {
            //arrange
            services.AddLogging();


            var expected = typeof(RetryConsumerWorker<Null, DummyMessage>);
            //act
            services.AddSharpKafka(kafkaConfig, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var hostedServices = provider.GetServices<IHostedService>();

            //assert
            Assert.NotEmpty(hostedServices);
            Assert.Contains(hostedServices, t => t.GetType() == expected);
        }

    }
}
