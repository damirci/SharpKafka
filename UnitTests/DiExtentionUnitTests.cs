using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using SharpKafka.Extentions;
using SharpKafka.Producer;
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
            var producerClassType = typeof(KafkaDependentProducer<,>);
            var producerType = Type.MakeGenericSignatureType(producerClassType, typeof(Null), typeof(TestMessage));

            //act
            services.AddSharpKafka(new SharpKafka.KafkaConfig { Producer = new ProducerConfig() { BootstrapServers="test" } }, typeof(DiExtentionUnitTests));
            var provider = services.BuildServiceProvider();
            var producer = provider.GetRequiredService<IKafkaDependentProducer<Null, TestMessage>>();

            //assert
            Assert.NotNull(producer);
        }
    }
}
