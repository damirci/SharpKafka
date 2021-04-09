using Xunit;
using SharpKafka.Workers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using SharpKafka.Producer;
using Moq;
using SharpKafka.Message;
using System.ComponentModel;
using Microsoft.Extensions.Logging.Abstractions;

namespace SharpKafka.Workers.Tests
{
    public class RetryConsumerWorkerTests
    {
        [Fact()]
        public void RetryConsumerWorkerTest()
        {
            //arrange
            var procuderMock = new Mock<IKafkaDependentProducer<Null, string>>();
            procuderMock
                .Setup(p => p.Produce(It.IsAny<string>(), It.IsAny<Message<Null, string>>(), null));

            var messageHandlerMock = new Mock<IMessageHandler<Null, string>>();
            messageHandlerMock
                .Setup(h => h.Handle(It.IsAny<Message<Null, string>>()))
                .Returns(false);

            TypeDescriptor.AddAttributes(messageHandlerMock.Object, new RetryAttribute());

            var retryWorker = new RetryConsumerWorker<Null, string>(
                new KafkaConfig { },
                NullLogger<RetryConsumerWorker<Null, string>>.Instance,
                messageHandlerMock.Object,
                Deserializers.Null,
                Deserializers.Utf8,
                procuderMock.Object);

            //act


            //arrange

        }

        [Fact()]
        public void DisposeTest()
        {
            Assert.True(false, "This test needs an implementation");
        }
    }
}