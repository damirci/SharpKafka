using Xunit;
using System;
using Confluent.Kafka;
using SharpKafka.Producer;
using Moq;
using SharpKafka.Message;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading;
using UnitTests.Dummies;

namespace SharpKafka.Workers.Tests
{
    public class RetryConsumerWorkerTests
    {
        CancellationTokenSource cancelationSource = new();
        readonly ConsumeResult<Null, string> message = new()
        {
            Message = new Message<Null, string> { Value = "test message", Headers = new() }
        };
        readonly ConsumeResult<Null, string> nullMessage = new()
        {
            Message = null
        };
        readonly KafkaConfig option = new() { Consumer = new() { GroupId = "test-group-id" } };

        [Fact()]
        public void Should_subscribe_topic_and_retrytopic_before_consume()
        {
            //arrange
            var procuderMock = new Mock<IKafkaDependentProducer<Null, string>>();
            var messageHandlerMock = new Mock<RetryMessageHandler>();

            var consumerMock = new Mock<IConsumer<Null, string>>();
            consumerMock
                .Setup(c => c.Consume(cancelationSource.Token))
                .Returns(nullMessage).Callback(()=> {
                    cancelationSource.Cancel();
                });

            consumerMock
                .Setup(c => c.Subscribe(It.IsAny<string[]>()));

            var dependentConsumerMock = new Mock<IDependentConsumer<Null, string>>();
            dependentConsumerMock
                .Setup(c => c.Consumer)
                .Returns(consumerMock.Object);

            var retryWorker = new RetryConsumerWorker<Null, string>(
                option,
                NullLogger<RetryConsumerWorker<Null, string>>.Instance,
                messageHandlerMock.Object,
                dependentConsumerMock.Object,
                procuderMock.Object);

            //act
            retryWorker.StartAsync(cancelationSource.Token);

            //arrange
            consumerMock.Verify(h => h.Subscribe(It.IsAny<string[]>()), Times.Once);
        }

        [Fact()]
        public void Should_handle_message_once()
        {
            //arrange
            var procuderMock = new Mock<IKafkaDependentProducer<Null, string>>();

            var messageHandlerMock = new Mock<RetryMessageHandler>();
            messageHandlerMock
                .Setup(h => h.Handle(It.IsAny<Message<Null, string>>()))
                .Returns(true);

            var consumerMock = new Mock<IConsumer<Null, string>>();
            var messageConsumeCounter = 0;
            consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Callback(() => {
                    if (messageConsumeCounter > 4)
                    {
                        cancelationSource.Cancel();//stops worker after 4 try for consume
                    }
                    messageConsumeCounter++;
                });
            consumerMock
                .SetupSequence(c => c.Consume(It.IsAny<CancellationToken>()))
                .Returns(message)
                .Returns(nullMessage);

            consumerMock
                .Setup(c => c.Subscribe(It.IsAny<string>()));

            var dependentConsumerMock = new Mock<IDependentConsumer<Null, string>>();
            dependentConsumerMock
                .Setup(c => c.Consumer)
                .Returns(consumerMock.Object);

            var retryWorker = new RetryConsumerWorker<Null, string>(
                option,
                NullLogger<RetryConsumerWorker<Null, string>>.Instance,
                messageHandlerMock.Object,
                dependentConsumerMock.Object,
                procuderMock.Object);

            //act
            retryWorker.StartAsync(cancelationSource.Token);

            //arrange
            messageHandlerMock.Verify(h => h.Handle(message.Message), Times.Once);
        }

        [Fact()]
        public void Should_produce_retry_message_5_time_and_dlq_once()
        {
            //arrange
            var procuderMock = new Mock<IKafkaDependentProducer<Null, string>>();
            procuderMock
                .Setup(p => p.Produce(It.IsAny<string>(), It.IsAny<Message<Null, string>>(), null))
                .Callback<string, Message<Null, string>, Action<DeliveryReport<Null, string>>>((topic,msg,deliverHandler)=> {
                    message.Message = msg;
                });

            var messageHandlerMock = new Mock<RetryMessageHandler>();
            messageHandlerMock
                .Setup(h => h.Handle(It.IsAny<Message<Null, string>>()))
                .Returns(false);

            var consumerMock = new Mock<IConsumer<Null, string>>();

            var messageConsumeCounter = 0;
            consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Callback(() => {
                    if (messageConsumeCounter > 7)
                    {
                        cancelationSource.Cancel();//stops worker after 7 try for consume
                    }
                    messageConsumeCounter++;
                });

            consumerMock
                .SetupSequence(c => c.Consume(It.IsAny<CancellationToken>()))
                .Returns(message)//1
                .Returns(message)//2
                .Returns(message)//3
                .Returns(message)//4
                .Returns(message)//5
                .Returns(message)//6
                .Returns(nullMessage);

            var dependentConsumerMock = new Mock<IDependentConsumer<Null, string>>();
            dependentConsumerMock
                .Setup(c => c.Consumer)
                .Returns(consumerMock.Object);

            var retryWorker = new RetryConsumerWorker<Null, string>(
                option,
                NullLogger<RetryConsumerWorker<Null, string>>.Instance,
                messageHandlerMock.Object,
                dependentConsumerMock.Object,
                procuderMock.Object);

            var topic = $"test";
            var retryTopic = $"{topic}__{option.Consumer.GroupId}__{new RetryAttribute().RetryPostfix}";
            var dlqTopic = $"{topic}__{option.Consumer.GroupId}__{new RetryAttribute().DlqPostfix}";

            //act
            retryWorker.StartAsync(cancelationSource.Token);

            //arrange
            procuderMock.Verify(h => h.Produce(retryTopic, It.IsAny<Message<Null, string>>(), null), Times.Exactly(5));
            procuderMock.Verify(h => h.Produce(dlqTopic, It.IsAny<Message<Null, string>>(), null), Times.Once);
        }
    }
}