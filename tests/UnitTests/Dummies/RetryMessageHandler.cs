using Confluent.Kafka;
using SharpKafka.Message;
using System;

namespace UnitTests.Dummies
{
    [Topic("test")]
    [Retry]
    public class RetryMessageHandler : IMessageHandler<Null, DummyMessage>
    {
        public bool Handle(Message<Null, DummyMessage> message)
        {
            throw new NotImplementedException();
        }
    }
}
