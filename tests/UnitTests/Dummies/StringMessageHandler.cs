using Confluent.Kafka;
using SharpKafka;
using SharpKafka.Message;
using System;

namespace UnitTests.Dummies
{
    [Topic("test")]
    public class StringMessageHandler : IMessageHandler<Null, string>
    {
        public bool Handle(Message<Null, string> message)
        {
            throw new NotImplementedException();
        }
    }
}
