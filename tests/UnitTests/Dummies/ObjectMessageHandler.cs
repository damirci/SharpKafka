using Confluent.Kafka;
using SharpKafka.Message;

namespace UnitTests.Dummies
{
    [Topic("test")]
    class ObjectMessageHandler : IMessageHandler<string, TestMessage>
    {
        public bool Handle(Message<string, TestMessage> message)
        {
            throw new System.NotImplementedException();
        }
    }
}
