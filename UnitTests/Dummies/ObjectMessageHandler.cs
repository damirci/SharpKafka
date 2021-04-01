using Confluent.Kafka;
using SharpKafka;
using SharpKafka.Message;

namespace UnitTests.Dummies
{
    [Topic("test")]
    class ObjectMessageHandler : IMessageHandler<string, TestMessage>
    {
        bool IMessageHandler<string, TestMessage>.Handle(Message<string, TestMessage> message)
        {
            throw new System.NotImplementedException();
        }
    }
}
