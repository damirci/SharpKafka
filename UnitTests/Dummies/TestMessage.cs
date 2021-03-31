using SharpKafka.Message;

namespace UnitTests.Dummies
{
    [Topic("test")]
    public class TestMessage : IMessage
    {
    }
}
