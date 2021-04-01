using SharpKafka.Message;

namespace Web.Messages
{
    [Topic("dummy-message-topic")]
    public class DummyMessage
    {
        public string Content { get; set; }
    }
}
