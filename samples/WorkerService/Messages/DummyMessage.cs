using SharpKafka.Message;

namespace WorkerService.Messages
{
    [Topic("dummy-message-topic")]
    public class DummyMessage
    {
        public string Content { get; set; }
    }
}
