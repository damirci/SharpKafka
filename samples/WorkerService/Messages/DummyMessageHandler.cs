using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SharpKafka.Message;

namespace WorkerService.Messages
{
    [Topic("dummy-message-topic")]
    [Retry]//if you need to retry the failed message hanling
    public class DummyMessageHandler : IMessageHandler<Null, DummyMessage>
    {
        private readonly ILogger<DummyMessageHandler> _logger;

        public DummyMessageHandler(ILogger<DummyMessageHandler> logger)
        {
            _logger = logger;
        }

        public bool Handle(Message<Null, DummyMessage> message)
        {
            _logger.LogInformation($"a dummy messages handled: {message.Value.Content}");
            return true;
        }
    }
}
