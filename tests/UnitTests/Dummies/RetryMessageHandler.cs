using Confluent.Kafka;
using SharpKafka.Message;
using System;

namespace UnitTests.Dummies
{
    [Topic("test")]
    [Retry(MaxRetry =5)]
    public class RetryMessageHandler : IMessageHandler<Null, string>
    {
        public virtual bool Handle(Message<Null, string> message)
        {
            return false;
        }
    }
}
