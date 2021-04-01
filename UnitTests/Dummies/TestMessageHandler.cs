using Confluent.Kafka;
using SharpKafka;
using SharpKafka.Message;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.Dummies
{
    [Topic("test")]
    public class TestMessageHandler : IMessageHandler<Null, string>
    {
        public bool Handle(Message<Null, string> message)
        {
            throw new NotImplementedException();
        }
    }
}
