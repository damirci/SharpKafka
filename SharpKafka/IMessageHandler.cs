using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SharpKafka
{
    public interface IMessageHandler<TKey,TValue>
    {
        public bool Handle(Message<TKey, TValue> message);
    }
}
