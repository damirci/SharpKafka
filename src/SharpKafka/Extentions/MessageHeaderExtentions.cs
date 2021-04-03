using Confluent.Kafka;
using System;

namespace SharpKafka.Extentions
{
    public static class MessageHeaderExtentions
    {
        private const string _retryHeader = "retry-counter";

        public static int GetRetryCounter(this Headers headers)
        {
            if (headers.TryGetLastBytes(_retryHeader, out var bytes))
            {
                var value = BitConverter.ToInt32(bytes, 0);
                return value;
            }
            else
            {
                return 0;
            }
        }

        public static void SetRetryCounter(this Headers headers, int value)
        {
            headers.Remove(_retryHeader);
            headers.Add(_retryHeader, BitConverter.GetBytes(value));
        }

    }
}
