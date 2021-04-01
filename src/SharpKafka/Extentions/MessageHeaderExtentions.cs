using Confluent.Kafka;
using System;

namespace SharpKafka.Extentions
{
    public static class MessageHeaderExtentions
    {
        private const string _retryHeader = "retry-counter";
        private const string _waitHeader = "retry-wait";

        public static int GetRetryCounter(this Headers headers)
        {
            if (headers.TryGetLastBytes(_retryHeader, out var bytes))
            {
                var value = BitConverter.ToInt32(bytes);
                return value;
            }
            else
            {
                return -1;
            }
        }

        public static void SetRetryCounter(this Headers headers, int value)
        {
            headers.Remove(_retryHeader);
            headers.Add(_retryHeader, BitConverter.GetBytes(value));
        }

        public static TimeSpan GetRetryWait(this Headers headers)
        {
            if (headers.TryGetLastBytes(_waitHeader, out var bytes))
            {
                var value = BitConverter.ToInt64(bytes);
                return new TimeSpan(value) ;
            }
            else
            {
                return TimeSpan.Zero;
            }
        }

        public static void SetRetryWait(this Headers headers, TimeSpan value)
        {
            headers.Remove(_waitHeader);
            headers.Add(_waitHeader, BitConverter.GetBytes(value.Ticks));
        }

    }
}
