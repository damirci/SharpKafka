using System;

namespace SharpKafka.Message
{
    [AttributeUsage(AttributeTargets.All,
              AllowMultiple = false,
              Inherited = true)]
    public class RetryAttribute : Attribute
    {
        /// <summary>
        /// waiting time between each try
        /// </summary>
        public long Wait { get; set; } = 1000;
        public int MaxRetry { get; set; } = 1;
        /// <summary>
        /// sample topic name: topic-name__consumer-group__postfix
        /// </summary>
        public string RetryPostfix { get; set; } = "retry";
        /// <summary>
        /// sample topic name: topic-name__consumer-group__postfix
        /// </summary>
        public string DlqPostfix { get; set; } = "dlq";
    }
}
