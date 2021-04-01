using System;

namespace SharpKafka.Message
{
    [AttributeUsage(AttributeTargets.All,
              AllowMultiple = false,
              Inherited = true)]
    public class RetryAttribute : Attribute
    {
        public long Wait { get; set; }
        public int MaxRetry { get; set; }
        /// <summary>
        /// sample topic name: topic-name__consumer-group__postfix
        /// </summary>
        public string TopicPostfix { get; set; }
        /// <summary>
        /// sample topic name: topic-name__consumer-group__postfix
        /// </summary>
        public string DlqPostfix { get; set; }
    }
}
