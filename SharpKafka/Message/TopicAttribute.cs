using System;

namespace SharpKafka.Message
{
    [AttributeUsage(AttributeTargets.All,
                  AllowMultiple = false,
                  Inherited = true)]
    public class TopicAttribute : Attribute
    {
        public TopicAttribute(string name, Type keyType)
        {
            Name = name;
            KeyType = keyType;
        }

        public string Name { get; set; }
        public Type KeyType { get; set; }
    }
}
