using Microsoft.Extensions.DependencyInjection;
using SharpKafka.Extentions;
using Xunit;

namespace UnitTests
{
    public class DiExtentionUnitTests
    {
        [Fact]
        public void Test1()
        {
            var services = new ServiceCollection();
            services.AddSharpKafka(new SharpKafka.KafkaConfig(), (typeof(DiExtentionUnitTests)));
        }
    }
}
