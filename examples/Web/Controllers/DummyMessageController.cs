using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using SharpKafka.Producer;
using System;
using System.Threading.Tasks;
using Web.Messages;

namespace Web.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class DummyMessageController : ControllerBase
    {
        private readonly ILogger<DummyMessageController> _logger;
        private readonly IKafkaDependentProducer<Null, DummyMessage> _kafkaDependentProducer;

        public DummyMessageController(ILogger<DummyMessageController> logger, IKafkaDependentProducer<Null, DummyMessage> kafkaDependentProducer)
        {
            _logger = logger;
            _kafkaDependentProducer = kafkaDependentProducer;
        }

        [HttpGet]
        public async Task<IActionResult> GetAsync()
        {
            var content = $"dummy message number {DateTime.Now.Ticks}";

            _logger.LogInformation($" message : {content}");

            await _kafkaDependentProducer.ProduceAsync(default, new DummyMessage { Content = content });
            return Ok();
        }
    }
}
