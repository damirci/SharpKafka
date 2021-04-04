using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using SharpKafka.Producer;
using System;
using System.Threading.Tasks;

namespace Web.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class SimpleStringController : ControllerBase
    {
        private readonly ILogger<SimpleStringController> _logger;
        private readonly IKafkaDependentProducer<Null, string> _kafkaDependentProducer;

        public SimpleStringController(ILogger<SimpleStringController> logger, IKafkaDependentProducer<Null,string> kafkaDependentProducer)
        {
            _logger = logger;
            _kafkaDependentProducer = kafkaDependentProducer;
        }

        [HttpGet]
        public async Task<IActionResult> GetAsync()
        {
            var message = $"Date: {DateTime.Now}";
            var dateTopic = "topic-name";

            _logger.LogInformation($"{dateTopic}: {message}");

            await _kafkaDependentProducer.ProduceAsync(dateTopic, new Message<Null, string> { Value = message });
            return Ok();
        }
    }
}
