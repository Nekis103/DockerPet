using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace WeatherAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly IConnection _rabbitConnection;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;

            // Подключаемся к RabbitMQ
            var factory = new ConnectionFactory()
            {
                HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost", //"localhost",  // Позже заменим на "rabbitmq" в Docker
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };

            _rabbitConnection = factory.CreateConnection();
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public IEnumerable<WeatherForecast> Get()
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        // GET: api/weather/request?city=Moscow
        [HttpGet("request")]
        public IActionResult RequestWeather(string city)
        {
            try
            {
                // Создаем задание для очереди
                var weatherTask = new
                {
                    City = city,
                    RequestId = Guid.NewGuid().ToString(),
                    RequestedAt = DateTime.Now
                };

                // Отправляем в очередь RabbitMQ
                using (var channel = _rabbitConnection.CreateModel())
                {
                    channel.QueueDeclare(
                        queue: "weather_requests",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    var message = JsonConvert.SerializeObject(weatherTask);
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(
                        exchange: "",
                        routingKey: "weather_requests",
                        basicProperties: null,
                        body: body
                    );
                }

                _logger.LogInformation($"Запрос погоды для города {city} отправлен в очередь");

                return Ok(new
                {
                    Message = $"Запрос погоды для {city} принят в обработку",
                    RequestId = weatherTask.RequestId
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при отправке в очередь");
                return StatusCode(500, "Ошибка при обработке запроса");
            }
        }
    }
}
