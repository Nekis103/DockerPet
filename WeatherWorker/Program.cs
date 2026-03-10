using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Newtonsoft.Json;

namespace WeatherWorker;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("Weather Worker запущен...");
        Console.WriteLine("Ожидаю RabbitMQ...");

        var factory = new ConnectionFactory()
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest",
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
        };

        IConnection connection = null;
        int retryCount = 0;
        const int maxRetries = 30;

        // Пытаемся подключиться с повторными попытками
        while (connection == null && retryCount < maxRetries)
        {
            try
            {
                Console.WriteLine($"Попытка подключения к RabbitMQ #{retryCount + 1}...");
                connection = factory.CreateConnection();
                Console.WriteLine("Подключение к RabbitMQ установлено!");
            }
            catch (Exception ex)
            {
                retryCount++;
                Console.WriteLine($"Не удалось подключиться к RabbitMQ. Попытка {retryCount} из {maxRetries}");
                Console.WriteLine($"Ошибка: {ex.Message}");

                if (retryCount >= maxRetries)
                {
                    Console.WriteLine("Превышено количество попыток подключения. Завершение работы.");
                    return;
                }

                Console.WriteLine("Повторная попытка через 5 секунд...");
                await Task.Delay(5000);
            }
        }

        if (connection == null)
        {
            Console.WriteLine("Не удалось подключиться к RabbitMQ. Завершение работы.");
            return;
        }

        using (var channel = connection.CreateModel())
        {
            // Объявляем очередь
            channel.QueueDeclare(
                queue: "weather_requests",
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );

            Console.WriteLine("Ожидаю задания на получение погоды...");

            // Создаем потребителя
            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var weatherTask = JsonConvert.DeserializeObject<dynamic>(message);

                    string city = weatherTask?.City;
                    string requestId = weatherTask?.RequestId;

                    Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss}] Получен запрос #{requestId}");
                    Console.WriteLine($"Запрашиваю погоду для города: {city}");

                    // Имитируем получение погоды
                    await Task.Delay(2000);

                    // Генерируем случайную погоду
                    var weather = GenerateRandomWeather();

                    Console.WriteLine($"Погода в {city}: {weather.Temperature}°C, {weather.Description}");
                    Console.WriteLine($"Запрос #{requestId} обработан");

                    // Подтверждаем обработку
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка при обработке сообщения: {ex.Message}");
                    // Отклоняем сообщение и не отправляем обратно в очередь
                    channel.BasicNack(ea.DeliveryTag, false, false);
                }
            };

            // Начинаем прослушивание
            channel.BasicConsume(
                queue: "weather_requests",
                autoAck: false,
                consumer: consumer
            );

            // Держим приложение запущенным
            Console.WriteLine("Worker готов к работе. Нажмите Ctrl+C для выхода.");

            // Бесконечное ожидание
            await Task.Delay(-1);
        }
    }

    static (int Temperature, string Description) GenerateRandomWeather()
    {
        var random = new Random();
        var descriptions = new[]
        {
            "Солнечно",
            "Облачно",
            "Дождливо",
            "Снежно",
            "Ветрено",
            "Туманно"
        };

        return (
            Temperature: random.Next(-10, 30),
            Description: descriptions[random.Next(descriptions.Length)]
        );
    }
}