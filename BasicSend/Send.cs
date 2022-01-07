using System;
using System.Text;
using RabbitMQ.Client;

namespace BasicSend
{
    class Send
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "hello", //will only get created if it doesn't exist already
                    durable: false,  //should this queue survive following broker restart
                    exclusive: false,
                    autoDelete: false, //should this queue be autoDelete if no consumers?
                    arguments: null);

                string message = "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "",
                                    routingKey: "hello",
                                    basicProperties: null,
                                    body: body);

                Console.WriteLine(" [x] Sent {0}", message);

            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
