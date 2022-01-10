using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer
{
    class ReceiveLogs
    {
        public static void Main()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

                //this will generate a non-durable, exclusive, autodelete queue with 
                //a generated name, meaning this will be deleted as soon as the connection closes
                //or last consumer unsubscribes
                var queueName = channel.QueueDeclare().QueueName;

                channel.QueueBind(queue: queueName,
                                    exchange: "logs",
                                    routingKey: "");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] {0} ", message);
                };

                channel.BasicConsume(queue: queueName,
                    autoAck: true,
                    consumer: consumer);
                Console.WriteLine("Press [enter] to exit");
                Console.ReadLine();

            }
        }
    }
}
