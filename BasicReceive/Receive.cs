using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace BasicReceive
{
    class Receive
    {
        static void Main()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())

            using (var channel = connection.CreateModel())
            {
                //in consumer we need to declare queue as well, since we might start consumer
                //before publisher. This will ensure queue exists
                channel.QueueDeclare(queue: "hello",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                //provide callback for when message gets pushed to this consumer
                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                };

                //autoAck means messages are marked for deletion as soon as they are delivered to consumer
                //so what happens if consumer dies while message is being processed (more complex message)?
                //message is lost,this issue is resolved in workqueues project
                channel.BasicConsume(queue: "hello",
                    autoAck: true, 
                    consumer: consumer);

                Console.WriteLine("Please enter to exit");
                Console.ReadLine();
            }

            

        }
    }
}
