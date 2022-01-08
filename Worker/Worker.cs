using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Worker
{
    class Worker
    {
        static void Main()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())

            using (var channel = connection.CreateModel())
            {
                //in consumer we need to declare queue as well, since we might start consumer
                //before publisher. This will ensure queue exists
                channel.QueueDeclare(queue: "task_queue",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                //this ensures that this worker won't receive another message
                //when it is busy, so next message will get passed on to the next non-busy worker/consumer
                //warning: if all workers are busy, queue may get filled up, so this needs to be monitored
                //and more worker added if needed
                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                Console.WriteLine("[*] waiting for messages.");

                //provide callback for when message gets pushed to this consumer
                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (sender, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);

                    int dots = message.Split('.').Length -1;
                    Thread.Sleep(dots * 10000);

                    Console.WriteLine("[x] Done");

                    //manually acknowledging delivery
                    //using this approach, even if this instance of worker dies while
                    //processing message, the message will be redelivered.
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };

                channel.BasicConsume(queue: "task_queue",
                    autoAck: false,  //so that message do not get marked automatically for deletion
                    consumer: consumer);

                Console.WriteLine("Please enter to exit");
                Console.ReadLine();
            }

            

        }
    }
    
}
