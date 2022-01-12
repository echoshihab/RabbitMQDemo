using RabbitMQ.Client;
using System;
using System.Text;


namespace WorkQueues
{
    class NewTask
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "task_queue", //will only get created if it doesn't exist already
                    durable: true,  //should this queue survive following broker restart
                    exclusive: false,
                    autoDelete: false, //should this queue be auto Deleted if no consumers?
                    arguments: null);

                string message = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);
                var properties = channel.CreateBasicProperties();

                //this ensures messages are persistent (will survive broker restart)
                //this needs to be set even if a queue is marked as durable, otherwise
                //only queue will survive the restart and not messages
                //Note: this is not 100% fail safe. There is a short window where
                //rabbitMQ accepts the message but hasn't save the message to disk yet,
                //so a broker restart during this time may end  in loss of message
                //for a more fail safe approach, we can use "publisher confirms"
                properties.Persistent = true;

                channel.BasicPublish(exchange: "",
                    routingKey: "task_queue",
                    basicProperties: properties,
                    body: body);

                Console.WriteLine(" [x] Sent {0}", message);

            }

            //Console.WriteLine("Press [enter] to exit.");
            //Console.ReadLine();
        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args): "Hello World!");
        }
    }
}
