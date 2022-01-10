using System;
using System.Text;
using RabbitMQ.Client;

namespace Publisher
{
    class EmitLog
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //exclusively declare an exchange with fanout (send to all queues)
                channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

                var message = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "logs",
                                    routingKey: "", //this value is ignored in fanout exchange
                                    basicProperties: null,
                                    body: body);

                Console.WriteLine("[x] Sent {0}", message);

            }

            Console.WriteLine("Please [enter] to exit.");
            Console.ReadLine();

            //note that if this publisher runs first, all messages will be lost since no queue
            //is bound yet. Only when a queue is bound to the exchange will the message 
            //start queuing up

        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) 
                ? string.Join(" ", args)
                : "info: Hello World!");
        }
    }
}
