using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace EmitLogDirect
{
    class EmitLogDirect
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //in direct exchange, a message goes to the queues
                //whose binding key exactly matches the routing key of the message
                channel.ExchangeDeclare(exchange: "direct_logs",
                    type: "direct");

                var severity = (args.Length > 0) ? args[0] : "info";
                var message = (args.Length > 1)
                    ? string.Join(" ", args.Skip(1).ToArray())
                    : "Hello World!";

                var body = Encoding.UTF8.GetBytes(message);

                //if this routing key defined below  matches exactly the binding key as defined in 
                //consumer, it will route to that queue
                //if no queue is available with with the matching binding key
                //message will be discarded
                channel.BasicPublish(exchange: "direct_logs",
                                    routingKey: severity, 
                                    basicProperties: null,
                                    body: body);

                Console.WriteLine(" [x] Sent '{0}' : '{1}'", severity, message);

            }

            //Console.WriteLine("Press [enter] to exit.");
            //Console.ReadLine();
        }
    }
}
