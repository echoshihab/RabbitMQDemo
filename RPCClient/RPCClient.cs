using System;
using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RPCClient
{
    public class RPCClient
    {
        private readonly IConnection connection;
        private readonly IModel channel;
        private readonly string replyQueueName;
        private readonly EventingBasicConsumer consumer;
        private readonly BlockingCollection<string> respQueue = new BlockingCollection<string>();
        private readonly IBasicProperties props;

        public RPCClient()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            this.connection = factory.CreateConnection(); //establish connection
            this.channel = this.connection.CreateModel(); //establish channel
            this.replyQueueName = this.channel.QueueDeclare().QueueName; //generate queue name (callback queue)
            this.consumer = new EventingBasicConsumer(channel);

            props = this.channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            //used to correlate RPC responses with request
            this.props.CorrelationId = correlationId;
            //when server replies with a response message, it will be sent back to this callback queue
            this.props.ReplyTo = this.replyQueueName; 


            this.consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var response = Encoding.UTF8.GetString(body);
                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    this.respQueue.Add(response);
                }
            };

            this.channel.BasicConsume(
                consumer: this.consumer,
                queue: this.replyQueueName,
                autoAck: true);

        }

        public string Call(string message)
        {
            var messageBytes = Encoding.UTF8.GetBytes(message);
            this.channel.BasicPublish(
                exchange: "",
                routingKey: "rpc_queue",
                basicProperties: this.props,
                body: messageBytes);

            return this.respQueue.Take();
        }

        public void Close()
        {
            this.connection.Close();
        }

        public class RPC
        {
            public static void Main()
            {
                var rpcClient = new RPCClient();
                Console.WriteLine(" [x] Requesting fib(30)");
                var response = rpcClient.Call("30");

                Console.WriteLine(" [.] Got '{0}'", response);
                rpcClient.Close();
                Console.ReadLine();
            }
        }
    }
}
