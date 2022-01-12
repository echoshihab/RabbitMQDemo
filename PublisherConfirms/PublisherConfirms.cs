using System;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using RabbitMQ.Client;

namespace PublisherConfirms
{
    //when publisher confirms are enabled on a channel, messages the client publishes
    //are confirmed asyncronously by the broker, meaning they have been taken care of the server side
    public class PublisherConfirms
    {
        private const int MESSAGE_COUNT = 50_000;

        public static void Main()
        {
            PublishMessagesIndividually();
            PublishMessagesInBatch();
            HandlePublishConfirmsAsynchronously();
            Console.ReadLine();
        }

        private static IConnection CreateConnection()
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            return factory.CreateConnection();
        }

        private static void PublishMessagesIndividually()
        {
            using(var connection = CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //declare a server-named queue

                var queueName = channel.QueueDeclare().QueueName;
                //publisher confirms is a rabbitmq extension to AMQP 0.9.1 protocol 
                //it is enabled at the channel level with the confirm select method like below
                //confirms should e enabled just once not for every message published
                channel.ConfirmSelect();

                var timer = new Stopwatch();
                timer.Start();

                for (int i = 0; i < MESSAGE_COUNT; i++)
                {
                    var body = Encoding.UTF8.GetBytes(i.ToString());
                    channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body:body);
                    //wait for confirmation with a timeout
                    //this will throw exception that we will need to handle if not confirmed within specified time
                    channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));
                }

                timer.Stop();
                Console.WriteLine($"Published {MESSAGE_COUNT:N0} messages individually in {timer.ElapsedMilliseconds:N0} ms");

            }
        }

        //Waiting for a batch of messages to be confirmed improves throughput drastically
        //over waiting for a confirm for individual message (up to 20-30 times with a remote
        //RabbitMQ node). One drawback is that we do not know exactly what went wrong in case
        //of failure, so we may have to keep a whole batch in memory to log something meaningful
        //or to re-publish the messages. And this solution is still synchronous, so it blocks the
        //publishing of messages.

        private static void PublishMessagesInBatch()
        {
            using(var connection = CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //declare a server named queue
                var queueName = channel.QueueDeclare().QueueName;
                channel.ConfirmSelect();

                var batchSize = 100;
                var outstandingMessagecount = 0;
                var timer = new Stopwatch();
                timer.Start();
                for (int i = 0; i < MESSAGE_COUNT; i++)
                {
                    var body = Encoding.UTF8.GetBytes(i.ToString());
                    channel.BasicPublish(exchange:"", routingKey: queueName, basicProperties: null, body: body);
                    outstandingMessagecount++;

                    if (outstandingMessagecount == batchSize)
                    {
                        channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));
                        outstandingMessagecount = 0;
                    }
                }

                if(outstandingMessagecount > 0)
                    channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));

                timer.Stop();
                Console.WriteLine($"Published {MESSAGE_COUNT:N0} messages in batch in {timer.ElapsedMilliseconds:N0} ms");

            }
        }

        private static void HandlePublishConfirmsAsynchronously()
        {
            using(var connection = CreateConnection())
            using (var channel = connection.CreateModel())
            {
                //declare a server-named queue
                var queueName = channel.QueueDeclare().QueueName;
                channel.ConfirmSelect();

                var outstandingConfirms = new ConcurrentDictionary<ulong, string>();

                void CleanOutstandigConfirms(ulong sequenceNumber, bool multiple)
                {
                    if (multiple)
                    {
                        var confirmed = outstandingConfirms.Where(k => k.Key <= sequenceNumber);
                        foreach (var entry in confirmed)
                            outstandingConfirms.TryRemove(entry.Key, out _);
                    }
                    else
                    {
                        outstandingConfirms.TryRemove(sequenceNumber, out _);
                    }
                }

                //We need to clean this dictionary when confirms arrive and do something like logging a warning when messages are nack-ed:
                channel.BasicAcks += (sender, ea) => CleanOutstandigConfirms(ea.DeliveryTag, ea.Multiple);
                channel.BasicNacks += (sender, ea) =>
                {
                    outstandingConfirms.TryGetValue(ea.DeliveryTag, out string body);
                    Console.WriteLine($"Message with body {body} has been nack-ed. Sequence number: {ea.DeliveryTag}, multiple: {ea.Multiple}");
                    CleanOutstandigConfirms(ea.DeliveryTag, ea.Multiple);
                };

                var timer = new Stopwatch();
                timer.Start();

                for (int i = 0; i < MESSAGE_COUNT; i++)
                {
                    var body = i.ToString();
                    //The publishing code now tracks outbound messages with a dictionary. We need to clean this dictionary when confirms
                    outstandingConfirms.TryAdd(channel.NextPublishSeqNo, i.ToString());
                    channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body: Encoding.UTF8.GetBytes(body));
                }

                if (!WaitUntil(60, () => outstandingConfirms.IsEmpty))
                    throw new Exception("All messages could not be confirmed in 60 seconds");

                timer.Stop();
                
                Console.WriteLine($"Published {MESSAGE_COUNT:N0} messages and handled confirm asynchronously {timer.ElapsedMilliseconds:N0} ms");

            }
        }

        private static bool WaitUntil(int numberOfSeconds, Func<bool> condition)
        {
            int waited = 0;
            while (!condition() && waited < numberOfSeconds * 1000)
            {
                Thread.Sleep(100);
                waited += 100;
            }

            return condition();
        }
    }
}
