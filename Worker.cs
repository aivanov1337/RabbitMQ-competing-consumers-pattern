using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading;

namespace Worker
{
    public class Worker
    {
        public static void Main()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "queue2",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine("Waiting for tasks.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, ea) =>
                {
                    var body = ea.Body.Span;
                    var message = Encoding.UTF8.GetString(body);
                    
                    Console.WriteLine(DateTime.Now + " - Received: {0}", message);
                    Console.WriteLine("Working");                   
                    Thread.Sleep(1500); //simulates doing actual work
                    Console.WriteLine("Done");

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false); //acknowledges that the message was delivered
                };
                //consumes the message
                channel.BasicConsume(queue: "queue2", 
                                     autoAck: false,
                                     consumer: consumer);
                Console.ReadLine();
            }
        }
    }
}
