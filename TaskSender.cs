using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace TaskSender
{
    class TaskSender
    {
        //initialize connection factory instance
        ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
        public static void Main()
        {
            int counter = 0;

            var startTimeSpan = TimeSpan.Zero;

            //How often messages will be sent by the sender
            var periodTimeSpan = TimeSpan.FromSeconds(0.25);

            var timer = new System.Threading.Timer((e) =>
            {
                SendTask("Message number " + counter++);
            }, null, startTimeSpan, periodTimeSpan);

            Console.ReadLine();
        }

        public static void SendTask(string input)
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

                var message = input;
                var body = Encoding.UTF8.GetBytes(message);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                channel.BasicPublish(exchange: "",
                                     routingKey: "queue2",
                                     basicProperties: properties,
                                     body: body);
                Console.WriteLine(DateTime.Now + " - Sent: {0}", message);
            }

            Console.ReadLine();
        }
    }
}
