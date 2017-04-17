using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace MemberConsumer
{
    public class RabbitMQConsumer
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;

        private QueueingBasicConsumer _consumer;
        private string response;

        public void CreateConnection()
        {
            _factory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            _connection = _factory.CreateConnection();

            _model = _connection.CreateModel();

            _model.QueueDeclare("MemberRPC_Queue", false, false, false, null);
            _model.BasicQos(0, 1, false);


            _consumer = new QueueingBasicConsumer(_model);
            _model.BasicConsume("MemberRPC_Queue", false, _consumer);

        }


        public void ProcessMessages()
        {
            while (true)
            {
                //get an item of the Queue
                var item = _consumer.Queue.Dequeue();
                // look at it's basic properties 
                var props = item.BasicProperties;

                //Create Basic Properties 
                var replyProps = _model.CreateBasicProperties();
                replyProps = _model.CreateBasicProperties();
                replyProps.CorrelationId = props.CorrelationId;

                Console.WriteLine("-----------------------------------------------------------------------------");

                try
                {
                    response = SignUpMember(item);
                    Console.WriteLine("Signed up Member with Correlation ID = {0}", props.CorrelationId);
                }
                finally
                {
                    if (response != null)
                    {
                        var responseBytes = Encoding.UTF8.GetBytes(response);
                        // Publish to the Queue that the requesting item is waiting on 
                        // reply properties contain the correlationid so it knows which message to process 
                        _model.BasicPublish("", props.ReplyTo, replyProps, responseBytes);
                    }
                    //take the message off the queue
                    _model.BasicAck(item.DeliveryTag, false);
                }
            }
        }
        public void Close()
        {
            _connection.Close();
        }

        private string SignUpMember(BasicDeliverEventArgs ea)
        {
            // this is totally a cool function that does nothing
            // look that way ----> 
            Random _rnd = new Random();
            System.Threading.Thread.Sleep(5000);
            return _rnd.Next(1000, 100000000).ToString();
            
        }
    }
}