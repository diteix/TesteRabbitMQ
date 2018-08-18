using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace Consumer
{
    class Program
    {
        private static string ExchangeName = "e_nums";
        private static string QueueName = "q_nums";

        static void Main(string[] args)
        {
            WriteLogo();

            Console.WriteLine(" Selecionar opção: ");
            Console.WriteLine(" - 1 Direct ");
            Console.WriteLine(" - 2 Topic ");
            Console.WriteLine(" - 3 Header ");
            Console.WriteLine(" - 4 Fanout ");

            var input = Console.ReadLine();

            switch (input)
            {
                case "1":
                    Console.WriteLine(" Selecionar DIRECT para se inscrever [odd] [even]: ");
                    var direct = Console.ReadLine();

                    if (direct != "odd" && direct != "even")
                    {
                        Log(" TOPIC {0} não encontrado ", direct);
                        break;
                    }

                    Direct(direct);
                    break;
                case "2":
                    Console.WriteLine(" Selecionar TOPIC para se inscrever [odd.#] [#.odd] [even.#] [#.even] [#.#]: ");
                    var topic = Console.ReadLine();

                    if (topic != "odd.#" && topic != "#.odd" && topic != "even.#" && topic != "#.even" && topic != "#.#")
                    {
                        Log(" TOPIC {0} não encontrado ", topic);
                        break;
                    }

                    Topic(topic);
                    break;
                case "3":
                    Console.WriteLine(" Selecionar HEADER para se inscrever [odd] [even]: ");
                    var header = Console.ReadLine();

                    if (header != "odd" && header != "even")
                    {
                        Log(" HEADER {0} não encontrado ", header);
                        break;
                    }

                    Header(header);
                    break;
                case "4":
                    Console.WriteLine(" Selecionar FANOUT para se inscrever [f1] [f2]: ");
                    var fan = Console.ReadLine();

                    if (fan != "f1" && fan != "f2")
                    {
                        Log(" FANOUT {0} não encontrado ", fan);
                        break;
                    }

                    Fanout(fan);
                    break;
                default:
                    Log(" Ação {0} não encontrada ", input);
                    break;
            }
        }

        private static void Direct(string direct)
        {
            Log("Utilizando Exchange por DIRECT para resposta de mensagens");

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_direct";

                    var queueName = string.Format("{0}_{1}", QueueName, direct);

                    Log("Criando/Conectando a Fila '{0}'", queueName);

                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: direct);

                    Log("Configurando BasicQoS");

                    channel.BasicQos(0, 1, false);

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Log("Número recebido: {0}", message);

                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                        Log("ACK {0} enviado para número {1}", ea.DeliveryTag, message);
                    };

                    channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                    Log("Aguardando números para receber...");

                    Console.ReadLine();
                }
            }
        }

        private static void Topic(string topic)
        {
            Log("Utilizando Exchange por TOPIC para resposta de mensagens");

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_topic";

                    var queueName = string.Format("{0}_{1}", QueueName, topic);

                    Log("Criando/Conectando a Fila '{0}'", queueName);

                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: topic);

                    Log("Configurando BasicQoS");

                    channel.BasicQos(0, 1, false);

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);

                        Log("Número recebido: {0}:{1}", message, ea.RoutingKey);

                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                        Log("ACK {0} enviado para número {1}", ea.DeliveryTag, message);
                    };

                    channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                    Log("Aguardando números para receber...");

                    Console.ReadLine();
                }
            }
        }

        private static void Header(string header)
        {
            Log("Utilizando Exchange por HEADER para resposta de mensagens");

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_header";
                    var queueName = string.Format("{0}_h_{1}", QueueName, header);

                    IDictionary<string, object> headers = new Dictionary<string, object>();
                    headers.Add("x-match", "all");
                    headers.Add("type", header);

                    Log("Criando/Conectando a Fila '{0}'", queueName);

                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: "", arguments: headers);

                    Log("Configurando BasicQoS");

                    channel.BasicQos(0, 1, false);

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Log("Número recebido: {0}", message);

                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                        Log("ACK {0} enviado para número {1}", ea.DeliveryTag, message);
                    };

                    channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                    Log("Aguardando números para receber...");

                    Console.ReadLine();
                }
            }
        }

        private static void Fanout(string fan)
        {
            Log("Utilizando Exchange por FANOUT para resposta de mensagens");

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_fanout";

                    var queueName = string.Format("{0}_{1}", QueueName, fan);

                    Log("Criando/Conectando a Fila '{0}'", queueName);

                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: fan);

                    Log("Configurando BasicQoS");

                    channel.BasicQos(0, 1, false);

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Log("Número recebido: {0}", message);

                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                        Log("ACK {0} enviado para número {1}", ea.DeliveryTag, message);
                    };

                    channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                    Log("Aguardando números para receber...");

                    Console.ReadLine();
                }
            }
        }

        private static void Log(string message)
        {
            Console.WriteLine(" [{0}] - {1} ", DateTime.Now.ToString("dd/MM/yy HH:mm:ss"), message);
        }

        private static void Log(string message, params object[] args)
        {
            Log(string.Format(message, args));
        }

        private static void WriteLogo()
        {
            Console.WriteLine(@" _____        _          ______      _     _     _ _  ___  ________ ");
            Console.WriteLine(@"|_   _|      | |         | ___ \    | |   | |   (_| | |  \/  |  _  |");
            Console.WriteLine(@"  | | ___ ___| |_ ___    | |_/ /__ _| |__ | |__  _| |_| .  . | | | |");
            Console.WriteLine(@"  | |/ _ / __| __/ _ \   |    // _` | '_ \| '_ \| | __| |\/| | | | |");
            Console.WriteLine(@"  | |  __\__ | ||  __/   | |\ | (_| | |_) | |_) | | |_| |  | \ \/' /");
            Console.WriteLine(@"  \_/\___|___/\__\___|   \_| \_\__,_|_.__/|_.__/|_|\__\_|  |_/\_/\_\");

            Console.WriteLine();

            Console.WriteLine(@"   ___                                ");
            Console.WriteLine(@"  / __|___ _ _  ____  _ _ __  ___ _ _ ");
            Console.WriteLine(@" | (__/ _ | ' \(_-| || | '  \/ -_| '_|");
            Console.WriteLine(@"  \___\___|_||_/__/\_,_|_|_|_\___|_|  ");

            Console.WriteLine();
        }
    }
}
