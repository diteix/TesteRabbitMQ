using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Producer
{
    class Program
    {
        private static string ExchangeName = "e_nums";

        static void Main(string[] args)
        {
            WriteLogo();

            Console.WriteLine(" Selecionar opção: ");
            Console.WriteLine(" - 1 Direct ");
            Console.WriteLine(" - 2 Topic ");
            Console.WriteLine(" - 3 Header ");
            Console.WriteLine(" - 4 Fanout ");

            string input = Console.ReadLine();
            string cont = "s";

            while (!string.IsNullOrEmpty(input) && cont == "s")
            {
                Console.WriteLine(" Primeiro número: ");
                var first = Console.ReadLine();

                Console.WriteLine(" Segundo número: ");
                var second = Console.ReadLine();

                Action<string, string> action;

                switch (input)
                {
                    case "1":
                        action = Direct;
                        break;
                    case "2":
                        action = Topic;
                        break;
                    case "3":
                        action = Header;
                        break;
                    case "4":
                        action = Fanout;
                        break;
                    default:
                        action = (f, s) =>
                        {
                            Log(" Ação {0} não encontrada ", input);
                        };
                        break;
                }

                action(first, second);

                Console.WriteLine(" Continuar [s] ou parar [n]? ");

                cont = Console.ReadLine();
            }
        }

        private static void Direct(string first, string second)
        {
            Log("Utilizando Exchange por DIRECT para envio de mensagens");

            string routingKeyOdd = "odd", routingKeyEven = "even";

            int from, to;

            from = int.Parse(first);
            to = int.Parse(second);

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_direct";

                    Log("Criando/Conectando ao Exchange '{0}'", exchangeName);

                    channel.ExchangeDeclare(exchangeName, ExchangeType.Direct, durable: true);

                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;

                    byte[] body;

                    foreach (var num in Enumerable.Range(from, to - from + 1))
                    {
                        body = Encoding.UTF8.GetBytes(num.ToString());

                        var routingKey = num % 2 != 0 ? routingKeyOdd : routingKeyEven;

                        Log("Enviando número {0}:{1}", num, routingKey);

                        channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: properties,
                            mandatory: true, body: body);

                        Thread.Sleep(2000);
                    }
                }
            }
        }

        private static void Topic(string first, string second)
        {
            Log("Utilizando Exchange por TOPIC para envio de mensagens");

            string routingKeyOdd = "odd", routingKeyEven = "even";

            int from, to;

            from = int.Parse(first);
            to = int.Parse(second);

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_topic";

                    Log("Criando/Conectando ao Exchange '{0}'", exchangeName);

                    channel.ExchangeDeclare(exchangeName, ExchangeType.Topic, durable: true);

                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;

                    byte[] body;

                    foreach (var num in Enumerable.Range(from, to - from + 1))
                    {
                        body = Encoding.UTF8.GetBytes(num.ToString());

                        var routingKey = num % 2 != 0 ? routingKeyOdd : routingKeyEven;

                        Log("Enviando número {0}:{1}", num, routingKey);

                        channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: properties,
                            mandatory: true, body: body);

                        Thread.Sleep(2000);
                    }
                }
            }
        }

        private static void Header(string first, string second)
        {
            Log("Utilizando Exchange por HEADER para envio de mensagens");

            string headerOdd = "odd", headerEven = "even";
            int from, to;

            from = int.Parse(first);
            to = int.Parse(second);

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_header";

                    IDictionary<string, object> headers = new Dictionary<string, object>();
                    headers.Add("x-match", "all");

                    Log("Criando/Conectando ao Exchange '{0}'", exchangeName);

                    channel.ExchangeDeclare(exchangeName, ExchangeType.Headers, durable: true);

                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.Headers = new Dictionary<string, object>();
                    properties.Headers.Add("type", "");

                    byte[] body;

                    foreach (var num in Enumerable.Range(from, to - from + 1))
                    {
                        body = Encoding.UTF8.GetBytes(num.ToString());

                        properties.Headers["type"] = num % 2 != 0 ? headerOdd : headerEven;

                        Log("Enviando número {0}:{1}", num, properties.Headers["type"]);

                        channel.BasicPublish(exchange: exchangeName, routingKey: "", basicProperties: properties,
                            mandatory: true, body: body);

                        Thread.Sleep(2000);
                    }
                }
            }
        }

        private static void Fanout(string first, string second)
        {
            Log("Utilizando Exchange por FANOUT para envio de mensagens");

            int from, to;

            from = int.Parse(first);
            to = int.Parse(second);

            var factory = new ConnectionFactory() { HostName = "localhost" };

            factory.AutomaticRecoveryEnabled = true;

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var exchangeName = ExchangeName + "_fanout";

                    Log("Criando/Conectando ao Exchange '{0}'", exchangeName);

                    channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout, durable: true);

                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;

                    byte[] body;

                    foreach (var num in Enumerable.Range(from, to - from + 1))
                    {
                        body = Encoding.UTF8.GetBytes(num.ToString());

                        Log("Enviando número {0}", num);

                        channel.BasicPublish(exchange: exchangeName, routingKey: "", basicProperties: properties,
                            mandatory: true, body: body);

                        Thread.Sleep(2000);
                    }
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

            Console.WriteLine(@"  ___            _                 ");
            Console.WriteLine(@" | _ \_ _ ___ __| |_  _ __ ___ _ _ ");
            Console.WriteLine(@" |  _| '_/ _ / _` | || / _/ -_| '_|");
            Console.WriteLine(@" |_| |_| \___\__,_|\_,_\__\___|_|  ");

            Console.WriteLine();
        }
    }
}
