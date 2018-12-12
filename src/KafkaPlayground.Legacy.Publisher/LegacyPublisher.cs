using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaPlayground.Legacy.Publisher
{
    internal class LegacyPublisher
    {
        private static readonly ILogger Log = new LoggerConfiguration()
            .MinimumLevel.ControlledBy(new LoggingLevelSwitch
            {
                MinimumLevel = LogEventLevel.Information
            })
            .WriteTo.Console()
            .CreateLogger();

        private const string Topic = "kafka.playground.legacy.test";

        private static void Main(string[] args)
        {
            Console.Title = AppDomain.CurrentDomain.FriendlyName;

            //SendOne();
            //SendMany(1000);
            BulkSend(10000);

            Console.ReadKey();
        }

        private static void SendOne()
        {
            var start = DateTime.Now;

            var producer = CreateProducer<long, string>();

            var key = DateTime.Now.Ticks;

            var task = producer.ProduceAsync(Topic, key, new Random().Next().ToString());

            task.Wait();

            LogProduceResult(task.Result);

            Log.Information("{0}: 1 message sent, elapsed time {1}", nameof(SendOne), DateTime.Now.Subtract(start));
        }

        private static void SendMany(int quantity)
        {
            var start = DateTime.Now;

            if (quantity < 1)
                return;

            var producer = CreateProducer<long, string>();

            foreach (var i in Enumerable.Range(1, quantity))
            {
                var key = DateTime.Now.Ticks;

                var task = producer.ProduceAsync(Topic, key, i.ToString());

                task.Wait();

                LogProduceResult(task.Result);
            }

            Log.Information("{0}: {1} message(s) sent, elapsed time {2}", nameof(SendMany), quantity, DateTime.Now.Subtract(start));
        }

        private static void BulkSend(int quantity)
        {
            var start = DateTime.Now;

            if (quantity < 1)
                return;

            var producer = CreateProducer<long, string>();

            var items = Enumerable.Range(1, quantity);

            var tasks = items
                .Select(i =>
                    producer
                        .ProduceAsync(Topic, DateTime.Now.Ticks, i.ToString())
                        .ContinueWith(t => LogProduceResult(t.Result))
                )
                .ToArray();

            Task.WaitAll(tasks);

            Log.Information("{0}: {1} message(s) sent, elapsed time {2}", nameof(BulkSend), quantity, DateTime.Now.Subtract(start));
        }

        private static Producer<TKey, TValue> CreateProducer<TKey, TValue>()
        {
            Log.Information("Creating producer");

            var producerJson = File.ReadAllText("legacy.producer.json");

            var config = JsonConvert.DeserializeObject<Dictionary<string, object>>(producerJson);

            var producer = new Producer<TKey, TValue>(config, new AvroSerializer<TKey>(), new AvroSerializer<TValue>());

            producer.OnError += (_, error) => Log.Error("{0}: {1}", error.Code, error.Reason);

            producer.OnLog += (_, log) => Log.Information("Log: {0}", log.Message);

            producer.OnStatistics += (_, statistic) => Log.Information("Statistics: {0}", statistic);

            Log.Information("Producer created");

            return producer;
        }

        private static void LogProduceResult(Message<long, string> message)
        {
            if (message.Error.HasError)
                Log.Error("Error sending ({0}). {1}",
                    message.TopicPartitionOffset,
                    message.Error);
            else
            {
                Log.Debug("Message sent ({0})", message.TopicPartitionOffset);
                Log.Verbose("\tKey: {0}", message.Key);
                Log.Verbose("\tValue: {0}", message.Value);
            }
        }
    }
}