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
using System.Threading;

namespace KafkaPlayground.Legacy.Subscriber
{
    internal class LegacySubscriber
    {
        private static readonly ILogger Log =
            new LoggerConfiguration()
                .MinimumLevel.ControlledBy(new LoggingLevelSwitch
                {
                    MinimumLevel = LogEventLevel.Debug
                })
                .WriteTo.Console()
                .CreateLogger();

        private static readonly string[] Topics = { "kafka.playground.legacy.test" };
        private static readonly TimeSpan SleepMilliseconds = TimeSpan.FromSeconds(10);

        static void Main(string[] args)
        {
            Console.Title = AppDomain.CurrentDomain.FriendlyName;

            //ConsumeAutoCommit();
            //ConsumeCommitAll();
            ConsumeCommitLast();

            Console.ReadKey();
        }

        private static void ConsumeAutoCommit()
        {
            var start = DateTime.Now;

            var consumer = CreateConsumer<long, string>(autoCommit: true);

            Thread.Sleep(SleepMilliseconds);

            var messages = new List<Message<long, string>>();

            while (consumer.Consume(out var message, TimeSpan.FromSeconds(5)))
            {
                Log.Debug("Message received ({0})", message.TopicPartitionOffset);
                Log.Verbose("\tKey: {0}", message.Key);
                Log.Verbose("\tValue: {0}", message.Value);

                messages.Add(message);
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeAutoCommit), messages.Count, DateTime.Now.Subtract(start));
        }

        private static void ConsumeCommitAll()
        {
            var start = DateTime.Now;

            var consumer = CreateConsumer<long, string>(autoCommit: false);

            Thread.Sleep(SleepMilliseconds);

            var messages = new List<Message<long, string>>();

            while (consumer.Consume(out var message, TimeSpan.FromSeconds(10)))
            {
                Log.Debug("Message received ({0})", message.TopicPartitionOffset);
                Log.Verbose("\tKey: {0}", message.Key);
                Log.Verbose("\tValue: {0}", message.Value);

                consumer.CommitAsync(message).Wait();
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeCommitAll), messages.Count, DateTime.Now.Subtract(start));
        }

        private static void ConsumeCommitLast()
        {
            var start = DateTime.Now;

            var consumer = CreateConsumer<long, string>(autoCommit: false);

            Thread.Sleep(SleepMilliseconds);

            var messages = new List<Message<long, string>>();

            while (consumer.Consume(out var message, TimeSpan.FromSeconds(10)))
            {
                if (message.Error.HasError)
                {
                    Log.Debug("Error consuming ({0}). {1}", message.TopicPartitionOffset, message.Error);
                    continue;
                }

                Log.Debug("Message received ({0})", message.TopicPartitionOffset);
                Log.Verbose("\tKey: {0}", message.Key);
                Log.Verbose("\tValue: {0}", message.Value);

                messages.Add(message);
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeCommitLast), messages.Count, DateTime.Now.Subtract(start));

            start = DateTime.Now;

            if (messages.Any())
            {
                foreach (var messagesPerPartition in messages.GroupBy(m => m.Partition))
                {
                    var higherOffset = messagesPerPartition.OrderByDescending(m => m.Offset.Value).First();

                    consumer.CommitAsync(higherOffset);
                }
            }

            Log.Information("{0}: Committing {1} messages, elapsed {2}", nameof(ConsumeCommitLast), messages.Count, DateTime.Now.Subtract(start));
        }

        private static Consumer<TKey, TValue> CreateConsumer<TKey, TValue>(bool autoCommit)
        {
            Log.Information("Creating consumer");

            var consumerJson = File.ReadAllText("legacy.consumer.json");

            var config = JsonConvert.DeserializeObject<Dictionary<string, object>>(consumerJson);

            config["enable.auto.commit"] = autoCommit;

            var consumer = new Consumer<TKey, TValue>(config, new AvroDeserializer<TKey>(), new AvroDeserializer<TValue>());

            consumer.OnConsumeError += (_, error) => Console.WriteLine($"[CONSUMER ERROR] Code: {error.Error.Code}, Reason: {error.Error.Reason}");
            consumer.OnError += (_, error) => Console.WriteLine($"[ERROR] Code: {error.Code}, Reason: {error.Reason}");
            consumer.OnLog += (_, log) => Console.WriteLine($"[LOG] {log.Message}");
            consumer.OnStatistics += (_, statistic) => Console.WriteLine($"[STATISTIC] {statistic}");

            Log.Information("Consumer created");

            consumer.Subscribe(Topics);

            Log.Information("Subscribed to: {0}", string.Join(',', Topics));

            return consumer;
        }
    }
}