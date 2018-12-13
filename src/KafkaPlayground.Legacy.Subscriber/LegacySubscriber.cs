using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using KafkaPlayground.Common;
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
                    MinimumLevel = LogEventLevel.Information
                })
                .WriteTo.Console()
                .CreateLogger();

        private static readonly CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();
        private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(100);
        private static readonly TimeSpan Delay = TimeSpan.FromSeconds(1);
        private static readonly string Topic = Topics.DefaultTopic;

        private static void Main()
        {
            Console.Title = AppDomain.CurrentDomain.FriendlyName;

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                CancellationTokenSource.Cancel();
            };

            //var autoCommitConsumer = CreateConsumer<long, string>(enableAutoCommit: true);
            var manualCommitConsumer = CreateConsumer<long, string>(enableAutoCommit: false);

            Thread.Sleep(Delay);

            while (!CancellationTokenSource.Token.IsCancellationRequested)
            {
                //ConsumeAutoCommit(autoCommitConsumer);
                //ConsumeCommitAll(manualCommitConsumer);
                ConsumeCommitLast(manualCommitConsumer);

                Thread.Sleep(Delay);
            }

            Console.ReadKey();
        }

        private static void ConsumeAutoCommit(Consumer<long, string> consumer)
        {
            var start = DateTime.Now;

            var messages = new List<Message<long, string>>();

            while (consumer.Consume(out var message, ConsumeTimeout))
            {
                Print(message);

                messages.Add(message);
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeAutoCommit), messages.Count, DateTime.Now.Subtract(start));
        }

        private static void ConsumeCommitAll(Consumer<long, string> consumer)
        {
            var start = DateTime.Now;

            var messages = new List<Message<long, string>>();

            while (consumer.Consume(out var message, ConsumeTimeout))
            {
                Print(message);

                consumer.CommitAsync(message).Wait();
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeCommitAll), messages.Count, DateTime.Now.Subtract(start));
        }

        private static void ConsumeCommitLast(Consumer<long, string> consumer)
        {
            var start = DateTime.Now;

            var messages = new List<Message<long, string>>();

            while (consumer.Consume(out var message, ConsumeTimeout))
            {
                if (message.Error.HasError)
                {
                    Log.Debug("Error consuming ({0}). {1}", message.TopicPartitionOffset, message.Error);
                    continue;
                }

                Print(message);

                messages.Add(message);
            }

            Log.Debug("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeCommitLast), messages.Count, DateTime.Now.Subtract(start));

            if (messages.Count == 0)
                return;

            start = DateTime.Now;

            if (messages.Any())
            {
                foreach (var messagesPerPartition in messages.GroupBy(m => m.Partition))
                {
                    var higherOffset = messagesPerPartition.OrderByDescending(m => m.Offset.Value).First();

                    consumer.CommitAsync(higherOffset).Wait();
                }
            }

            Log.Information("{0}: Committing {1} messages, elapsed {2}", nameof(ConsumeCommitLast), messages.Count, DateTime.Now.Subtract(start));
        }

        private static Consumer<TKey, TValue> CreateConsumer<TKey, TValue>(bool enableAutoCommit)
        {
            Log.Information("Creating consumer");

            var consumer = new Consumer<TKey, TValue>(CreateConsumerConfig(enableAutoCommit), new AvroDeserializer<TKey>(), new AvroDeserializer<TValue>());

            consumer.OnConsumeError += (_, error) => Console.WriteLine($"[CONSUMER ERROR] Code: {error.Error.Code}, Reason: {error.Error.Reason}");
            consumer.OnError += (_, error) => Console.WriteLine($"[ERROR] Code: {error.Code}, Reason: {error.Reason}");
            consumer.OnLog += (_, log) => Console.WriteLine($"[LOG] {log.Message}");
            consumer.OnStatistics += (_, statistic) => Console.WriteLine($"[STATISTIC] {statistic}");

            Log.Information("Consumer created");

            consumer.Subscribe(Topic);

            Log.Information("Subscribed to: {0}", Topic);

            return consumer;
        }

        private static IDictionary<string, object> CreateConsumerConfig(bool enableAutoCommit)
        {
            var consumerJson = File.ReadAllText("legacy.consumer.json");

            var config = JsonConvert.DeserializeObject<Dictionary<string, object>>(consumerJson);

            config["enable.auto.commit"] = enableAutoCommit;
            config["group.id"] = nameof(LegacySubscriber);

            config.ToList().ForEach(i => Log.Information("{0}[{1}]: {2}", "Config", i.Key, i.Value));

            return config;
        }

        private static void Print<TKey, TValue>(Message<TKey, TValue> consumeResult) =>
            Log.Debug("Message received ({0}) - [{1}]: {2}", consumeResult.TopicPartitionOffset, consumeResult.Key, consumeResult.Value);
    }
}