using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Confluent.SchemaRegistry;
using KafkaPlayground.Beta.Messages;
using Newtonsoft.Json;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace KafkaPlayground.Beta.Subscriber
{
    class BetaSubscriber
    {
        private static readonly ILogger Log =
            new LoggerConfiguration()
                .MinimumLevel.ControlledBy(new LoggingLevelSwitch
                {
                    MinimumLevel = LogEventLevel.Verbose
                })
                .WriteTo.Console()
                .CreateLogger();

        private static readonly string[] Topics = { BetaTopics.Test };
        private static readonly TimeSpan SleepMilliseconds = TimeSpan.FromSeconds(10);
        private static readonly CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();

        static void Main(string[] args)
        {
            Console.Title = AppDomain.CurrentDomain.FriendlyName;

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                CancellationTokenSource.Cancel();
            };

            while (!CancellationTokenSource.Token.IsCancellationRequested)
            {
                //ConsumeAutoCommit();
                //ConsumeCommitAll();
                ConsumeCommitLast();

                Thread.Sleep(SleepMilliseconds);
            }

            Console.ReadKey();
        }

        private static void ConsumeAutoCommit()
        {
            var start = DateTime.Now;

            var consumer = CreateConsumer<long, string>(autoCommit: true);

            var messages = new List<ConsumeResult<long, string>>();

            var message = consumer.Consume(TimeSpan.FromSeconds(10));

            while (message != null)
            {
                messages.Add(message);

                Print(message);

                consumer.Commit(message);

                message = consumer.Consume(TimeSpan.FromSeconds(10));
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeAutoCommit), messages.Count, DateTime.Now.Subtract(start));
        }

        private static void ConsumeCommitAll()
        {
            var start = DateTime.Now;

            var consumer = CreateConsumer<long, string>(autoCommit: false);

            var messages = new List<ConsumeResult<long, string>>();

            var message = consumer.Consume(TimeSpan.FromSeconds(10));

            while (message != null)
            {
                messages.Add(message);

                Print(message);

                consumer.Commit(message);

                message = consumer.Consume(TimeSpan.FromSeconds(10));
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeCommitAll), messages.Count, DateTime.Now.Subtract(start));
        }

        private static void ConsumeCommitLast()
        {
            var start = DateTime.Now;

            var consumer = CreateConsumer<long, string>(autoCommit: false);

            var messages = new List<ConsumeResult<long, string>>();

            var message = consumer.Consume(TimeSpan.FromSeconds(10));

            while (message != null)
            {
                messages.Add(message);

                Print(message);

                consumer.Commit(message);

                message = consumer.Consume(TimeSpan.FromSeconds(10));
            }

            Log.Information("{0}: {1} messages consumed, elapsed {2}", nameof(ConsumeCommitLast), messages.Count, DateTime.Now.Subtract(start));

            start = DateTime.Now;

            if (messages.Any())
            {
                foreach (var messagesPerPartition in messages.GroupBy(m => m.Partition))
                {
                    var higherOffset = messagesPerPartition.OrderByDescending(m => m.Offset.Value).First();

                    consumer.Commit(higherOffset);
                }
            }

            Log.Information("{0}: Committing {1} messages, elapsed {2}", nameof(ConsumeCommitLast), messages.Count, DateTime.Now.Subtract(start));
        }

        private static Consumer<TKey, TValue> CreateConsumer<TKey, TValue>(bool autoCommit)
        {
            Log.Information("Creating consumer");

            var avroSerdeProvider = new AvroSerdeProvider(CreateAvroSerdeProviderConfig());

            var config = CreateConsumerConfig(autoCommit);

            var keyDeserializer = avroSerdeProvider.GetDeserializerGenerator<TKey>();
            var valueDeserializer = avroSerdeProvider.GetDeserializerGenerator<TValue>();

            var consumer = new Consumer<TKey, TValue>(config, keyDeserializer, valueDeserializer);

            consumer.OnError += (_, error) => Console.WriteLine($"[ERROR] Code: {error.Code}, Reason: {error.Reason}");
            consumer.OnLog += (_, log) => Console.WriteLine($"[LOG] {log.Message}");
            consumer.OnStatistics += (_, statistic) => Console.WriteLine($"[STATISTIC] {statistic}");

            Log.Information("Consumer created");

            consumer.Subscribe(Topics);

            Log.Information("Subscribed to: {0}", string.Join(',', Topics));

            return consumer;
        }

        private static AvroSerdeProviderConfig CreateAvroSerdeProviderConfig()
        {
            var config = new AvroSerdeProviderConfig();

            if (!File.Exists("beta.consumer.schemaregistry.json"))
                return config;

            var content = File.ReadAllText("beta.consumer.schemaregistry.json");
            var json = JsonConvert.DeserializeObject<Dictionary<string, string>>(content);

            if (json.ContainsKey(AvroSerdeProviderConfig.PropertyNames.AvroSerializerAutoRegisterSchemas))
                if (bool.TryParse(json[AvroSerdeProviderConfig.PropertyNames.AvroSerializerAutoRegisterSchemas], out var value))
                    config.AvroSerializerAutoRegisterSchemas = value;

            if (json.ContainsKey(AvroSerdeProviderConfig.PropertyNames.AvroSerializerBufferBytes))
                if (int.TryParse(json[AvroSerdeProviderConfig.PropertyNames.AvroSerializerBufferBytes], out var value))
                    config.AvroSerializerBufferBytes = value;

            if (json.ContainsKey(SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas))
                if (int.TryParse(json[SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas], out var value))
                    config.SchemaRegistryMaxCachedSchemas = value;

            if (json.ContainsKey(SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs))
                if (int.TryParse(json[SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs], out var value))
                    config.SchemaRegistryRequestTimeoutMs = value;

            if (json.ContainsKey(SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl))
                config.SchemaRegistryUrl = json[SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl];

            return config;
        }

        private static ConsumerConfig CreateConsumerConfig(bool enableAutoCommit)
        {
            var config = default(ConsumerConfig);

            if (File.Exists("beta.consumer.json"))
            {
                var content = File.ReadAllText("beta.consumer.json");
                var json = JsonConvert.DeserializeObject<Dictionary<string, string>>(content);

                config = new ConsumerConfig(json);
            }

            config = config ?? new ConsumerConfig();

            config.EnableAutoCommit = enableAutoCommit;
            config.GroupId = Guid.NewGuid().ToString();

            config.ToList().ForEach(i => Log.Information("{0}[{1}]: {2}", nameof(ConsumerConfig), i.Key, i.Value));

            return config;
        }

        private static void Print<TKey, TValue>(ConsumeResult<TKey, TValue> consumeResult) =>
            Log.Debug("Message received ({0}) - [{1}]: {2}", consumeResult.TopicPartitionOffset, consumeResult.Key, consumeResult.Value);
    }
}