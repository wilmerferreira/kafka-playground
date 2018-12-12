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
using System.Threading.Tasks;

namespace KafkaPlayground.Beta.Publisher
{
    class BetaPublisher
    {
        private static readonly ILogger Log = new LoggerConfiguration()
            .MinimumLevel.ControlledBy(new LoggingLevelSwitch
            {
                MinimumLevel = LogEventLevel.Verbose
            })
            .WriteTo.Console()
            .CreateLogger();

        static void Main(string[] args)
        {
            Console.Title = AppDomain.CurrentDomain.FriendlyName;

            //SendOne();
            //SendMany(1000);
            BulkSend(1000);

            Console.ReadKey();
        }

        private static void SendOne()
        {
            var start = DateTime.Now;

            var producer = CreateProducer<long, string>();

            var message = new Message<long, string>
            {
                Key = DateTime.Now.Ticks,
                Value = new Random().Next().ToString()
            };

            var task = producer
                .ProduceAsync(BetaTopics.Test, message)
                .ContinueWith(LogDeliveryReport);

            task.Wait();

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
                var message = new Message<long, string>
                {
                    Key = DateTime.Now.Ticks,
                    Value = new Random().Next().ToString()
                };

                var task = producer
                    .ProduceAsync(BetaTopics.Test, message)
                    .ContinueWith(LogDeliveryReport);

                task.Wait();
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
                {
                    var message = new Message<long, string>
                    {
                        Key = DateTime.Now.Ticks,
                        Value = new Random().Next().ToString()
                    };

                    return producer
                        .ProduceAsync(BetaTopics.Test, message)
                        .ContinueWith(LogDeliveryReport);
                })
                .ToArray();

            Task.WaitAll(tasks);

            Log.Information("{0}: {1} message(s) sent, elapsed time {2}", nameof(BulkSend), quantity, DateTime.Now.Subtract(start));
        }

        private static Producer<TKey, TValue> CreateProducer<TKey, TValue>()
        {
            Log.Information("Creating producer");

            var avroSerdeProvider = new AvroSerdeProvider(CreateAvroSerdeProviderConfig());

            var producer = new Producer<TKey, TValue>(CreateProducerConfig(), avroSerdeProvider.GetSerializerGenerator<TKey>(), avroSerdeProvider.GetSerializerGenerator<TValue>());

            producer.OnError += (_, error) => Log.Error("{0}: {1}", error.Code, error.Reason);

            producer.OnLog += (_, log) => Log.Information("Log: {0}", log.Message);

            producer.OnStatistics += (_, statistic) => Log.Information("Statistics: {0}", statistic);

            Log.Information("Producer created");

            return producer;
        }

        private static AvroSerdeProviderConfig CreateAvroSerdeProviderConfig()
        {
            var config = new AvroSerdeProviderConfig();

            if (!File.Exists("beta.producer.schemaregistry.json"))
                return config;

            var content = File.ReadAllText("beta.producer.schemaregistry.json");
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

            config.ToList().ForEach(i => Log.Information("{0}[{1}]: {2}", nameof(AvroSerdeProviderConfig), i.Key, i.Value));

            return config;
        }

        private static ProducerConfig CreateProducerConfig()
        {
            var config = default(ProducerConfig);

            if (File.Exists("beta.producer.json"))
            {
                var content = File.ReadAllText("beta.producer.json");
                var json = JsonConvert.DeserializeObject<Dictionary<string, string>>(content);

                config = new ProducerConfig(json);
            }

            config = config ?? new ProducerConfig();

            config.ToList().ForEach(i => Log.Information("{0}[{1}]: {2}", nameof(ProducerConfig), i.Key, i.Value));

            return config;
        }

        private static void LogDeliveryReport(Task<DeliveryReport<long, string>> deliveryTask)
        {
            if (deliveryTask.IsFaulted)
                Log.Error("Error sending. {0}", deliveryTask.Exception?.Message);
            else
                Log.Verbose("Message sent ({0}) - [{1}]: {2}", deliveryTask.Result.TopicPartitionOffset, deliveryTask.Result.Key, deliveryTask.Result.Value);
        }
    }
}