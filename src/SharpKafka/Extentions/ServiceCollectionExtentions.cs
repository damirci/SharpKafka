using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpKafka.Consumer;
using SharpKafka.Producer;
using SharpKafka.Workers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace SharpKafka.Extentions
{
    public static class ServiceCollectionExtentions
    {
        public static IServiceCollection AddSharpKafka(this IServiceCollection services, KafkaConfig kafkaConfig, params Type[] profileAssemblyMarkerTypes)
         => SharpKafkaClasses(services, kafkaConfig, profileAssemblyMarkerTypes.Select(t => t.GetTypeInfo().Assembly));

        public static IServiceCollection AddSharpKafka(this IServiceCollection services, KafkaConfig kafkaConfig, IEnumerable<Type> profileAssemblyMarkerTypes)
        => SharpKafkaClasses(services, kafkaConfig, profileAssemblyMarkerTypes.Select(t => t.GetTypeInfo().Assembly));

        private static IServiceCollection SharpKafkaClasses(IServiceCollection services, KafkaConfig config, IEnumerable<Assembly> assembliesToScan)
        {
            services.AddTransient((sp) => Deserializers.Null);
            services.AddTransient((sp) => Deserializers.ByteArray);
            services.AddTransient((sp) => Deserializers.Double);
            services.AddTransient((sp) => Deserializers.Ignore);
            services.AddTransient((sp) => Deserializers.Int32);
            services.AddTransient((sp) => Deserializers.Int64);
            services.AddTransient((sp) => Deserializers.Single);
            services.AddTransient((sp) => Deserializers.Utf8);

            services.AddTransient((sp) => Serializers.Null);
            services.AddTransient((sp) => Serializers.ByteArray);
            services.AddTransient((sp) => Serializers.Double);
            services.AddTransient((sp) => Serializers.Int32);
            services.AddTransient((sp) => Serializers.Int64);
            services.AddTransient((sp) => Serializers.Single);
            services.AddTransient((sp) => Serializers.Utf8);

            services.AddSingleton(config);
            services.AddSingleton<ProducerClientHandler>();
            services.AddSingleton(typeof(IKafkaDependentProducer<,>), typeof(KafkaDependentProducer<,>));
            services.AddTransient(typeof(IKafkaConsumer<,>), typeof(KafkaConsumer<,>));

            var assembliesToScanArray = assembliesToScan as Assembly[] ?? assembliesToScan?.ToArray();

            if (assembliesToScanArray != null && assembliesToScanArray.Length > 0)
            {
                var allTypes = assembliesToScanArray
                    .Where(a => !a.IsDynamic && a.GetName().Name != nameof(SharpKafka))
                    .Distinct()
                    .SelectMany(a => a.DefinedTypes)
                    .ToArray();

                var messageHandlerTypes = allTypes
                .Where(t => t.IsClass
                    && !t.IsInterface
                    && !t.IsAbstract
                    && t.ImplementedInterfaces.Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<,>)));


                foreach (var messageHandlerType in messageHandlerTypes)
                {
                    var iMessageHandlerType = messageHandlerType.ImplementedInterfaces.First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<,>));
                    services.AddTransient(iMessageHandlerType, messageHandlerType);

                    var consumerWorkerType = typeof(ConsumerWorker<,>).MakeGenericType(iMessageHandlerType.GetGenericArguments());

                    services.AddTransient(typeof(IHostedService),consumerWorkerType);
                }
            }

            return services;
        }
    }
}
