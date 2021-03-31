using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using SharpKafka.Message;
using SharpKafka.Producer;
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
            services.Configure<KafkaConfig>(opt => opt = config);
            services.AddSingleton<ProducerClientHandler>();

            var assembliesToScanArray = assembliesToScan as Assembly[] ?? assembliesToScan?.ToArray();

            if (assembliesToScanArray != null && assembliesToScanArray.Length > 0)
            {
                var allTypes = assembliesToScanArray
                    .Where(a => !a.IsDynamic && a.GetName().Name != nameof(SharpKafka))
                    .Distinct()
                    .SelectMany(a => a.DefinedTypes)
                    .ToArray();

                var messageTypes = allTypes
                    .Where(t => t.IsClass
                        && !t.IsAbstract
                        && t.AsType().IsAssignableFrom(typeof(IMessage)));

                var producerInterfaceType = typeof(IKafkaDependentProducer<,>);
                var producerClassType = typeof(KafkaDependentProducer<,>);

                foreach (var item in messageTypes)
                {
                    var topic = item.GetCustomAttribute<TopicAttribute>();
                    if (topic == null)
                    {
                        continue;
                    }
                    var keyType = topic.KeyType ?? typeof(Null);

                    var producerInterface= Type.MakeGenericSignatureType(producerInterfaceType, keyType, item);
                    var producerClass = Type.MakeGenericSignatureType(producerClassType, keyType, item);

                    services.AddSingleton(producerInterface, producerClass);
                }


                var messageHandlerTypes = allTypes
                .Where(t => t.IsClass
                    && !t.IsAbstract
                    && t.AsType().IsAssignableFrom(typeof(IMessage)));

                var consumerInterfaceType = typeof(IKafkaDependentProducer<,>);
                var consumerClassType = typeof(KafkaDependentProducer<,>);

                foreach (var item in messageHandlerTypes)
                {
                    services.AddTransient(item);
                    var topic = item.GetCustomAttribute<TopicAttribute>();
                    if (topic == null)
                    {
                        continue;
                    }
                    var keyType = topic.KeyType ?? typeof(Null);
                    var messageType = item.GetInterfaceMap(typeof(IMessageHandler<,>)).InterfaceType.GetGenericArguments()[1];
                    var consumerInterface = Type.MakeGenericSignatureType(consumerInterfaceType, keyType, messageType);
                    var consumerClass = Type.MakeGenericSignatureType(consumerClassType, keyType, item);

                    services.AddSingleton(consumerInterfaceType, consumerClassType);
                }
            }

            return services;
        }
    }
}
