using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using SharpKafka.Consumer;
using SharpKafka.Message;
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
            services.AddSingleton(config);
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
                        && t.AsType().IsAssignableTo(typeof(IMessage)));

                var producerInterfaceType = typeof(IKafkaDependentProducer<,>);
                var producerClassType = typeof(KafkaDependentProducer<,>);

                //foreach (var item in messageTypes)
                //{
                //    var topic = item.GetCustomAttribute<TopicAttribute>();
                //    if (topic == null)
                //    {
                //        continue;
                //    }
                //    var keyType = topic.KeyType ?? typeof(Null);

                //    var producerInterface = Type.MakeGenericSignatureType(producerInterfaceType, keyType, item);
                //    var producerClass = Type.MakeGenericSignatureType(producerClassType, keyType, item);

                //    services.AddSingleton(producerInterface, producerClass);
                //}

                services.AddScoped(typeof(IKafkaDependentProducer<,>), typeof(KafkaDependentProducer<,>));


                //var messageHandlerTypes = allTypes
                //.Where(t => t.IsClass
                //    && !t.IsAbstract
                //    && t.AsType().IsAssignableTo(typeof(IMessageHandler)));

                //var consumerInterfaceType = typeof(IKafkaConsumer<,>);
                //var consumerClassType = typeof(KafkaConsumer<,>);
                //var consumerWorkerType = typeof(ConsumerWorker<,>);

                //foreach (var item in messageHandlerTypes)
                //{
                //    services.AddTransient(item);
                //    var topic = item.GetCustomAttribute<TopicAttribute>();
                //    if (topic == null)
                //    {
                //        continue;
                //    }
                //    var keyType = topic.KeyType ?? typeof(Null);
                //    var messageType = item.ImplementedInterfaces.First(i => i.IsGenericType && i.IsAssignableTo(typeof(IMessageHandler))).GetGenericArguments()[1];
                //    var consumerInterface = Type.MakeGenericSignatureType(consumerInterfaceType, keyType, messageType);
                //    var consumerClass = Type.MakeGenericSignatureType(consumerClassType, keyType, item);

                //    var consumerWorker = Type.MakeGenericSignatureType(consumerWorkerType, keyType, item);

                //    services.AddTransient(consumerInterfaceType, consumerClassType);
                //    services.AddSingleton(consumerWorker);
                //}


            }

            return services;
        }
    }
}
