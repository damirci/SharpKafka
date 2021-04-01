using Microsoft.Extensions.Hosting;
using System;

namespace SharpKafka.Workers
{
    public interface IConsumerWorker<TKey, TValue> : IHostedService, IDisposable
    {
    }
}