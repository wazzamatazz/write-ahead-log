using System.Collections.Concurrent;

using Jaahas.WriteAheadLog.DependencyInjection.Internal;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.DependencyInjection;

/// <summary>
/// <see cref="WriteAheadLogFactory"/> manages the creation and lifecycle of registered
/// <see cref="IWriteAheadLog"/> instances.
/// </summary>
public sealed partial class WriteAheadLogFactory : IAsyncDisposable {

    private bool _disposed;
    
    private readonly ILogger<WriteAheadLogFactory> _logger;
    
    private readonly IServiceScope _serviceScope;
    
    private readonly ConcurrentDictionary<string, WriteAheadLogRegistrationExtended> _registrations = new ConcurrentDictionary<string, WriteAheadLogRegistrationExtended>(StringComparer.Ordinal);
    

    /// <summary>
    /// Creates a new <see cref="WriteAheadLogFactory"/> instance.
    /// </summary>
    /// <param name="serviceProvider">
    ///   The <see cref="IServiceProvider"/> to use when instantiating <see cref="IWriteAheadLog"/> instances.
    /// </param>
    /// <param name="logger">
    ///   The logger for the factory.
    /// </param>
    public WriteAheadLogFactory(IServiceProvider serviceProvider, ILogger<WriteAheadLogFactory>? logger = null) {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        _serviceScope = serviceProvider.CreateScope();
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WriteAheadLogFactory>.Instance;
        
        var registrations = serviceProvider.GetServices<WriteAheadLogRegistration>();
        
        foreach (var registration in registrations) {
            _registrations[registration.Name] = new WriteAheadLogRegistrationExtended(
                registration.Name,
                registration.Factory);
        }
    }
    
    
    /// <summary>
    /// Gets the names of all registered write-ahead logs.
    /// </summary>
    /// <returns>
    ///   The names of all registered write-ahead logs.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The factory has been disposed.
    /// </exception>
    public ICollection<WriteAheadLogMetadata> GetDescriptors() {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return _registrations.Values.Select(x => GetOrCreateWriteAheadLog(x)!.Metadata).ToList();
    }


    /// <summary>
    /// Gets the <see cref="IWriteAheadLog"/> instance for the specified name.
    /// </summary>
    /// <param name="name">
    ///   The name of the write-ahead log to retrieve.
    /// </param>
    /// <returns>
    ///   The <see cref="IWriteAheadLog"/> instance if found, otherwise <see langword="null"/>.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The factory has been disposed.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="name"/> is <see langword="null"/>.
    /// </exception>
    public IWriteAheadLog? GetWriteAheadLog(string name) {
        ArgumentNullException.ThrowIfNull(name);
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        return GetOrCreateWriteAheadLog(_registrations.GetValueOrDefault(name));
    }


    private IWriteAheadLog? GetOrCreateWriteAheadLog(WriteAheadLogRegistrationExtended? registration) {
        if (registration is null) {
            return null;
        }
        
        if (registration.Instance is not null) {
            return registration.Instance;
        }

        try {
            LogInitialisingLog(registration.Name);
            registration.Instance = registration.Factory.Invoke(_serviceScope.ServiceProvider);
        }
        catch (Exception e) {
            LogInitialiseLogError(registration.Name, e);
            throw;
        }

        return registration.Instance;
    }


    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }
        
        var logs = _registrations.Values;
        _registrations.Clear();

        foreach (var item in logs) {
            if (item.Instance is IAsyncDisposable asyncDisposable) {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            } else if (item.Instance is IDisposable disposable) {
                disposable.Dispose();
            }
        }
        
        _serviceScope.Dispose();

        _disposed = true;
    }


    [LoggerMessage(1, LogLevel.Debug, "Initialising write-ahead log '{name}'.")]
    partial void LogInitialisingLog(string name);
    
    [LoggerMessage(2, LogLevel.Error, "Error initialising write-ahead log '{name}'.")]
    partial void LogInitialiseLogError(string name, Exception error);


    private record WriteAheadLogRegistrationExtended(string Name, Func<IServiceProvider, IWriteAheadLog> Factory) : WriteAheadLogRegistration(Name, Factory) {

        public IWriteAheadLog? Instance { get; set; }

    }

}
