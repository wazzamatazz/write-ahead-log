using Jaahas.WriteAheadLog.DependencyInjection.Internal;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Jaahas.WriteAheadLog.DependencyInjection;

/// <summary>
/// Extensions for registering Write-Ahead Log (WAL) services in an <see cref="IServiceCollection"/>.
/// </summary>
public static class ServiceCollectionExtensions {

    /// <summary>
    /// Adds a file-based <see cref="IWriteAheadLog"/> service to the <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">
    ///   The <see cref="IServiceCollection"/>.
    /// </param>
    /// <param name="configure">
    ///   An action to configure the <see cref="FileWriteAheadLogOptions"/> for the WAL service.
    /// </param>
    /// <returns>
    ///   The <see cref="IServiceCollection"/>.
    /// </returns>
    /// <remarks>
    ///   The <see cref="IWriteAheadLog"/> service is registered as a singleton.
    /// </remarks>
    public static IServiceCollection AddFileWriteAheadLog(this IServiceCollection services, Action<FileWriteAheadLogOptions> configure) {
        services.AddFileWriteAheadLog(string.Empty, configure);
        services.AddSingleton(provider => provider.GetRequiredKeyedService<IWriteAheadLog>(string.Empty));
        
        return services;
    }
    
    
    /// <summary>
    /// Adds a named, file-based <see cref="IWriteAheadLog"/> service to the <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">
    ///   The <see cref="IServiceCollection"/>.
    /// </param>
    /// <param name="name">
    ///   The name of the WAL service.
    /// </param>
    /// <param name="configure">
    ///   An action to configure the <see cref="FileWriteAheadLogOptions"/> for the WAL service.
    /// </param>
    /// <returns>
    ///   The <see cref="IServiceCollection"/>.
    /// </returns>
    /// <remarks>
    ///   The <see cref="IWriteAheadLog"/> service is registered as a singleton.
    /// </remarks>
    public static IServiceCollection AddFileWriteAheadLog(this IServiceCollection services, string name, Action<FileWriteAheadLogOptions> configure) {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);
        
        services.AddOptions<FileWriteAheadLogOptions>(name).Configure(configure);
        
        services.AddWriteAheadLog(name, (provider, key) => {
            var options = provider.GetRequiredService<IOptionsMonitor<FileWriteAheadLogOptions>>();
            var opts = options.Get(key);
            return ActivatorUtilities.CreateInstance<FileWriteAheadLog>(provider, opts);
        });
        
        return services;
    }
    
    
    /// <summary>
    /// Adds core services required for a named <see cref="IWriteAheadLog"/> instance.
    /// </summary>
    /// <param name="services">
    ///   The <see cref="IServiceCollection"/>.
    /// </param>
    /// <param name="name">
    ///   The name of the WAL service.
    /// </param>
    /// <param name="factory">
    ///   A factory function for creating the <typeparamref name="TImplementation"/> instance.
    /// </param>
    /// <typeparam name="TImplementation">
    ///   The <see cref="IWriteAheadLog"/> implementation type.
    /// </typeparam>
    /// <returns>
    ///   The <see cref="IServiceCollection"/>.
    /// </returns>
    /// <remarks>
    ///   The <see cref="IWriteAheadLog"/> service is registered as a singleton.
    /// </remarks>
    public static IServiceCollection AddWriteAheadLog<TImplementation>(this IServiceCollection services, string name, Func<IServiceProvider, string, TImplementation> factory) where TImplementation : class, IWriteAheadLog {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(factory);

        services.TryAddSingleton(TimeProvider.System);
        
        services.AddSingleton(new KeyedLogRegistration(name));

        // Will only be registered once, even if multiple logs are registered.
        services.AddHostedService<LogInitService>();
        
        services.AddKeyedSingleton<IWriteAheadLog>(name, (provider, key) => factory.Invoke(provider, (string) key!));
        
        return services;
    }

}
