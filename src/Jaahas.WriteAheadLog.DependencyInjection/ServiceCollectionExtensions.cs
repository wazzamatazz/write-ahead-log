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
    /// Adds a Write-Ahead Log (WAL) service to the <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">
    ///   The <see cref="IServiceCollection"/>.
    /// </param>
    /// <param name="configure">
    ///   An action to configure the <see cref="LogOptions"/> for the WAL service.
    /// </param>
    /// <returns>
    ///   The <see cref="IServiceCollection"/>.
    /// </returns>
    /// <remarks>
    ///   The <see cref="Log"/> service is registered as a singleton.
    /// </remarks>
    public static IServiceCollection AddWriteAheadLog(this IServiceCollection services, Action<LogOptions> configure) {
        services.AddWriteAheadLog(string.Empty, configure);
        services.AddSingleton(provider => provider.GetRequiredKeyedService<Log>(string.Empty));
        
        return services;
    }
    
    
    /// <summary>
    /// Adds a named Write-Ahead Log (WAL) service to the <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">
    ///   The <see cref="IServiceCollection"/>.
    /// </param>
    /// <param name="name">
    ///   The name of the WAL service.
    /// </param>
    /// <param name="configure">
    ///   An action to configure the <see cref="LogOptions"/> for the WAL service.
    /// </param>
    /// <returns>
    ///   The <see cref="IServiceCollection"/>.
    /// </returns>
    /// <remarks>
    ///   The <see cref="Log"/> service is registered as a singleton.
    /// </remarks>
    public static IServiceCollection AddWriteAheadLog(this IServiceCollection services, string name, Action<LogOptions> configure) {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);

        services.TryAddSingleton(TimeProvider.System);
        services.AddOptions<LogOptions>(name).Configure(configure);
        
        services.AddKeyedSingleton(name, (provider, key) => {
            var options = provider.GetRequiredService<IOptionsMonitor<LogOptions>>();
            var opts = options.Get((string?) key);
            return ActivatorUtilities.CreateInstance<Log>(provider, opts);
        });
        services.AddSingleton(new KeyedLogRegistration(name));

        // Will only be registered once, even if multiple logs are registered.
        services.AddHostedService<LogInitService>();
        
        return services;
    }

}
