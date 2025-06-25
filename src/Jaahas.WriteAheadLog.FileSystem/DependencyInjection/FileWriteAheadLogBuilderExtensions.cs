using Jaahas.WriteAheadLog.FileSystem;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

// ReSharper disable once CheckNamespace
namespace Jaahas.WriteAheadLog.DependencyInjection;

/// <summary>
/// Extensions for registering Write-Ahead Log (WAL) services in an <see cref="IServiceCollection"/>.
/// </summary>
public static class FileWriteAheadLogBuilderExtensions {

    /// <summary>
    /// Adds a file-based <see cref="IWriteAheadLog"/> service to the <see cref="IWriteAheadLogBuilder"/>.
    /// </summary>
    /// <param name="builder">
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
    public static IWriteAheadLogBuilder AddFile(this IWriteAheadLogBuilder builder, Action<FileWriteAheadLogOptions> configure) 
        => builder.AddFile(string.Empty, configure);


    /// <summary>
    /// Adds a file-based <see cref="IWriteAheadLog"/> service to the <see cref="IWriteAheadLogBuilder"/>.
    /// </summary>
    /// <param name="builder">
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
    public static IWriteAheadLogBuilder AddFile(this IWriteAheadLogBuilder builder, string name, Action<FileWriteAheadLogOptions> configure) {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);
        
        builder.Services.AddOptions<FileWriteAheadLogOptions>(name)
            .Configure(configure);
        
        return builder.AddLog<FileWriteAheadLog>(name, (provider, key) => ActivatorUtilities.CreateInstance<FileWriteAheadLog>(provider, provider.GetRequiredService<IOptionsMonitor<FileWriteAheadLogOptions>>().Get(key)));
    }

}
