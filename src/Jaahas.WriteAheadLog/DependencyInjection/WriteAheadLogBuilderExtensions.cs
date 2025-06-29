using Jaahas.WriteAheadLog.DependencyInjection.Internal;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Jaahas.WriteAheadLog.DependencyInjection;

/// <summary>
/// Extensions for <see cref="IWriteAheadLogBuilder"/>.
/// </summary>
public static class WriteAheadLogBuilderExtensions {
    
    /// <summary>
    /// Adds core services to the <see cref="IWriteAheadLogBuilder"/>.
    /// </summary>
    /// <param name="builder">
    ///   The <see cref="IWriteAheadLogBuilder"/>.
    /// </param>
    /// <returns>
    ///   The <see cref="IWriteAheadLogBuilder"/>.
    /// </returns>
    public static IWriteAheadLogBuilder AddCoreServices(
        this IWriteAheadLogBuilder builder
    ) {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<WriteAheadLogFactory>();
        
        return builder;
    }
    
    
    /// <summary>
    /// Adds a write-ahead log registration to the <see cref="IWriteAheadLogBuilder"/>.
    /// </summary>
    /// <param name="builder">
    ///   The <see cref="IWriteAheadLogBuilder"/>.
    /// </param>
    /// <param name="name">
    ///   The name of the log.
    /// </param>
    /// <param name="implementationFactory">
    ///   A factory function that creates an instance of the write-ahead log.
    /// </param>
    /// <typeparam name="TImplementation">
    ///   The type of the write-ahead log implementation.
    /// </typeparam>
    /// <returns>
    ///   The <see cref="IWriteAheadLogBuilder"/>.
    /// </returns>
    public static IWriteAheadLogBuilder AddLog<TImplementation>(
        this IWriteAheadLogBuilder builder, 
        string name, 
        Func<IServiceProvider, TImplementation> implementationFactory
    ) where TImplementation : class, IWriteAheadLog {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(implementationFactory);

        builder.Services.AddSingleton(new WriteAheadLogRegistration(name, implementationFactory));
        builder.Services.AddTransient<IWriteAheadLog>(provider => provider.GetRequiredService<WriteAheadLogFactory>().GetWriteAheadLog(name)!);
        builder.Services.AddKeyedTransient<IWriteAheadLog>(name, (provider, _) => provider.GetRequiredService<WriteAheadLogFactory>().GetWriteAheadLog(name)!);
        
        return builder;
    }
    
}
