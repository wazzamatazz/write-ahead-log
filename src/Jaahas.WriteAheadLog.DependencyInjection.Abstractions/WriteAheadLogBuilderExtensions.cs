using Jaahas.WriteAheadLog.DependencyInjection.Internal;

using Microsoft.Extensions.DependencyInjection;

namespace Jaahas.WriteAheadLog.DependencyInjection;

public static class WriteAheadLogBuilderExtensions {

    public static IWriteAheadLogBuilder AddWriteAheadLog<TImplementation>(this IWriteAheadLogBuilder builder, Func<IServiceProvider, string, TImplementation> factory) where TImplementation : class, IWriteAheadLog
        => builder.AddWriteAheadLog(string.Empty, factory);
    
    
    public static IWriteAheadLogBuilder AddWriteAheadLog<TImplementation>(this IWriteAheadLogBuilder builder, string name, Func<IServiceProvider, string, TImplementation> factory) where TImplementation : class, IWriteAheadLog {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(factory);
        
        builder.Services.AddKeyedSingleton<IWriteAheadLog>(name, (provider, key) => factory.Invoke(provider, (string) key!));
        builder.Services.AddSingleton<IWriteAheadLog>(provider => provider.GetRequiredKeyedService<IWriteAheadLog>(name));
        
        return builder;
    }

}
