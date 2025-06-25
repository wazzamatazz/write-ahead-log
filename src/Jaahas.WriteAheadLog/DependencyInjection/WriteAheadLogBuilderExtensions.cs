using Jaahas.WriteAheadLog.DependencyInjection.Internal;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Jaahas.WriteAheadLog.DependencyInjection;

public static class WriteAheadLogBuilderExtensions {
    
    public static IWriteAheadLogBuilder AddLog<TImplementation>(
        this IWriteAheadLogBuilder builder, 
        string name, 
        Func<IServiceProvider, string, TImplementation> implementationFactory
    ) where TImplementation : class, IWriteAheadLog {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(implementationFactory);
        
        builder.Services.AddKeyedSingleton<IWriteAheadLog>(name, (provider, key) => implementationFactory.Invoke(provider, (string) key!));
        builder.Services.AddSingleton<IWriteAheadLog>(provider => provider.GetRequiredKeyedService<IWriteAheadLog>(name));
        
        return builder;
    }
    
}
