using Jaahas.WriteAheadLog.DependencyInjection.Internal;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Jaahas.WriteAheadLog.DependencyInjection;

public static class ServiceCollectionExtensions {

    public static IWriteAheadLogBuilder AddWriteAheadLogServices(this IServiceCollection services) {
        ArgumentNullException.ThrowIfNull(services);
        
        services.TryAddSingleton(TimeProvider.System);
        services.AddHostedService<LogInitService>();
        
        return new WriteAheadLogBuilder(services);
        
        
    }

}
