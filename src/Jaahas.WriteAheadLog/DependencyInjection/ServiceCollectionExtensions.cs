using Jaahas.WriteAheadLog.DependencyInjection.Internal;
using Jaahas.WriteAheadLog.Internal;

using Microsoft.Extensions.DependencyInjection;

namespace Jaahas.WriteAheadLog.DependencyInjection;

/// <summary>
/// Extensions for <see cref="IServiceCollection"/>.
/// </summary>
public static class ServiceCollectionExtensions {

    /// <summary>
    /// Adds Write-Ahead Log services to the <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">
    ///   The <see cref="IServiceCollection"/>.
    /// </param>
    /// <returns>
    ///   An <see cref="IWriteAheadLogBuilder"/> that is used for further configuration of
    ///   Write-Ahead Log services.
    /// </returns>
    public static IWriteAheadLogBuilder AddWriteAheadLogServices(this IServiceCollection services) {
        ExceptionHelper.ThrowIfNull(services);
        
        return new WriteAheadLogBuilder(services).AddCoreServices();
    }

}
