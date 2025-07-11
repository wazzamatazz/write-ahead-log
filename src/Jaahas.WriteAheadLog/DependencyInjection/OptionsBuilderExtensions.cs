using Jaahas.WriteAheadLog.DependencyInjection.Internal;
using Jaahas.WriteAheadLog.Internal;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Jaahas.WriteAheadLog.DependencyInjection;

/// <summary>
/// Extensions for <see cref="OptionsBuilder{TOptions}"/>
/// </summary>
public static class OptionsBuilderExtensions {

    /// <summary>
    /// Adds default post-configuration for the write-ahead log <typeparamref name="TOptions"/> to
    /// the <see cref="OptionsBuilder{TOptions}"/>.
    /// </summary>
    /// <param name="builder">
    ///   The <see cref="OptionsBuilder{TOptions}"/> to which the post-configuration is added.
    /// </param>
    /// <typeparam name="TOptions">
    ///   The write-ahead log options type.
    /// </typeparam>
    /// <returns>
    ///   The <see cref="OptionsBuilder{TOptions}"/> with the post-configuration added.
    /// </returns>
    public static OptionsBuilder<TOptions> AddDefaultPostConfigureOptions<TOptions>(
        this OptionsBuilder<TOptions> builder
    ) where TOptions : WriteAheadLogOptions, new() {
        ExceptionHelper.ThrowIfNull(builder);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IPostConfigureOptions<TOptions>, WriteAheadLogPostConfigureOptions<TOptions>>());
        
        return builder;
    }

}
