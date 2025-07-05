using Microsoft.Extensions.Options;

namespace Jaahas.WriteAheadLog.DependencyInjection.Internal;

/// <summary>
/// Configures <see cref="WriteAheadLogOptions"/> to ensure that the <see cref="WriteAheadLogOptions.Name"/>
/// property matches the name of the options instance.
/// </summary>
/// <typeparam name="TOptions">
///   The type of the write-ahead log options.
/// </typeparam>
internal class WriteAheadLogPostConfigureOptions<TOptions> : IPostConfigureOptions<TOptions> where TOptions : WriteAheadLogOptions, new() {

    /// <inheritdoc />
    public void PostConfigure(string? name, TOptions options) {
        options.Name = name ?? string.Empty;
    }

}
