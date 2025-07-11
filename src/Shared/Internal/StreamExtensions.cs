#if !NETCOREAPP
namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// .NET Standard 2.0 polyfills.
/// </summary>
internal static class StreamExtensions {

    /// <summary>
    /// Disposes of the stream.
    /// </summary>
    /// <param name="stream">
    ///   The stream.
    /// </param>
    /// <returns>
    ///   A <see cref="ValueTask"/> that represents the asynchronous operation.
    /// </returns>
    public static ValueTask DisposeAsync(this Stream stream) {
        stream?.Dispose();
        return default;
    }

}
#endif
