#if !NETCOREAPP
namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// .NET Standard polyfills.
/// </summary>
internal static class CancellationTokenSourceExtensions {

    /// <summary>
    /// Cancels the <see cref="CancellationTokenSource"/>.
    /// </summary>
    /// <param name="cts">
    ///   The <see cref="CancellationTokenSource"/> to cancel.
    /// </param>
    /// <returns>
    ///   A <see cref="Task"/> that represents the asynchronous operation.
    /// </returns>
    public static Task CancelAsync(this CancellationTokenSource cts) {
        cts.Cancel();
        return Task.CompletedTask;
    }

}
#endif
