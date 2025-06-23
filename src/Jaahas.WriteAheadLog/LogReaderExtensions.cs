namespace Jaahas.WriteAheadLog;

/// <summary>
/// Extensions for <see cref="LogReader"/>.
/// </summary>
public static class LogReaderExtensions {

    /// <summary>
    /// Runs the <see cref="LogReader"/> until cancellation is requested.
    /// </summary>
    /// <param name="reader">
    ///   The <see cref="LogReader"/> to run.
    /// </param>
    /// <param name="cancellationToken">
    ///   The <see cref="CancellationToken"/> to use for cancellation. The method will run until
    ///   cancellation is requested.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="reader"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    ///   <paramref name="cancellationToken"/> is <see cref="CancellationToken.None"/>. This method
    ///   is designed to run indefinitely until cancellation is requested, so using <see cref="CancellationToken.None"/>
    ///   would result in a method invocation that can never be cancelled.
    /// </exception>
    public static async Task RunAsync(this LogReader reader, CancellationToken cancellationToken) {
        ArgumentNullException.ThrowIfNull(reader);
        if (cancellationToken == CancellationToken.None) {
            throw new ArgumentException("This method runs until cancellation is requested. CancellationToken.None cannot be used as this would result in a method invocation that could never return.", nameof(cancellationToken));
        }
        
        await reader.StartAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        try {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        finally {
            await reader.StopAsync().ConfigureAwait(false);
        }
    }
    
    
    /// <summary>
    /// Runs the <see cref="LogReader"/> until cancellation is requested.
    /// </summary>
    /// <param name="reader">
    ///   The <see cref="LogReader"/> to run.
    /// </param>
    /// <param name="options">
    ///   The <see cref="LogReaderStartOptions"/> to use when starting the reader. 
    /// </param>
    /// <param name="cancellationToken">
    ///   The <see cref="CancellationToken"/> to use for cancellation. The method will run until
    ///   cancellation is requested.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="reader"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    ///   <paramref name="cancellationToken"/> is <see cref="CancellationToken.None"/>. This method
    ///   is designed to run indefinitely until cancellation is requested, so using <see cref="CancellationToken.None"/>
    ///   would result in a method invocation that can never be cancelled.
    /// </exception>
    public static async Task RunAsync(this LogReader reader, LogReaderStartOptions options, CancellationToken cancellationToken) {
        ArgumentNullException.ThrowIfNull(reader);
        if (cancellationToken == CancellationToken.None) {
            throw new ArgumentException("This method runs until cancellation is requested. CancellationToken.None cannot be used as this would result in a method invocation that could never return.", nameof(cancellationToken));
        }
        
        await reader.StartAsync(options, cancellationToken).ConfigureAwait(false);

        try {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        finally {
            await reader.StopAsync().ConfigureAwait(false);
        }
    }

}
