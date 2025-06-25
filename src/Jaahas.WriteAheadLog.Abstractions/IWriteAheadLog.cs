using System.Buffers;

namespace Jaahas.WriteAheadLog;

public interface IWriteAheadLog : IAsyncDisposable {

    /// <summary>
    /// Initializes the log.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="ValueTask"/> that completes when the log is initialised.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The log has been disposed.
    /// </exception>
    ValueTask InitAsync(CancellationToken cancellationToken = default);


    /// <summary>
    /// Writes a log entry.
    /// </summary>
    /// <param name="data">
    ///  The log entry data.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="WriteResult"/> containing the sequence ID and timestamp of the written entry.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The log has been disposed.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="data"/> is <see langword="null"/>.
    /// </exception>
    ValueTask<WriteResult> WriteAsync(ReadOnlySequence<byte> data, CancellationToken cancellationToken = default);


    /// <summary>
    /// Reads entries from the log.
    /// </summary>
    /// <param name="options">
    ///   The options for reading from the log.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A stream of <see cref="LogEntry"/> objects read from the log. Ensure that you dispose of
    ///   each <see cref="LogEntry"/> instance after use to ensure that underlying resources are
    ///   released.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The log has been disposed.
    /// </exception>
    IAsyncEnumerable<LogEntry> ReadAllAsync(LogReadOptions options, CancellationToken cancellationToken = default);

}
