using System.Buffers;
using System.Runtime.CompilerServices;

using Jaahas.WriteAheadLog.Internal;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// Extensions for <see cref="IWriteAheadLog"/>.
/// </summary>
public static class WriteAheadLogExtensions {

    /// <summary>
    /// Writes a log message to the current segment.
    /// </summary>
    /// <param name="log">
    ///   The <see cref="IWriteAheadLog"/> to write the log message to.
    /// </param>
    /// <param name="data">
    ///   The log message to write.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="WriteResult"/> containing the sequence ID and timestamp of the written
    ///   message.
    /// </returns>
    public static ValueTask<WriteResult> WriteAsync(this IWriteAheadLog log, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) {
        ExceptionHelper.ThrowIfNull(log);
        return log.WriteAsync(new ReadOnlySequence<byte>(data), cancellationToken);
    }
    

    /// <summary>
    /// Reads entries from the log.
    /// </summary>
    /// <param name="log">
    ///   The <see cref="IWriteAheadLog"/> to read from.
    /// </param>
    /// <param name="position">
    ///   The position to start reading from.
    /// </param>
    /// <param name="limit">
    ///   The maximum number of entries to read. Specify less than 1 for no limit.
    /// </param>
    /// <param name="watchForChanges">
    ///   If <see langword="true"/>, the operation will continue to watch for changes once it
    ///   reaches the end of the log. Otherwise, the operation will complete when it reaches the
    ///   end of the log.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   An asynchronous sequence of <see cref="LogEntry"/> instances read from the log.
    /// </returns>
    public static async IAsyncEnumerable<LogEntry> ReadAsync(this IWriteAheadLog log, LogPosition position = default, long limit = -1, bool watchForChanges = false, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        ExceptionHelper.ThrowIfNull(log);
        await foreach (var entry in log.ReadAsync(new LogReadOptions(Position: position, Limit: limit, WatchForChanges: watchForChanges), cancellationToken).ConfigureAwait(false)) {
            yield return entry;
        }
    }

}
