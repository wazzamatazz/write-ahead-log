using System.Buffers;
using System.Runtime.CompilerServices;

using Jaahas.WriteAheadLog.Internal;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// Extensions for <see cref="IWriteAheadLog"/>.
/// </summary>
public static class WriteAheadLogExtensions {
    
    /// <summary>
    /// Writes a log message to the segment with the specified sequence ID.
    /// </summary>
    /// <param name="writer">
    ///   The <see cref="SegmentWriter"/> to write the log message to.
    /// </param>
    /// <param name="data">
    ///   The log message to write to the segment.
    /// </param>
    /// <param name="sequenceId">
    ///   The sequence ID of the log message. This should be unique and monotonically increasing.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   The number of bytes written to the segment.
    /// </returns>
    internal static ValueTask<long> WriteAsync(this SegmentWriter writer, ReadOnlyMemory<byte> data, ulong sequenceId, CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(writer);
        return writer.WriteAsync(new ReadOnlySequence<byte>(data), sequenceId, cancellationToken);
    }
    

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
        ArgumentNullException.ThrowIfNull(log);
        return log.WriteAsync(new ReadOnlySequence<byte>(data), cancellationToken);
    }
    

    /// <summary>
    /// Reads log entries starting from a specific sequence ID.
    /// </summary>
    /// <param name="log">
    ///   The <see cref="IWriteAheadLog"/> to read from.
    /// </param>
    /// <param name="position">
    ///   The position to start reading from.
    /// </param>
    /// <param name="count">
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
    public static async IAsyncEnumerable<LogEntry> ReadAllAsync(this IWriteAheadLog log, LogPosition position = default, long count = -1, bool watchForChanges = false, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(log);
        await foreach (var entry in log.ReadAllAsync(new LogReadOptions(Position: position, Limit: count, WatchForChanges: watchForChanges), cancellationToken).ConfigureAwait(false)) {
            yield return entry;
        }
    }

}
