using System.Buffers;

namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// Extensions for <see cref="SegmentWriter"/>.
/// </summary>
internal static class SegmentWriterExtensions {

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

}
