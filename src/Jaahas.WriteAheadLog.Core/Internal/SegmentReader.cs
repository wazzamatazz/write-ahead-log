using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// Base class for reading segments from a write-ahead log.
/// </summary>
internal abstract class SegmentReader {
    
    /// <summary>
    /// Reads a segment header from the specified stream.
    /// </summary>
    /// <param name="stream">
    ///   The stream to read the segment header from.
    /// </param>
    /// <returns>
    ///   The <see cref="SegmentHeader"/> read from the stream.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="stream"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidDataException">
    ///   A valid segment header could not be read from the stream.
    /// </exception>
    public static SegmentHeader ReadHeader(Stream stream) {
        ArgumentNullException.ThrowIfNull(stream);
        return SegmentWriter.ReadHeaderFromStream(stream);
    }


    /// <summary>
    /// Reads log entries from the specified <see cref="PipeReader"/> until the reader completes
    /// or cancellation is requested.
    /// </summary>
    /// <param name="pipeReader">
    ///   The <see cref="PipeReader"/> to read log entries from.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   An asynchronous sequence of <see cref="LogEntry"/> instances read from the pipe reader.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="pipeReader"/> is <see langword="null"/>.
    /// </exception>
    protected static async IAsyncEnumerable<LogEntry> ReadLogEntriesAsync(PipeReader pipeReader, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(pipeReader);

        var @continue = true;
        
        while (!cancellationToken.IsCancellationRequested && @continue) {
            var readResult = await pipeReader.ReadAsync(cancellationToken).ConfigureAwait(false);
            var buffer = readResult.Buffer;

            try {
                if (buffer.IsEmpty) {
                    // No data to process.
                    pipeReader.AdvanceTo(buffer.Start, buffer.End);
                    continue;
                }

                while (LogEntry.TryRead(ref buffer, out var entry)) {
                    yield return entry;
                }

                pipeReader.AdvanceTo(buffer.Start, buffer.End);
            }
            finally {
                if (readResult.IsCompleted || readResult.IsCanceled) {
                    @continue = false;
                }
            }
        }
    }

}
