using System.Buffers;

using Microsoft.IO;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// <see cref="LogWriter"/> simplifies writing a stream of messages to a Write-Ahead Log (WAL).
/// </summary>
/// <remarks>
///
/// <para>
///   <see cref="LogWriter"/> implements <see cref="IBufferWriter{T}"/> to allow for efficient
///   writing of message data. Call <see cref="WriteToLogAsync"/> to write the buffered data to the
///   log.
/// </para>
///
/// <para>
///   <see cref="LogWriter"/> makes use of shared buffers to reduce allocations. Dispose of the
///   instance when it is no longer needed to return the buffer to the pool. You can reuse the
///   same instance to write multiple log messages; each time you call <see cref="WriteToLogAsync"/>,
///   the buffer is reset.
/// </para>
/// 
/// </remarks>
public sealed class LogWriter : IBufferWriter<byte>, IDisposable, IAsyncDisposable {

    /// <summary>
    /// Specifies if the instance has been disposed.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// The underlying stream used to write the log message data.
    /// </summary>
    private RecyclableMemoryStream? _stream;
    
    
    private RecyclableMemoryStream GetStream(int sizeHint) {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return _stream ??= FileWriteAheadLog.RecyclableMemoryStreamManager.GetStream(
            id: Guid.NewGuid(),
            tag: null,
            requiredSize: sizeHint);
    }


    /// <summary>
    /// Writes the buffered log message to the specified <see cref="IWriteAheadLog"/> and resets
    /// the buffer.
    /// </summary>
    /// <param name="log">
    ///   The <see cref="IWriteAheadLog"/> to write the buffered log message to.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="WriteResult"/> containing the sequence ID and timestamp of the written
    ///   message.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The <see cref="LogWriter"/> has already been disposed.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    ///   <paramref cref="log"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    ///   The <see cref="LogWriter"/> does not have any pending message data to publish.
    /// </exception>
    public async ValueTask<WriteResult> WriteToLogAsync(IWriteAheadLog log, CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(log);
        if (_stream == null) {
            throw new InvalidOperationException("No message data to publish.");
        }

        try {
            var sequence = _stream.GetReadOnlySequence();
            return await log.WriteAsync(sequence, cancellationToken).ConfigureAwait(false);
        }
        finally {
            Reset();
        }
    }
    

    /// <inheritdoc />
    public void Advance(int count) => _stream?.Advance(count);


    /// <inheritdoc />
    public Memory<byte> GetMemory(int sizeHint = 0) => GetStream(sizeHint).GetMemory(sizeHint);


    /// <inheritdoc />
    public Span<byte> GetSpan(int sizeHint = 0) => GetStream(sizeHint).GetSpan(sizeHint);
    
    
    /// <summary>
    /// Resets the underlying buffer, allowing the <see cref="LogWriter"/> instance to be
    /// reused.
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    ///   The instance has already been disposed.
    /// </exception>
    private void Reset() {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _stream?.Dispose();
        _stream = null;
    }
    
    
    /// <inheritdoc />
    public void Dispose() {
        if (_disposed) {
            return;
        }
        
        _stream?.Dispose();
        _stream = null;
        
        _disposed = true;
    }


    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }
        
        if (_stream is not null) {
            await _stream.DisposeAsync().ConfigureAwait(false);
            _stream = null;
        }
        
        _disposed = true;
    }

}
