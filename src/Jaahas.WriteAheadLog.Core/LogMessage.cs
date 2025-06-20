using System.Buffers;

using Microsoft.IO;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// A message to be written to the Write-Ahead Log (WAL).
/// </summary>
/// <remarks>
///
/// <para>
///   <see cref="LogMessage"/> implements <see cref="IBufferWriter{T}"/> to allow for efficient
///   writing of message data.
/// </para>
///
/// <para>
///   <see cref="LogMessage"/> makes use of shared buffers to reduce allocations. Dispose of the
///   instance when it is no longer needed to return the buffer to the pool. You can reuse the
///   same instance to write multiple log messages by calling <see cref="Reset"/> to clear the
///   underlying buffer after each write to the <see cref="Log"/>.
/// </para>
/// 
/// </remarks>
public sealed class LogMessage : IBufferWriter<byte>, IDisposable {

    /// <summary>
    /// Specifies if the instance has been disposed.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// The underlying stream used to write the log message data.
    /// </summary>
    internal RecyclableMemoryStream? Stream { get; private set; }
    
    
    /// <summary>
    /// Creates a new <see cref="LogMessage"/> instance.
    /// </summary>
    public LogMessage() { }
    
    
    /// <summary>
    /// Creates a new <see cref="LogMessage"/> instance containing the specified data.
    /// </summary>
    /// <param name="data">
    ///   The data to write to the log message.
    /// </param>
    public LogMessage(ReadOnlySpan<byte> data) {
        this.Write(data);
    }
    
    
    private RecyclableMemoryStream GetStream(int sizeHint) {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Stream ??= Log.RecyclableMemoryStreamManager.GetStream(
            id: Guid.NewGuid(),
            tag: null,
            requiredSize: sizeHint);
    }


    /// <inheritdoc />
    public void Advance(int count) => Stream?.Advance(count);


    /// <inheritdoc />
    public Memory<byte> GetMemory(int sizeHint = 0) => GetStream(sizeHint).GetMemory(sizeHint);


    /// <inheritdoc />
    public Span<byte> GetSpan(int sizeHint = 0) => GetStream(sizeHint).GetSpan(sizeHint);
    
    
    /// <summary>
    /// Resets the underlying buffer, allowing the <see cref="LogMessage"/> instance to be
    /// reused.
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    ///   The instance has already been disposed.
    /// </exception>
    public void Reset() {
        ObjectDisposedException.ThrowIf(_disposed, this);
        Stream?.Dispose();
        Stream = null;
    }
    
    
    /// <inheritdoc />
    public void Dispose() {
        if (_disposed) {
            return;
        }
        
        Stream?.Dispose();
        
        _disposed = true;
    }

}
