using System.Buffers;

using Microsoft.IO;

namespace Jaahas.WriteAheadLog;

public sealed class LogMessage : IBufferWriter<byte>, IDisposable {

    private bool _disposed;

    internal RecyclableMemoryStream? Stream { get; private set; }


    public LogMessage() { }
    
    
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
