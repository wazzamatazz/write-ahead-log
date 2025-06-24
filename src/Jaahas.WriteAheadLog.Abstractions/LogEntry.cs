using System.Buffers;

using Microsoft.Extensions.ObjectPool;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// Describes a single log entry in the Write-Ahead Log (WAL).
/// </summary>
/// <remarks>
///   <see cref="LogEntry"/> instance must be disposed after use to release the underlying buffer
///   holding the serialized entry.
/// </remarks>
public class LogEntry : IDisposable {

    /// <summary>
    /// The pool used to rent and return <see cref="LogEntry"/> instances.
    /// </summary>
    private static readonly ObjectPool<LogEntry> s_pool = new DefaultObjectPool<LogEntry>(new PooledLogEntryPolicy());

    /// <summary>
    /// Specifies if the <see cref="LogEntry"/> has been disposed.
    /// </summary>
    private bool _disposed;
    
    /// <summary>
    /// The pool that the <see cref="_buffer"/> was obtained from.
    /// </summary>
    private ArrayPool<byte>? _bufferPool;
    
    /// <summary>
    /// The buffer holding the serialized entry.
    /// </summary>
    private byte[] _buffer = null!;

    /// <summary>
    /// The sequence ID for the log entry.
    /// </summary>
    public ulong SequenceId { get; private set; }
    
    /// <summary>
    /// The timestamp of the log entry, measured in ticks.
    /// </summary>
    public long Timestamp { get; private set; }

    /// <summary>
    /// The data segment of the log entry, which contains the actual message payload.
    /// </summary>
    public ArraySegment<byte> Data { get; private set; }

    
    /// <summary>
    /// Creates a new <see cref="LogEntry"/> instance.
    /// </summary>
    private LogEntry() { }
    

    /// <summary>
    /// Creates a new <see cref="LogEntry"/> instance with the specified sequence ID, timestamp,
    /// and data payload.
    /// </summary>
    /// <param name="sequenceId">
    ///   The sequence ID for the log entry.
    /// </param>
    /// <param name="timestamp">
    ///   The timestamp for the log entry, measured in ticks.
    /// </param>
    /// <param name="data">
    ///   The data payload to copy to the log entry.
    /// </param>
    /// <param name="bufferPool">
    ///   The <see cref="ArrayPool{T}"/> to obtain the instance's underlying buffer from. Specify
    ///   <see langword="null"/> to use <see cref="ArrayPool{T}.Shared"/>.
    /// </param>
    /// <returns>
    ///   A new <see cref="LogEntry"/> instance containing the specified sequence ID, timestamp,
    ///   and payload data.
    /// </returns>
    public static LogEntry Create(ulong sequenceId, long timestamp, ReadOnlySpan<byte> data, ArrayPool<byte>? bufferPool = null) {
        bufferPool ??= ArrayPool<byte>.Shared;
        var entry = s_pool.Get();
        var buffer = bufferPool.Rent(data.Length);
        data.CopyTo(buffer);
        entry.Update(sequenceId, timestamp, buffer, 0, data.Length, bufferPool);
        return entry;
    }


    /// <summary>
    /// Creates a new <see cref="LogEntry"/> instance with the specified sequence ID, timestamp,
    /// and data payload.
    /// </summary>
    /// <param name="sequenceId">
    ///   The sequence ID for the log entry.
    /// </param>
    /// <param name="timestamp">
    ///   The timestamp for the log entry, measured in ticks.
    /// </param>
    /// <param name="buffer">
    ///   The buffer containing the message payload.
    /// </param>
    /// <param name="offset">
    ///   The <paramref name="buffer"/> offset that the message payload starts at.
    /// </param>
    /// <param name="count">
    ///   The length of the message payload.
    /// </param>
    /// <param name="bufferPool">
    ///   The <see cref="ArrayPool{T}"/> that the <paramref name="buffer"/> was obtained from.
    ///   Specify <see langword="null"/> if the <paramref name="buffer"/> was not rented from a
    ///   pool.
    /// </param>
    /// <returns></returns>
    public static LogEntry Create(ulong sequenceId, long timestamp, byte[] buffer, int offset, int count, ArrayPool<byte>? bufferPool) {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(bufferPool);
        
        var entry = s_pool.Get();
        entry.Update(sequenceId, timestamp, buffer, offset, count, bufferPool);
        return entry;
    }


    private void Update(ulong sequenceId, long timestamp, byte[] buffer, int offset, int count, ArrayPool<byte>? bufferPool) {
        SequenceId = sequenceId;
        Timestamp = timestamp;
        if (_buffer != null && _bufferPool != null) {
            _bufferPool.Return(_buffer);
        }

        _buffer = buffer;
        _bufferPool = bufferPool;
        Data = new ArraySegment<byte>(_buffer, offset, count);

        _disposed = false;
    }
    

    private void Reset() {
        if (_buffer != null && _bufferPool != null) {
            _bufferPool.Return(_buffer);
            _buffer = null!;
            _bufferPool = null;
        }
        SequenceId = 0;
        Timestamp = 0;
        Data = default;
    }


    /// <inheritdoc/>
    public void Dispose() {
        if (_disposed) {
            return;
        }
        
        s_pool.Return(this);

        _disposed = true;
    }
    

    /// <summary>
    /// Policy for renting and returning <see cref="LogEntry"/> instances from the object pool.
    /// </summary>
    private class PooledLogEntryPolicy : PooledObjectPolicy<LogEntry> {

        /// <inheritdoc />
        public override LogEntry Create() => new LogEntry();


        /// <inheritdoc />
        public override bool Return(LogEntry obj) {
            obj.Reset();
            return true;
        }

    }
    
}
