using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// Base class for writing segments in a write-ahead log.
/// </summary>
internal abstract class SegmentWriter : IAsyncDisposable {

    /// <summary>
    /// The size of the serialized segment header in bytes.
    /// </summary>
    protected internal const int SerializedSegmentHeaderSize =
        sizeof(uint) + // magic number (4 bytes)
        sizeof(uint) + // version (4 bytes)
        sizeof(ulong) + // first sequence ID (8 bytes)
        sizeof(ulong) + // last sequence ID (8 bytes)
        sizeof(long) + // first timestamp (8 bytes)
        sizeof(long) + // last timestamp (8 bytes)
        sizeof(long) + // message count (8 bytes)
        sizeof(long) + // size of the segment (8 bytes)
        sizeof(bool) + // read-only flag (1 byte)
        67 + // reserved/padding to align to 128 bytes (67 bytes)
        4; // checksum (4 bytes)

    /// <summary>
    /// Specifies if the segment writer has been disposed.
    /// </summary>
    private bool _disposed;
    
    /// <summary>
    /// The time provider used for generating timestamps when writing log messages.
    /// </summary>
    private readonly TimeProvider _timeProvider;
    
    /// <summary>
    /// Used to signal that the segment writer has been disposed and to cancel any ongoing
    /// operations.
    /// </summary>
    private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();
    
    /// <summary>
    /// Used to signal that the segment writer has been disposed and to cancel any ongoing
    /// operations.
    /// </summary>
    protected CancellationToken DisposedToken => _disposedTokenSource.Token;
    
    /// <summary>
    /// Lock to ensure that only one write operation can occur at a time.
    /// </summary>
    private readonly Nito.AsyncEx.AsyncLock _writeLock = new Nito.AsyncEx.AsyncLock();

    /// <summary>
    /// The options used to configure the segment writer.
    /// </summary>
    private readonly SegmentWriterOptions _options;

    /// <summary>
    /// The name of the segment writer.
    /// </summary>
    public string Name => _options.Name;
    
    /// <summary>
    /// The header of the segment being written to.
    /// </summary>
    public SegmentHeader Header { get; protected init; }
    
    /// <summary>
    /// The expiration time for the segment, if specified.
    /// </summary>
    public DateTimeOffset? NotAfter => _options.NotAfter;


    /// <summary>
    /// Creates a new <see cref="SegmentWriter"/> instance.
    /// </summary>
    /// <param name="options">
    ///   The options used to configure the segment writer.
    /// </param>
    /// <param name="timeProvider">
    ///   The time provider used for generating timestamps when writing log messages. Specify <see langword="null"/>
    ///   to use <see cref="TimeProvider.System"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="options"/> is <see langword="null"/>.
    /// </exception>
    protected SegmentWriter(SegmentWriterOptions options, TimeProvider? timeProvider) {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? TimeProvider.System;
        
        Header = new SegmentHeader() {
            FirstSequenceId = 0,
            LastSequenceId = 0,
            FirstTimestamp = -1,
            LastTimestamp = -1,
            MessageCount = 0,
            Size = 0,
            ReadOnly = false // Default to writable
        };
    }


    /// <summary>
    /// Reads the segment header from the provided stream.
    /// </summary>
    /// <param name="stream">
    ///   The stream to read the segment header from.
    /// </param>
    /// <returns>
    ///   A <see cref="SegmentHeader"/> instance containing the deserialized header information.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="stream"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidDataException">
    ///   The segment header is incomplete, or has an invalid magic number, version, or checksum.
    /// </exception>
    public static SegmentHeader ReadHeaderFromStream(Stream stream) {
        ArgumentNullException.ThrowIfNull(stream);
        
        var buffer = ArrayPool<byte>.Shared.Rent(SerializedSegmentHeaderSize);
        try {
            var bytesRead = stream.Read(buffer, 0, SerializedSegmentHeaderSize);
            if (bytesRead < SerializedSegmentHeaderSize) {
                throw new InvalidDataException("Segment header is incomplete.");
            }
            return DeserializeHeader(new ReadOnlySpan<byte>(buffer, 0, SerializedSegmentHeaderSize));
        }
        finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
    
    
    /// <summary>
    /// Writes a log message to the segment with the specified sequence ID.
    /// </summary>
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
    /// <remarks>
    ///   The timestamp for the log message will be automatically generated by the writer.
    /// </remarks>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="data"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    ///   The segment writer has been disposed.
    /// </exception>
    public ValueTask<long> WriteAsync(ReadOnlySequence<byte> data, ulong sequenceId, CancellationToken cancellationToken = default) => WriteAsync(data, sequenceId, _timeProvider.GetTimestamp(), cancellationToken);
    
    
    /// <summary>
    /// Writes a log message to the segment with the specified sequence ID and timestamp.
    /// </summary>
    /// <param name="data">
    ///   The log message to write to the segment.
    /// </param>
    /// <param name="sequenceId">
    ///   The sequence ID of the log message. This should be unique and monotonically increasing.
    /// </param>
    /// <param name="timestamp">
    ///   The timestamp of the log message.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   The number of bytes written to the segment.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="data"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    ///   The segment writer has been disposed.
    /// </exception>
    internal async ValueTask<long> WriteAsync(ReadOnlySequence<byte> data, ulong sequenceId, long timestamp, CancellationToken cancellationToken) {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Don't add read-only or expiry checks here, as these are handled in the WAL itself.

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        using var handle = await _writeLock.LockAsync(cts.Token).ConfigureAwait(false);
        
        var bytesWritten = await WriteMessageCoreAsync(data, sequenceId, timestamp).ConfigureAwait(false);

        ++Header.MessageCount;
        Header.Size += bytesWritten;
        if (Header.FirstSequenceId == 0) {
            Header.FirstSequenceId = sequenceId;
            Header.FirstTimestamp = timestamp;
        }
        Header.LastSequenceId = sequenceId;
        Header.LastTimestamp = timestamp;

        await WriteHeaderCoreAsync().ConfigureAwait(false);
        
        return bytesWritten;
    }


    /// <summary>
    /// Manually flushes the segment writer to ensure all pending writes are persisted to disk.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <exception cref="ObjectDisposedException">
    ///   The segment writer has been disposed.
    /// </exception>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        using var handle = await _writeLock.LockAsync(cts.Token).ConfigureAwait(false);
        await FlushCoreAsync().ConfigureAwait(false);
    }

    
    /// <summary>
    /// Closes the segment writer, marking the segment as read-only and ensuring all pending writes
    /// are flushed to disk.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <exception cref="ObjectDisposedException">
    ///   The segment writer has been disposed.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    ///   The segment is already closed (read-only).
    /// </exception>
    public async ValueTask CloseSegmentAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        using var handle = await _writeLock.LockAsync(cts.Token).ConfigureAwait(false);
        
        if (Header.ReadOnly) {
            throw new InvalidOperationException("Segment is already closed.");
        }
        
        Header.ReadOnly = true; // Mark the segment as read-only
        
        await FlushCoreAsync().ConfigureAwait(false);
        await CloseSegmentCoreAsync().ConfigureAwait(false);
        await DisposeNoLockAsync().ConfigureAwait(false);
    }
    

    /// <summary>
    /// Serializes the segment header into the provided buffer.
    /// </summary>
    /// <param name="header">
    ///   The segment header to serialize.
    /// </param>
    /// <param name="buffer">
    ///   The buffer to write the serialized header to. Must be at least <see cref="SerializedSegmentHeaderSize"/>
    ///   bytes long.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="header"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    ///   The <paramref name="buffer"/> is too small to hold the serialized segment header.
    /// </exception>
    protected static void SerializeHeader(SegmentHeader header, Span<byte> buffer) { 
        ArgumentNullException.ThrowIfNull(header);
        if (buffer.Length < SerializedSegmentHeaderSize) {
            throw new ArgumentException("Buffer is too small to hold SegmentHeader.", nameof(buffer));
        }

        Constants.SegmentMagicBytes.Span.CopyTo(buffer);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[4..], header.Version);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[8..], header.FirstSequenceId);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[16..], header.LastSequenceId);
        BinaryPrimitives.WriteInt64LittleEndian(buffer[24..], header.FirstTimestamp);
        BinaryPrimitives.WriteInt64LittleEndian(buffer[32..], header.LastTimestamp);
        BinaryPrimitives.WriteInt64LittleEndian(buffer[40..], header.MessageCount);
        BinaryPrimitives.WriteInt64LittleEndian(buffer[48..], header.Size);
        buffer[56] = Convert.ToByte(header.ReadOnly);
        // Clear the reserved bytes
        buffer[57..124].Clear();
        // Calculate checksum over the entire header except the checksum field
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[124..], Crc32.HashToUInt32(buffer[..124]));
        
    }


    /// <summary>
    /// Deserializes a segment header from the provided buffer.
    /// </summary>
    /// <param name="buffer">
    ///   The buffer containing the serialized segment header. Must be at least <see cref="SerializedSegmentHeaderSize"/>
    ///   bytes long.
    /// </param>
    /// <returns>
    ///   The deserialized <see cref="SegmentHeader"/> instance.
    /// </returns>
    /// <exception cref="ArgumentException">
    ///   The <paramref name="buffer"/> is too small to hold the serialized segment header.
    /// </exception>
    /// <exception cref="InvalidDataException">
    ///   The segment header is incomplete, or has an invalid magic number, version, or checksum.
    /// </exception>
    protected static SegmentHeader DeserializeHeader(ReadOnlySpan<byte> buffer) {
        if (buffer.Length < SerializedSegmentHeaderSize) {
            throw new ArgumentException("Buffer is too small to hold SegmentHeader.", nameof(buffer));
        }
        
        // Check magic number
        if (!Constants.SegmentMagicBytes.Span.SequenceEqual(buffer[..4])) {
            throw new InvalidDataException("Invalid magic number.");
        }
        
        // Check version
        if (BinaryPrimitives.ReadUInt32LittleEndian(buffer[4..]) != 1) {
            throw new InvalidDataException("Unsupported version.");
        }

        // Calculate checksum over all but the last 4 bytes (checksum field)
        var checksumActual = Crc32.HashToUInt32(buffer[..(SerializedSegmentHeaderSize - 4)]);
        // Read the expected checksum from the header
        var checksumExpected = BinaryPrimitives.ReadUInt32LittleEndian(buffer[(SerializedSegmentHeaderSize - 4)..]);
        
        if (checksumActual != checksumExpected) {
            throw new InvalidDataException("Checksum mismatch.");
        }
        
        var result = new SegmentHeader {
            Version = BinaryPrimitives.ReadUInt32LittleEndian(buffer[4..]),
            FirstSequenceId = BinaryPrimitives.ReadUInt64LittleEndian(buffer[8..]),
            LastSequenceId = BinaryPrimitives.ReadUInt64LittleEndian(buffer[16..]),
            FirstTimestamp = BinaryPrimitives.ReadInt64LittleEndian(buffer[24..]),
            LastTimestamp = BinaryPrimitives.ReadInt64LittleEndian(buffer[32..]),
            MessageCount = BinaryPrimitives.ReadInt64LittleEndian(buffer[40..]),
            Size = BinaryPrimitives.ReadInt64LittleEndian(buffer[48..]),
            ReadOnly = Convert.ToBoolean(buffer[56])
        };

        return result;
    }
    

    protected abstract ValueTask<long> WriteMessageCoreAsync(ReadOnlySequence<byte> message, ulong sequenceId, long timestamp);
    
    
    protected abstract ValueTask WriteHeaderCoreAsync();


    protected abstract ValueTask FlushCoreAsync();


    protected abstract ValueTask CloseSegmentCoreAsync();

    
    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }

        using var handle = await _writeLock.LockAsync().ConfigureAwait(false);
        await DisposeNoLockAsync().ConfigureAwait(false);
    }
    
    
    private async ValueTask DisposeNoLockAsync() {
        if (_disposed) {
            return;
        }

        await _disposedTokenSource.CancelAsync().ConfigureAwait(false);
        
        // Ensure all completed writes are flushed.
        await FlushCoreAsync().ConfigureAwait(false);
        
        await DisposeCoreAsync().ConfigureAwait(false);
        
        _disposed = true;
    }


    protected abstract ValueTask DisposeCoreAsync();


}
