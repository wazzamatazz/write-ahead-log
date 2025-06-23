using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;

using Nito.AsyncEx;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// <see cref="ICheckpointStore"/> implementation that stores the checkpoint in a file.
/// </summary>
/// <remarks>
///   <see cref="FileCheckpointStore"/> uses a <see cref="MemoryMappedFile"/> for efficient
///   updates to the checkpoint data.
/// </remarks>
public sealed class FileCheckpointStore : ICheckpointStore, IAsyncDisposable {

    private const int SerializedPositionSize = 2 + 8; // 2 bytes for the position type, 8 bytes for the position value.
    
    private bool _disposed;
    
    private readonly MemoryMappedFile _memoryMappedFile;

    private readonly MemoryMappedViewAccessor _memoryMappedViewAccessor;
    
    private readonly AsyncReaderWriterLock _lock = new AsyncReaderWriterLock();
    
    private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();
    
    private readonly AsyncManualResetEvent _flushCompleted = new AsyncManualResetEvent(set: true);


    /// <summary>
    /// Creates a new <see cref="FileCheckpointStore"/> instance.
    /// </summary>
    /// <param name="options">
    ///   The checkpoint store options.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="options"/> is <see langword="null"/>.
    /// </exception>
    public FileCheckpointStore(FileCheckpointStoreOptions options) {
        ArgumentNullException.ThrowIfNull(options);
        
        var dataDir = Path.IsPathRooted(options.DataDirectory) 
            ? options.DataDirectory 
            : Path.Combine(AppContext.BaseDirectory, options.DataDirectory);

        var file = new FileInfo(Path.Combine(dataDir, options.Name));

        var isNewFile = !file.Exists;
        if (isNewFile) {
            file.Directory!.Create();
        }
        
        var stream = file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        if (isNewFile || stream.Length != SerializedPositionSize) {
            stream.SetLength(SerializedPositionSize);
        }
        
        _memoryMappedFile = MemoryMappedFile.CreateFromFile(
            stream, 
            mapName: null, 
            capacity: 0, // Use file size
            access: MemoryMappedFileAccess.ReadWrite,
            inheritability: HandleInheritability.None,
            leaveOpen: false);

        _memoryMappedViewAccessor = _memoryMappedFile.CreateViewAccessor(
            offset: 0,
            size: SerializedPositionSize,
            access: MemoryMappedFileAccess.ReadWrite);
        
        if (options.FlushInterval > TimeSpan.Zero) {
            // Start a background task to flush the checkpoint periodically
            _ = RunBackgroundFlushAsync(options.FlushInterval, _disposedTokenSource.Token);
        }
    }


    /// <inheritdoc />
    public async ValueTask SaveCheckpointAsync(LogPosition position, CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var _ = await _lock.WriterLockAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        SaveCheckpoint(position);
    }


    /// <inheritdoc />
    public async ValueTask<LogPosition> LoadCheckpointAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var _ = await _lock.ReaderLockAsync(cancellationToken).ConfigureAwait(true);
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        return LoadCheckpoint();
    }


    /// <summary>
    /// Flushes pending changes to disk.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var _ = await _lock.WriterLockAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        FlushCore();
    }
    
    
    /// <summary>
    /// Waits until any pending changes have been flushed.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    public async ValueTask WaitForFlushAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        // Wait for the flush to complete
        await _flushCompleted.WaitAsync(cancellationToken).ConfigureAwait(false);
    }
    

    private unsafe void SaveCheckpoint(LogPosition position) {
        // Get a pointer to the mapped memory
        byte* ptr = null; 
        _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        try {
            // Create a span over the mapped memory
            var span = new Span<byte>(ptr, SerializedPositionSize);
            
            if (position.Timestamp.HasValue) {
                // Timestamp-based position
                span[0] = 0x54; // 'T'
                span[1] = 0x53; // 'S'
                BinaryPrimitives.WriteInt64LittleEndian(span[2..], position.Timestamp.Value);
            }
            else {
                // Sequence ID-based position. This is also the fallback if neither timestamp nor
                // sequence ID is provided.
                span[0] = 0x49; // 'I'
                span[1] = 0x44; // 'D'
                BinaryPrimitives.WriteUInt64LittleEndian(span[2..], position.SequenceId ?? 0);
            }
        }
        finally {
            _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        _flushCompleted.Reset();
    }


    private unsafe LogPosition LoadCheckpoint() {
        // Get a pointer to the mapped memory
        byte* ptr = null; 
        _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        try {
            // Create a span over the mapped memory
            var span = new Span<byte>(ptr, SerializedPositionSize);
            switch (span[0]) {
                // Timestamp-based position has prefix "TS"
                case 0x54 when span[1] == 0x53: {
                    // Timestamp-based position
                    var timestamp = BinaryPrimitives.ReadInt64LittleEndian(span[2..]);
                    return LogPosition.FromTimestamp(timestamp);
                }
                // Sequence ID-based position has prefix "ID"
                case 0x49 when span[1] == 0x44: {
                    // Sequence ID-based position
                    var sequenceId = BinaryPrimitives.ReadUInt64LittleEndian(span[2..]);
                    return LogPosition.FromSequenceId(sequenceId);
                }
                default: {
                    // Invalid data, return a default position
                    return default;
                }
            }
        }
        finally {
            _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    
    private async Task RunBackgroundFlushAsync(TimeSpan interval, CancellationToken cancellationToken) {
        try {
            while (!cancellationToken.IsCancellationRequested) {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
                using var _ = await _lock.WriterLockAsync(cancellationToken).ConfigureAwait(false);
                FlushCore();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }
    

    private void FlushCore() {
        if (_flushCompleted.IsSet) {
            return;
        }

        _memoryMappedViewAccessor.Flush();
        _flushCompleted.Set();
    }


    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }

        await _disposedTokenSource.CancelAsync().ConfigureAwait(false);
        using var _ = await _lock.WriterLockAsync().ConfigureAwait(false);
        FlushCore();
        
        _memoryMappedViewAccessor.Dispose();
        _memoryMappedFile.Dispose();

        _disposed = true;
    }

}
