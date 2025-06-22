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

    private bool _disposed;
    
    private readonly MemoryMappedFile _memoryMappedFile;

    private readonly MemoryMappedViewAccessor _memoryMappedViewAccessor;

    private int _flushRequired;
    
    private readonly AsyncReaderWriterLock _lock = new AsyncReaderWriterLock();


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
        
        var stream = file.Open(FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
        if (isNewFile || stream.Length != sizeof(ulong)) {
            stream.SetLength(sizeof(ulong));
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
            size: sizeof(ulong),
            access: MemoryMappedFileAccess.ReadExecute);
    }


    /// <inheritdoc />
    public async ValueTask SaveCheckpointAsync(ulong sequenceId, CancellationToken cancellationToken = default) {
        using var _ = await _lock.WriterLockAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(_disposed, this);
        SaveCheckpoint(sequenceId);
    }


    /// <inheritdoc />
    public async ValueTask<ulong> LoadCheckpointAsync(CancellationToken cancellationToken = default) {
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
        using var _ = await _lock.WriterLockAsync().ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(_disposed, this);
        FlushCore();
    }
    

    private unsafe void SaveCheckpoint(ulong sequenceId) {
        // Get a pointer to the mapped memory
        byte* ptr = null; 
        _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        try {
            // Create a span over the mapped memory
            var span = new Span<byte>(ptr, sizeof(ulong));
            BinaryPrimitives.WriteUInt64LittleEndian(span, sequenceId);
        }
        finally {
            _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        Interlocked.Exchange(ref _flushRequired, 1);
    }


    private unsafe ulong LoadCheckpoint() {
        // Get a pointer to the mapped memory
        byte* ptr = null; 
        _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        try {
            // Create a span over the mapped memory
            var span = new Span<byte>(ptr, sizeof(ulong));
            return BinaryPrimitives.ReadUInt64LittleEndian(span);
        }
        finally {
            _memoryMappedViewAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }


    private void FlushCore() {
        if (Interlocked.CompareExchange(ref _flushRequired, 0, 1) == 0) {
            return;
        }

        _memoryMappedViewAccessor.Flush();
    }


    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }

        using var _ = await _lock.WriterLockAsync().ConfigureAwait(false);
        FlushCore();
        
        _memoryMappedViewAccessor.Dispose();
        _memoryMappedFile.Dispose();

        _disposed = true;
    }

}
