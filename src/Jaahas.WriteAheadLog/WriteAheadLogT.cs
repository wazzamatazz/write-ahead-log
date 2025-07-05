using System.Buffers;
using System.Runtime.CompilerServices;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// Base <see cref="IWriteAheadLog"/> implementation.
/// </summary>
public abstract class WriteAheadLog<TOptions> : IWriteAheadLog where TOptions : WriteAheadLogOptions, new() {
    
    private bool _disposed;
    
    private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();
    
    private readonly Nito.AsyncEx.AsyncLock _initLock = new Nito.AsyncEx.AsyncLock();
    
    private readonly Nito.AsyncEx.AsyncLock _writeLock = new Nito.AsyncEx.AsyncLock();
    
    /// <inheritdoc/>
    public WriteAheadLogMetadata Metadata { get; }
    
    /// <summary>
    /// The options for the log, providing additional configuration and capabilities.
    /// </summary>
    protected TOptions Options { get; }
    
    /// <summary>
    /// Specifies whether the log has been initialized.
    /// </summary>
    protected bool Initialized { get; private set; }
    
    /// <summary>
    /// A cancellation token that is triggered when the log is disposed.
    /// </summary>
    protected CancellationToken LifecycleToken => _disposedTokenSource.Token;
    
    
    /// <summary>
    /// Creates a new <see cref="WriteAheadLog{TOptions}"/> instance with the specified options.
    /// </summary>
    /// <param name="options">
    ///   The options for the log. If <see langword="null"/>, a new instance of <typeparamref name="TOptions"/>
    ///   is created with default values.
    /// </param>
    protected WriteAheadLog(TOptions? options) {
        Options = options ?? new TOptions();
        Metadata = new WriteAheadLogMetadata {
            Name = Options.Name ?? string.Empty,
            Description = Options.Description,
            MaxEntryPayloadSize = Options.MaxEntryPayloadSize
        };
    }


    /// <inheritdoc />
    public async ValueTask InitAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_disposedTokenSource.Token, cancellationToken);
        await EnsureInitializedAsync(cts.Token).ConfigureAwait(false);
    }


    protected async ValueTask EnsureInitializedAsync(CancellationToken cancellationToken) {
        if (Initialized) {
            return;
        }
        
        using var _ = await _initLock.LockAsync(cancellationToken).ConfigureAwait(false);
        
        if (Initialized) {
            return;
        }
        
        await InitCoreAsync(cancellationToken).ConfigureAwait(false);
        Initialized = true;
    }
    
    
    protected abstract ValueTask InitCoreAsync(CancellationToken cancellationToken);


    /// <inheritdoc />
    public async ValueTask<WriteResult> WriteAsync(ReadOnlySequence<byte> data, CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (Metadata.MaxEntryPayloadSize > 0 && data.Length > Metadata.MaxEntryPayloadSize) {
            throw new ArgumentException($"Log entry exceeds maximum size of {Metadata.MaxEntryPayloadSize} bytes.", nameof(data));
        }
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_disposedTokenSource.Token, cancellationToken);
        
        await EnsureInitializedAsync(cts.Token).ConfigureAwait(false);
        
        using var _ = await WaitForWriteLockAsync(cts.Token).ConfigureAwait(false);
        
        return await WriteCoreAsync(data, cts.Token).ConfigureAwait(false);
    }
    
    
    protected abstract ValueTask<WriteResult> WriteCoreAsync(ReadOnlySequence<byte> data, CancellationToken cancellationToken);


    /// <inheritdoc />
    public async IAsyncEnumerable<LogEntry> ReadAsync(LogReadOptions options, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_disposedTokenSource.Token, cancellationToken);
        
        await EnsureInitializedAsync(cts.Token).ConfigureAwait(false);
        
        await foreach (var item in ReadCoreAsync(options, cts.Token).ConfigureAwait(false)) {
            yield return item;
        }
    }
    
    
    protected abstract IAsyncEnumerable<LogEntry> ReadCoreAsync(LogReadOptions options, CancellationToken cancellationToken);

    
    protected async ValueTask<IDisposable> WaitForWriteLockAsync(CancellationToken cancellationToken) {
        return await _writeLock.LockAsync(cancellationToken).ConfigureAwait(false);
    }
    

    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }

        using var _ = await _writeLock.LockAsync().ConfigureAwait(false);
        await DisposeAsyncCore().ConfigureAwait(false);
        
        _disposed = true;
    }


    protected virtual async ValueTask DisposeAsyncCore() {
        await _disposedTokenSource.CancelAsync().ConfigureAwait(false);
        _disposedTokenSource.Dispose();
    }

}
