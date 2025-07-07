using System.Runtime.CompilerServices;

using Microsoft.Extensions.Logging;

using Nito.AsyncEx;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// <see cref="LogReader"/> simplifies reading and processing entries from a Write-Ahead Log (WAL).
/// </summary>
/// <remarks>
///
/// <para>
///   <see cref="LogReader"/> does not provide a method for directly reading log entries. Instead,
///   the <see cref="ProcessEntry"/> event is raised for each entry read from the log. If an
///   exception occurs while processing an entry, the <see cref="ProcessError"/> event is raised.
///   <see cref="LogEntry"/> instances received from the log are automatically disposed once the
///   processing of the entry is complete.
/// </para>
///
/// <para>
///   The <see cref="LogReader"/> can be started and stopped using the <see cref="StartAsync"/>
///   and <see cref="StopAsync"/> methods respectively. The reader can be started and stopped
///   multiple times and by default will continue processing entries from the last-known position
///   in the log each time it starts.
/// </para>
///
/// <para>
///   Passing an <see cref="ICheckpointStore"/> to the constructor allows the <see cref="LogReader"/>
///   to persist its last-known position in the log, allowing the reader position to survive
///   disposal of the <see cref="LogReader"/> itself.
/// </para>
/// 
/// </remarks>
public sealed partial class LogReader : IAsyncDisposable {

    private bool _disposed;
    
    private readonly ILogger<LogReader> _logger;

    private readonly IWriteAheadLog _log;

    private readonly ICheckpointStore? _checkpointStore;
    
    private LogPosition _currentPosition;
    
    private readonly AsyncLock _startLock = new AsyncLock();

    private readonly AsyncManualResetEvent _running = new AsyncManualResetEvent(false);
    
    private bool _skipInitialPositionCheck;
    
    private readonly AsyncAutoResetEvent _stopped = new AsyncAutoResetEvent(false);

    private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();
    
    /// <summary>
    /// Raised when an entry is read from the log.
    /// </summary>
    /// <remarks>
    ///   The <see cref="LogEntry"/> specified in the <see cref="ProcessLogReaderEntryArgs"/> is
    ///   automatically disposed by the <see cref="LogReader"/> when no longer required.
    /// </remarks>
    public event Func<ProcessLogReaderEntryArgs, Task>? ProcessEntry;
    
    /// <summary>
    /// Raised when a handler for the <see cref="ProcessEntry"/> event throws an exception.
    /// </summary>
    /// <remarks>
    ///   The <see cref="LogEntry"/> specified in the <see cref="ProcessLogReaderErrorArgs"/> is
    ///   automatically disposed by the <see cref="LogReader"/> when no longer required.
    /// </remarks>
    public event Func<ProcessLogReaderErrorArgs, Task>? ProcessError;


    /// <summary>
    /// Creates a new <see cref="LogReader"/> instance.
    /// </summary>
    /// <param name="log">
    ///   The <see cref="IWriteAheadLog"/> to read entries from.
    /// </param>
    /// <param name="checkpointStore">
    ///   The optional <see cref="ICheckpointStore"/> to use for persisting the last-known log
    ///   position for the reader.
    /// </param>
    /// <param name="logger">
    ///   The logger for the <see cref="LogReader"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="log"/> is <see langword="null"/>.
    /// </exception>
    public LogReader(IWriteAheadLog log, ICheckpointStore? checkpointStore, ILogger<LogReader>? logger = null) {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _checkpointStore = checkpointStore;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<LogReader>.Instance;

        if (_checkpointStore is null) {
            LogCheckpointStoreNotSet();
        }
        
        _ = ReadFromLogAsync(_disposedTokenSource.Token);
    }


    /// <summary>
    /// Starts the log reader and overwrites the existing log position checkpoint.
    /// </summary>
    /// <param name="options">
    ///   The log reader start options to use.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="ValueTask"/> that represents the asynchronous operation.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The <see cref="LogReader"/> has already been disposed.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    ///   The <see cref="LogReader"/> is already running.
    /// </exception>
    public async ValueTask StartAsync(LogReaderStartOptions options = default, CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var _ = await _startLock.LockAsync(cancellationToken).ConfigureAwait(false);
        
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_running.IsSet) {
            throw new InvalidOperationException("Log reader is already running.");
        }

        switch (options.StartBehaviour) {
            case LogReaderStartBehaviour.OverrideCheckpoint:
                _currentPosition = options.Position;
                if (_checkpointStore is not null) {
                    await _checkpointStore.SaveCheckpointAsync(_currentPosition, cancellationToken).ConfigureAwait(false);
                }

                _skipInitialPositionCheck = true;
                break;
            case LogReaderStartBehaviour.UseCheckpointIfAvailable:
            default:
                _currentPosition = _checkpointStore is not null
                    ? await _checkpointStore.LoadCheckpointAsync(cancellationToken).ConfigureAwait(false)
                    : default;
                
                if (_currentPosition == default && options.Position != default) {
                    // If no checkpoint is available, use the provided position
                    _currentPosition = options.Position;
                    _skipInitialPositionCheck = true;
                }
                else {
                    // If a checkpoint is available, we will skip the initial position check
                    // to avoid reprocessing the entry at the checkpoint
                    _skipInitialPositionCheck = false;
                }
                break;
        }
        
        _running.Set();
    }
    

    /// <summary>
    /// Stops the log reader, pausing the processing loop.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="ValueTask"/> that represents the asynchronous operation.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    ///   The <see cref="LogReader"/> has already been disposed.
    /// </exception>
    public async ValueTask StopAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var _ = await _startLock.LockAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        _running.Reset();
        await _stopped.WaitAsync(cancellationToken).ConfigureAwait(false);
    }


    private async Task ReadFromLogAsync(CancellationToken cancellationToken) {
        while (!cancellationToken.IsCancellationRequested) {
            if (!_running.IsSet) {
                LogWaitingForStart();
                await _running.WaitAsync(cancellationToken).ConfigureAwait(false);

                if (ProcessEntry is null) {
                    LogProcessEntryHandlerNotSet();
                }
                if (ProcessError is null) {
                    LogProcessErrorHandlerNotSet();
                }
                
                LogStartedProcessingEntries();
            }

            var initialPosition = _currentPosition;
            var skipEntryAtInitialPosition = !_skipInitialPositionCheck && (initialPosition.SequenceId.HasValue || initialPosition.Timestamp.HasValue);
            
            await foreach (var item in _log.ReadAsync(position: initialPosition, watchForChanges: true, cancellationToken: cancellationToken)) {
                // Ensure that the log entry is always disposed after processing.
                using var _ = item;
                
                try {
                    // Check for shutdown or stop before processing the entry
                    if (!CanContinueProcessing(cancellationToken)) {
                        LogStoppedProcessingEntries();
                        _stopped.Set();
                        break;
                    }
                    if (skipEntryAtInitialPosition) {
                        skipEntryAtInitialPosition = false;
                        if ((initialPosition.SequenceId.HasValue && initialPosition.SequenceId.Value == item.SequenceId) ||
                            (initialPosition.Timestamp.HasValue && initialPosition.Timestamp.Value == item.Timestamp)) {
                            // Skip the item if it matches the initial checkpoint - we don't want to
                            // reprocess it
                            continue;
                        }
                    }

                    if (_logger.IsEnabled(LogLevel.Trace)) {
                        LogProcessingEntry(item.SequenceId, item.Timestamp);
                    }
                    
                    if (ProcessEntry is not null) {
                        await ProcessEntry.Invoke(new ProcessLogReaderEntryArgs(item)).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                    // If the operation was cancelled, we just exit the loop
                    LogStoppedProcessingEntries();
                    break;
                }
                catch (Exception e) {
                    if (ProcessError is not null) {
                        try {
                            var args = new ProcessLogReaderErrorArgs(item, e);
                            await ProcessError.Invoke(args).ConfigureAwait(false);
                            if (!args.Handled) {
                                LogProcessEntryHandlerError(item.SequenceId, item.Timestamp, e);
                            }
                        }
                        catch (Exception errorHandlerException) {
                            LogProcessEntryHandlerError(item.SequenceId, item.Timestamp, e);
                            LogProcessErrorHandlerError(item.SequenceId, item.Timestamp, errorHandlerException);
                        }
                    }
                    else {
                        LogProcessEntryHandlerError(item.SequenceId, item.Timestamp, e);
                    }
                }
                finally {
                    // Only update checkpoint if not shutting down
                    if (CanContinueProcessing(cancellationToken)) {
                        LogPosition newPosition = _currentPosition.Timestamp.HasValue
                            ? item.Timestamp
                            : item.SequenceId;
                        
                        _currentPosition = newPosition;
                        if (_checkpointStore is not null) {
                            await _checkpointStore.SaveCheckpointAsync(_currentPosition, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }

                if (_running.IsSet) {
                    continue;
                }

                LogStoppedProcessingEntries();
                _stopped.Set();
                break;
            }
        }
    }
    
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanContinueProcessing(CancellationToken cancellationToken) => !_disposed && _running.IsSet && !cancellationToken.IsCancellationRequested;


    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }

        await _disposedTokenSource.CancelAsync().ConfigureAwait(false);
        _disposedTokenSource.Dispose();
        _running.Reset();

        _disposed = true;
    }


    [LoggerMessage(1, LogLevel.Warning, "Checkpoint store is not set. The log reader will not save progress.")]
    partial void LogCheckpointStoreNotSet();

    [LoggerMessage(2, LogLevel.Debug, "Log reader is waiting for start signal.")]
    partial void LogWaitingForStart();
    
    [LoggerMessage(3, LogLevel.Debug, "Log reader has started processing entries.")]
    partial void LogStartedProcessingEntries();
    
    [LoggerMessage(4, LogLevel.Debug, "Log reader has stopped processing entries.")]
    partial void LogStoppedProcessingEntries();
    
    [LoggerMessage(5, LogLevel.Warning, nameof(ProcessEntry) + " event handler is not set. The log position will advance without processing received entries.")]
    partial void LogProcessEntryHandlerNotSet();
    
    [LoggerMessage(6, LogLevel.Debug, nameof(ProcessError) + " event handler is not set. All log processing errors will be logged.")]
    partial void LogProcessErrorHandlerNotSet();
    
    [LoggerMessage(7, LogLevel.Trace, "Processing log entry: sequence ID = {sequenceId}, timestamp = {timestamp}", SkipEnabledCheck = true)]
    partial void LogProcessingEntry(ulong sequenceId, long timestamp);

    [LoggerMessage(8, LogLevel.Error, "An error occurred while processing a log entry: sequence ID = {sequenceId}, timestamp = {timestamp}")]
    partial void LogProcessEntryHandlerError(ulong sequenceId, long timestamp, Exception error);

    [LoggerMessage(9, LogLevel.Error, "The registered error handler faulted while handling a log entry processing error: sequence ID = {sequenceId}, timestamp = {timestamp}")]
    partial void LogProcessErrorHandlerError(ulong sequenceId, long timestamp, Exception error);
    
}


/// <summary>
/// Event arguments for the <see cref="LogReader.ProcessEntry"/> event.
/// </summary>
/// <param name="Entry">
///   The log entry to process.
/// </param>
public record ProcessLogReaderEntryArgs(LogEntry Entry);


/// <summary>
/// Event arguments for the <see cref="LogReader.ProcessError"/> event.
/// </summary>
/// <param name="Entry">
///   The log entry that caused the error.
/// </param>
/// <param name="Exception">
///   The exception that occurred while processing the entry.
/// </param>
public record ProcessLogReaderErrorArgs(LogEntry Entry, Exception Exception) {

    /// <summary>
    /// When set to <see langword="true"/>, indicates that the error has been handled and the log
    /// reader does not need to log the <see cref="Exception"/>.
    /// </summary>
    public bool Handled { get; set; }
    
}
