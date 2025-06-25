using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;

using Jaahas.WriteAheadLog.FileSystem.Internal;

using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.FileSystem;

/// <summary>
/// <see cref="FileWriteAheadLog"/> is a Write-Ahead Log (WAL) implementation that provides a durable, append-only
/// log for storing messages.
/// </summary>
/// <remarks>
///
/// <para>
///   Incoming messages are defined using read-only byte sequences. Use the <see cref="WriteAsync"/>
///   method or an extension method overload defined in <see cref="WriteAheadLogExtensions"/> to write
///   messages to the log. 
/// </para>
///
/// <para>
///   You can also use the <see cref="LogWriter"/> class in scenarios where it is preferable to
///   write log message payloads using an <see cref="IBufferWriter{T}"/> (for example, when
///   serializing data to JSON using <see cref="System.Text.Json.JsonSerializer"/>).
/// </para>
///
/// <para>
///   Messages written to the log are written to the current writable segment and assigned a
///   sequence ID and timestamp. You can read messages from the log using the <see cref="ReadAllAsync"/>
///   method or an extension method defined in <see cref="WriteAheadLogExtensions"/>.
/// </para>
///
/// <para>
///   The log supports automatic segment rollover based on segment size, message count, and/or
///   time. Old segments can be automatically cleaned up after a retention period or when the
///   number of segments exceeds a maximum count.
/// </para>
/// 
/// </remarks>
public sealed partial class FileWriteAheadLog : IWriteAheadLog {
    
    /// <summary>
    /// Specifies whether the log has been disposed.
    /// </summary>
    private bool _disposed;
    
    /// <summary>
    /// The logger for the log instance.
    /// </summary>
    private readonly ILogger<FileWriteAheadLog> _logger;
    
    /// <summary>
    /// The logger factory used to create loggers for internal components.
    /// </summary>
    private readonly ILoggerFactory _loggerFactory;
    
    /// <summary>
    /// A cancellation token source that is used to signal when the log has been disposed.
    /// </summary>
    private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();

    /// <summary>
    /// Specifies whether the log has been initialised.
    /// </summary>
    private bool _initialised;
    
    /// <summary>
    /// Lock to ensure that only one initialization operation can occur at a time.
    /// </summary>
    private readonly Nito.AsyncEx.AsyncAutoResetEvent _initLock = new Nito.AsyncEx.AsyncAutoResetEvent(true);
    
    /// <summary>
    /// The time provider used to obtain the timestamp for log entries.
    /// </summary>
    private readonly TimeProvider _timeProvider;
    
    /// <summary>
    /// The options for configuring the log's behavior, such as data directory, segment size, and
    /// flush interval.
    /// </summary>
    private readonly FileWriteAheadLogOptions _options;
    
    /// <summary>
    /// The directory where the log data is stored.
    /// </summary>
    private readonly DirectoryInfo _dataDirectory;
    
    /// <summary>
    /// The last sequence ID written to the log.
    /// </summary>
    private ulong _lastSequenceId;
    
    /// <summary>
    /// The timestamp of the last message written to the log.
    /// </summary>
    private long _lastTimestamp;
    
    /// <summary>
    /// The writer for the current (writable) log segment.
    /// </summary>
    private FileSegmentWriter? _writer;

    /// <summary>
    /// The sparse index for the current writable log segment.
    /// </summary>
    private MutableSegmentIndex? _writerSegmentIndex;
    
    /// <summary>
    /// The sparse indices for read-only log segments.
    /// </summary>
    private readonly List<SegmentIndexWrapper> _readOnlySegmentIndices = new List<SegmentIndexWrapper>();
    
    /// <summary>
    /// Lock for synchronizing access to the segment indices.
    /// </summary>
    private readonly Nito.AsyncEx.AsyncReaderWriterLock _segmentIndicesLock = new Nito.AsyncEx.AsyncReaderWriterLock();

    /// <summary>
    /// Write lock to ensure that only one write operation can occur at a time.
    /// </summary>
    private readonly Nito.AsyncEx.AsyncLock _writeLock = new Nito.AsyncEx.AsyncLock();
    
    /// <summary>
    /// In-progress read operations that are currently reading from the log.
    /// </summary>
    /// <remarks>
    ///   Read operations are tracked to allow the log to notify them when new segments are
    ///   available for reading.
    /// </remarks>
    private readonly ConcurrentDictionary<Guid, ReadOperationStatus> _readOperations = new ConcurrentDictionary<Guid, ReadOperationStatus>();
    

    /// <summary>
    /// Creates a new <see cref="FileWriteAheadLog"/> instance.
    /// </summary>
    /// <param name="options">
    ///   The log options.
    /// </param>
    /// <param name="timeProvider">
    ///   The time provider used to obtain timestamps for log entries. If not provided,
    ///   <see cref="TimeProvider.System"/> will be used.
    /// </param>
    /// <param name="logger">
    ///   The logger for the <see cref="FileWriteAheadLog"/> instance.
    /// </param>
    /// <param name="loggerFactory">
    ///   The logger factory used to create loggers for internal components.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="options"/> is <see langword="null"/>.
    /// </exception>
    public FileWriteAheadLog(FileWriteAheadLogOptions options, TimeProvider? timeProvider = null, ILogger<FileWriteAheadLog>? logger = null, ILoggerFactory? loggerFactory = null) {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _loggerFactory = loggerFactory ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
        _logger = logger ?? _loggerFactory.CreateLogger<FileWriteAheadLog>();
        
        var dataDir = string.IsNullOrWhiteSpace(options.DataDirectory) 
            ? "wal" 
            : options.DataDirectory;
        _dataDirectory = new DirectoryInfo(Path.IsPathRooted(dataDir) ? dataDir : Path.Combine(AppContext.BaseDirectory, dataDir));
    }
    
    
    /// <inheritdoc/>
    public async ValueTask InitAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        await InitCoreAsync(cts.Token).ConfigureAwait(false);
    }
    
    
    /// <summary>
    /// Gets the current segments in the log.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A read-only list of <see cref="SegmentHeader"/> objects representing the segments in the
    ///   log.
    /// </returns>
    /// <remarks>
    ///   Calling this method will initialize the log if it has not already been initialized.
    /// </remarks>
    public async ValueTask<IReadOnlyList<SegmentHeader>> GetSegmentsAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        await InitCoreAsync(cts.Token).ConfigureAwait(false);
        
        using var handle = await _segmentIndicesLock.ReaderLockAsync(cts.Token).ConfigureAwait(false);
        
        var builder = ImmutableArray.CreateBuilder<SegmentHeader>();

        builder.AddRange(_readOnlySegmentIndices.Select(x => SegmentHeader.CopyFrom(x.Index.Header)));
        if (_writerSegmentIndex is not null) {
            builder.Add(SegmentHeader.CopyFrom(_writerSegmentIndex.Header));
        }
        
        return builder.ToImmutable();
    } 
    
    
    /// <inheritdoc/>
    public async ValueTask<WriteResult> WriteAsync(ReadOnlySequence<byte> data, CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        await InitCoreAsync(cts.Token).ConfigureAwait(false);
        
        using var handle = await _writeLock.LockAsync(cts.Token).ConfigureAwait(false);
        
        // Check if we need to roll over to a new segment.
        var rolloverCheck = IsRolloverRequired(data.Length);
        if (rolloverCheck.Rollover) {
            await RolloverCoreAsync(rolloverCheck.Reason, cts.Token).ConfigureAwait(false);
        }

        var timestamp = _timeProvider.GetTimestamp();
        var bytesWritten = await _writer!.WriteAsync(data, ++_lastSequenceId, timestamp, cts.Token).ConfigureAwait(false);
        _lastTimestamp = timestamp;

        if (_options.SparseIndexInterval > 0 && _writer.Header.MessageCount % _options.SparseIndexInterval == 0) {
            // Add an index entry for this message.
            var entry = new MessageIndexEntry(_lastSequenceId, _lastTimestamp, _writer.Header.Size - bytesWritten);
            _writerSegmentIndex!.AddEntry(entry);
        }
        
        return new WriteResult(_lastSequenceId, _lastTimestamp);
    }
    

    /// <summary>
    /// Flushes the current log segment to disk.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <exception cref="ObjectDisposedException">
    ///   The log has been disposed.
    /// </exception>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_initialised) {
            return;
        }
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        await InitCoreAsync(cts.Token).ConfigureAwait(false);
        
        using var handle = await _writeLock.LockAsync(cts.Token).ConfigureAwait(false);

        if (_writer is null) {
            return;
        }
        
        await _writer.FlushAsync(cts.Token).ConfigureAwait(false);
    }
    
    
    /// <summary>
    /// Forces a rollover of the current log segment.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <exception cref="ObjectDisposedException">
    ///   The log has been disposed.
    /// </exception>
    public async ValueTask RolloverAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        await InitCoreAsync(cts.Token).ConfigureAwait(false);
        
        using var handle = await _writeLock.LockAsync(cts.Token).ConfigureAwait(false);
        await RolloverCoreAsync(RolloverReason.Manual, cts.Token).ConfigureAwait(false);
    }


    /// <summary>
    /// Cleans up the log by removing old segments that are no longer needed based on the log's
    /// maximum segment count and retention period.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <remarks>
    ///
    /// <para>
    ///   Cleanup is also performed periodically in the background if the <see cref="FileWriteAheadLogOptions.SegmentCleanupInterval"/>
    ///   setting is greater than <see cref="TimeSpan.Zero"/>.
    /// </para>
    ///
    /// <para>
    ///   The current writable segment is never deleted as part of the cleanup process, even if it
    ///   meets the cleanup criteria. Segment files that are in use by ongoing read operations will
    ///   be deleted once the read operations have finished reading the file.
    /// </para>
    /// 
    /// </remarks>
    public async ValueTask CleanupAsync(CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        
        using var handle = await _writeLock.LockAsync(cts.Token).ConfigureAwait(false);
        LogCleanupRequested("manual");
        await CleanupCoreAsync(cts.Token).ConfigureAwait(false);
    }
    
    
    /// <inheritdoc/>
    public async IAsyncEnumerable<LogEntry> ReadAllAsync(LogReadOptions options, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        var startingTimestamp = options.Position.Timestamp ?? -1;
        var startingSequenceId = options.Position.SequenceId ?? 0;
        
        if (startingTimestamp > 0 && startingTimestamp > _lastTimestamp && !options.WatchForChanges) {
            // After the last timestamp, there are no messages to read.
            yield break;
        }
        
        if (startingSequenceId > 0 && startingSequenceId > _lastSequenceId && !options.WatchForChanges) {
            // After the last sequence ID, there are no messages to read.
            yield break;
        }

        ReadOperationStatus status;
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposedTokenSource.Token);
        await InitCoreAsync(cts.Token).ConfigureAwait(false);

        using (await _segmentIndicesLock.ReaderLockAsync(cts.Token).ConfigureAwait(false)) {
            var segmentsToRead = new ConcurrentQueue<SegmentIndexWrapper>();
            
            foreach (var item in _readOnlySegmentIndices) {
                if (startingTimestamp > 0) {
                    if (item.Index.Header.LastTimestamp < startingTimestamp) {
                        // Skip segments that are before the requested timestamp.
                        continue;
                    }
                }
                else if (startingSequenceId > 0) {
                    if (item.Index.Header.LastSequenceId < startingSequenceId) {
                        // Skip segments that are before the requested sequence ID.
                        continue;
                    }
                }
                
                segmentsToRead.Enqueue(item);
            }

            if (_writerSegmentIndex is not null) {
                // Add the writable segment if we are watching for changes, or if the starting
                // timestamp or sequence ID fall before the end of the writable segment.

                if (options.WatchForChanges ||
                    startingTimestamp <= 0 ||
                    startingSequenceId == 0 ||
                    _writerSegmentIndex.Header.LastTimestamp >= startingTimestamp ||
                    _writerSegmentIndex.Header.LastSequenceId >= startingSequenceId) {
                    segmentsToRead.Enqueue(new SegmentIndexWrapper(_writer!.FilePath, _writerSegmentIndex));
                }
            }
            
            if (segmentsToRead.IsEmpty && !options.WatchForChanges) {
                // No segments to read, so we can exit early.
                yield break;
            }
            
            status = new ReadOperationStatus(segmentsToRead, options.WatchForChanges);
            _readOperations[status.Id] = status;
            
            LogRead(status.Id, startingSequenceId, startingTimestamp, options.Limit, options.WatchForChanges);
        }

        try {
            var isFirstSegment = true;

            while (!cts.IsCancellationRequested) {
                if (!options.WatchForChanges && status.Segments.IsEmpty) {
                    // If we are not watching for changes and there are no segments to read, we can exit early.
                    yield break;
                }
                
                await status.SegmentsAvailable.WaitAsync(cts.Token).ConfigureAwait(false);

                while (status.Segments.TryDequeue(out var index)) {
                    var offset = 0L;

                    if (isFirstSegment) {
                        // If this is the first matching segment, and we are seeking a specific sequence
                        // ID, we can use the index entries to skip
                        // ahead to the first message we are interested in.

                        isFirstSegment = false;

                        if (startingTimestamp > 0) {
                            for (var i = 0; i < index.Index.Entries.Count; i++) {
                                var entry = index.Index.Entries[i];
                                if (entry.Timestamp < startingTimestamp) {
                                    // Skip entries that are before the requested timestamp.
                                    continue; 
                                }

                                if (entry.Timestamp == startingTimestamp) {
                                    // We found the first message we are interested in, so we can start reading from here.
                                    offset = entry.Offset;
                                    break;
                                }

                                // We've found an entry that is after the requested timestamp; move back to the previous entry if possible.
                                if (i > 0) {
                                    offset = index.Index.Entries[i - 1].Offset;
                                }

                                break;
                            }
                        }
                        else if (startingSequenceId > 0) {
                            for (var i = 0; i < index.Index.Entries.Count; i++) {
                                var entry = index.Index.Entries[i];
                                if (entry.SequenceId < startingSequenceId) {
                                    continue; // Skip entries that are before the requested sequence ID.
                                }

                                if (entry.SequenceId == startingSequenceId) {
                                    // We found the first message we are interested in, so we can start reading from here.
                                    offset = entry.Offset;
                                    break;
                                }

                                // We've found an entry that is after the requested sequence ID; move back to the previous entry if possible.
                                if (i > 0) {
                                    offset = index.Index.Entries[i - 1].Offset;
                                }

                                break;
                            }
                        }
                    }

                    var watch = status.WatchForChanges && index.Index is MutableSegmentIndex;
                    long count = 0;
                    await foreach (var item in new FileSegmentReader(_loggerFactory.CreateLogger<FileSegmentReader>()).ReadLogEntriesAsync(index.FilePath, offset, SeekOrigin.Begin, watch, _options.ReadPollingInterval, cts.Token).ConfigureAwait(false)) {
                        if (startingTimestamp > 0) {
                            if (item.Timestamp < startingTimestamp) {
                                // Skip entries that are before the requested timestamp.
                                item.Dispose();
                                continue;
                            }
                        }
                        else if (startingSequenceId > 0) {
                            if (item.SequenceId < startingSequenceId) {
                                // Skip entries that are before the requested sequence ID.
                                item.Dispose();
                                continue;
                            }
                        }
                        
                        yield return item;
                        
                        if (options.Limit > 0 && ++count >= options.Limit) {
                            // If we have read enough messages, we can stop reading.
                            yield break;
                        }
                    }
                }
            }
        }
        finally {
            _readOperations.TryRemove(status.Id, out _);
        }
    }
    
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private FileInfo[] GetSegmentFiles() => !_dataDirectory.Exists 
        ? [] 
        : _dataDirectory.GetFiles("*.wal", SearchOption.TopDirectoryOnly);
    
    
    /// <summary>
    /// Initialises the log if it has not already been initialised.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    private async ValueTask InitCoreAsync(CancellationToken cancellationToken) {
        if (_initialised) {
            return;
        }
        
        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try {
            if (_initialised) {
                return;
            }
            
            using var handle = await _writeLock.LockAsync(cancellationToken).ConfigureAwait(false);
            using var segmentIndicesLockHandle = await _segmentIndicesLock.WriterLockAsync(cancellationToken).ConfigureAwait(false);
            
            LogInitialising(_dataDirectory.FullName);
            _dataDirectory.Create();

            var files = GetSegmentFiles();
            if (files.Length == 0) {
                _lastSequenceId = 0;
                MarkAsInitialised();
                return;
            }

            // Move from newest to oldest segment files.
            for (var i = files.Length - 1; i >= 0; i--) {
                var file = files[i];

                if (!TryGetSegmentDetailsFromFileName(file, out var segmentName, out var originTime)) {
                    // The file does not match the expected naming convention.
                    continue;
                }

                LogCheckingSegment(file.FullName);
                var header = FileSegmentReader.ReadHeader(file.FullName);

                if (header is null) {
                    LogInvalidSegmentHeader(file.FullName);
                    continue; // Skip invalid segment files
                }

                if (header.LastSequenceId > _lastSequenceId) {
                    _lastSequenceId = header.LastSequenceId;
                }

                if (header.LastTimestamp > _lastTimestamp) {
                    _lastTimestamp = header.LastTimestamp;
                }

                if (!header.ReadOnly) {
                    // This is our writable segment.
                    _writer ??= CreateFileSegmentWriter(
                        segmentName,
                        file.FullName,
                        GetSegmentExpiryTime(originTime));
                    _writerSegmentIndex = (MutableSegmentIndex) await BuildSegmentIndexAsync(header, file.FullName, cancellationToken).ConfigureAwait(false);
                    LogSegmentLoaded(file.FullName, false, header.MessageCount, header.FirstSequenceId, header.LastSequenceId, header.FirstTimestamp, header.LastTimestamp);
                    continue;
                }

                _readOnlySegmentIndices.Add(new SegmentIndexWrapper(file.FullName, await BuildSegmentIndexAsync(header, file.FullName, cancellationToken).ConfigureAwait(false)));
                LogSegmentLoaded(file.FullName, true, header.MessageCount, header.FirstSequenceId, header.LastSequenceId, header.FirstTimestamp, header.LastTimestamp);
            }

            MarkAsInitialised();
        }
        finally {
            _initLock.Set();
        }

        return;
        
        void MarkAsInitialised() {
            _initialised = true;
            if (_options.SegmentCleanupInterval > TimeSpan.Zero) {
                _ = RunCleanupLoopAsync(_options.SegmentCleanupInterval, _disposedTokenSource.Token);
            }
        }
    }


    /// <summary>
    /// Creates a name for a new log segment based on the specified timestamp.
    /// </summary>
    /// <param name="timestamp">
    ///   The timestamp to use for the segment name. This will be truncated to the nearest
    ///   second.
    /// </param>
    /// <returns>
    ///   A name for the new segment, formatted as <c>yyyyMMddHHmmss-xxxxxxxxxxxxxxxxxxxx</c>.
    /// </returns>
    /// <remarks>
    ///   The generated segment name uses the format <c>yyyyMMddHHmmss-xxxxxxxxxxxxxxxxxxxx</c>,
    ///   where <c>xxxxxxxxxxxxxxxxxxxx</c> is a v7 UUID based on the timestamp.
    /// </remarks>
    private string CreateSegmentName(DateTimeOffset timestamp) {
        return $"{timestamp:yyyyMMddHHmmss}-{Guid.CreateVersion7(timestamp):N}";
    }
    
    
    /// <summary>
    /// Gets the file name for a segment based on the segment name.
    /// </summary>
    /// <param name="segmentName">
    ///   The name of the segment.
    /// </param>
    /// <returns>
    ///   A file name for the segment, formatted as <c>{NAME}.wal</c>.
    /// </returns>
    private string GetSegmentFileName(string segmentName) 
        => Path.Combine(_dataDirectory.FullName, segmentName + ".wal");

    
    
    private static bool TryGetSegmentDetailsFromFileName(FileInfo file, [NotNullWhen(true)] out string? segmentName, out DateTimeOffset originTime) {
        segmentName = null;
        originTime = default;
        
        // Extract segment name and origin time from the file name, which is expected to be in the format "yyyyMMddHHmmss-xxxxxxxxxxxxxxxxxxxx.wal"
        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name);
        var parts = fileNameWithoutExtension.Split('-');
        if (parts.Length < 2 || !DateTimeOffset.TryParseExact(parts[0], "yyyyMMddHHmmss", null, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out originTime)) {
            return false;
        }
        
        segmentName = fileNameWithoutExtension;
        return true;
    }
    
    
    /// <summary>
    /// Gets the expiry time for a segment based on the origin time and the maximum segment time span.
    /// </summary>
    /// <param name="originTime">
    ///   The origin time of the segment, which is derived from the segment file name.
    /// </param>
    /// <returns>
    ///   A <see cref="DateTimeOffset"/> representing the expiry time of the segment, or
    ///   <see langword="null"/> if no expiry time is set.
    /// </returns>
    /// <remarks>
    ///   If the maximum segment time span is set to zero or less, the segment will not expire. If
    ///   the maximum segment time span is less than one second, an expiry time of one second will
    /// be used.
    /// </remarks>
    private DateTimeOffset? GetSegmentExpiryTime(DateTimeOffset originTime) {
        if (_options.MaxSegmentTimeSpan <= TimeSpan.Zero) {
            return null; // No expiry time set.
        }

        // Calculate the expiry time based on the origin time and the maximum segment time span.
        return originTime.Add(_options.MaxSegmentTimeSpan >= TimeSpan.FromSeconds(1)
            ? _options.MaxSegmentTimeSpan
            : TimeSpan.FromSeconds(1));
    }
    
    
    private FileSegmentWriter CreateFileSegmentWriter(string name, string filePath, DateTimeOffset? expiryTime)
        => new FileSegmentWriter(
            new FileSegmentWriterOptions(
                name,
                filePath,
                NotAfter: expiryTime, 
                FlushInterval: _options.FlushInterval, 
                FlushBatchSize: _options.FlushBatchSize), 
            _timeProvider,
            _loggerFactory.CreateLogger<FileSegmentWriter>());
    

    /// <summary>
    /// Builds a sparse index for a segment file.
    /// </summary>
    /// <param name="header">
    ///   The header of the segment file, which contains metadata such as size, message count, and
    ///   sequence ID and timestamp ranges.
    /// </param>
    /// <param name="filePath">
    ///   The path to the segment file.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="SegmentIndex"/> representing the sparse index for the segment file.
    /// </returns>
    private async ValueTask<SegmentIndex> BuildSegmentIndexAsync(SegmentHeader header, string filePath, CancellationToken cancellationToken) {
        var readOnly = header.ReadOnly;
        LogBuildingSegmentIndex(filePath, readOnly);
        
        var itemCount = 0;
        var offset = 0L;
        
        var entries = new List<MessageIndexEntry>();
        
        await foreach (var item in new FileSegmentReader(_loggerFactory.CreateLogger<FileSegmentReader>()).ReadLogEntriesAsync(filePath, cancellationToken).ConfigureAwait(false)) {
            try {
                ++itemCount;
                if (_options.SparseIndexInterval > 0 && itemCount % _options.SparseIndexInterval == 0) {
                    var indexEntry = new MessageIndexEntry(item.SequenceId, item.Timestamp, offset);
                    entries.Add(indexEntry);
                }
                
                offset += FileWriteAheadLogUtilities.GetSerializedLogEntrySize(item.Data.Count);
            }
            finally {
                item.Dispose();
            }
        }

        return readOnly
            ? new ImmutableSegmentIndex(header, entries)
            : new MutableSegmentIndex(header, entries);
    }
    
    
    /// <summary>
    /// Tests whether a rollover is required based on the current segment's size, time, and
    /// message count.
    /// </summary>
    /// <param name="incomingMessageSize">
    ///   The size of the incoming message that is about to be written to the log.
    /// </param>
    /// <returns>
    ///   A tuple indicating whether a rollover is required and, if so, the reason for the rollover.
    /// </returns>
    private (bool Rollover, RolloverReason Reason) IsRolloverRequired(long incomingMessageSize) {
        if (_writer is null) {
            // No writer exists, so we need to create a new segment.
            return (true, RolloverReason.NoWritableSegments); 
        }
        
        if (_options.MaxSegmentSizeBytes > 0 && _writer.Header.Size + incomingMessageSize >= _options.MaxSegmentSizeBytes) {
            // The current segment has reached the maximum size.
            return (true, RolloverReason.SegmentSizeLimitReached);
        }
        
        if (_writer.NotAfter.HasValue && _timeProvider.GetUtcNow() >= _writer.NotAfter.Value) {
            // The current segment has expired.
            return (true, RolloverReason.SegmentTimeLimitReached);
        }
        
        if (_options.MaxSegmentMessageCount > 0 && _writer.Header.MessageCount >= _options.MaxSegmentMessageCount) {
            // The current segment has reached the maximum message count.
            return (true, RolloverReason.SegmentMessageCountLimitReached);
        }

        return (false, default);
    }
    

    /// <summary>
    /// Rolls over to a new segment writer, closing the current segment and creating a new one.
    /// </summary>
    /// <param name="reason">
    ///   The reason for the rollover.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    private async ValueTask RolloverCoreAsync(RolloverReason reason, CancellationToken cancellationToken) {
        using var handle = await _segmentIndicesLock.WriterLockAsync(cancellationToken).ConfigureAwait(false);
        
        // Capture the current writer and segment index before creating a new writer.
        var previousWriter = _writer;
        var previousWriterSegmentIndex = _writerSegmentIndex;
        
        // Now create a new segment writer.
        var now = _timeProvider.GetUtcNow();
        var expiryDate = GetSegmentExpiryTime(new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Offset));

        var segmentName = CreateSegmentName(now);
        var filePath = GetSegmentFileName(segmentName);
        _writer = CreateFileSegmentWriter(segmentName, filePath, expiryDate);
        _writerSegmentIndex = new MutableSegmentIndex(_writer.Header);
        
        LogRolloverCompleted(reason, filePath);
        
        foreach (var readOperation in _readOperations.Values) {
            // Notify all in-flight read operations that a new segment is available.
            readOperation.Enqueue(new SegmentIndexWrapper(_writer.FilePath, _writerSegmentIndex));
        }
        
        // Now close the previous segment writer.
        if (previousWriter is not null) {
            await previousWriter.CloseSegmentAsync(cancellationToken).ConfigureAwait(false);
            if (previousWriterSegmentIndex is not null) {
                _readOnlySegmentIndices.Add(new SegmentIndexWrapper(previousWriter.FilePath, previousWriterSegmentIndex.ToImmutable()));
            }

            await previousWriter.DisposeAsync().ConfigureAwait(false);
        }
    }
    
    
    private async Task RunCleanupLoopAsync(TimeSpan interval, CancellationToken cancellationToken) {
        LogCleanupLoopStarted(interval);
        
        while (!cancellationToken.IsCancellationRequested) {
            try {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
                using var handle = await _writeLock.LockAsync(cancellationToken).ConfigureAwait(false);
                LogCleanupRequested("scheduled");
                await CleanupCoreAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
            catch (Exception e) {
                LogSegmentCleanupLoopError(e);
            }
        }

        LogCleanupLoopStopped();
    }


    private async ValueTask CleanupCoreAsync(CancellationToken cancellationToken) {
        if (_disposed) {
            return;
        }
        
        using var handle = await _segmentIndicesLock.WriterLockAsync(cancellationToken).ConfigureAwait(false);
        
        var segmentFiles = GetSegmentFiles()
            .Select(x => TryGetSegmentDetailsFromFileName(x, out var name, out var originTime) 
                ? new { File = x, Name = name, OriginTime = originTime } 
                : null)
            .Where(x => x is not null)
            // Filter out the current writable segment file.
            .Where(x => x!.File.FullName != _writer?.FilePath)
            .ToArray();
        
        var index = 0;
        
        if (_options.SegmentRetentionLimit > 0 && segmentFiles.Length > _options.SegmentRetentionLimit) {
            var filesToRemove = segmentFiles.Length - _options.SegmentRetentionLimit;
            for (; index < filesToRemove; index++) {
                var file = segmentFiles[index];
                
                try {
                    LogDeletingSegmentFileCountExceeded(file!.File.FullName, segmentFiles.Length, _options.SegmentRetentionLimit);
                    file.File.Delete();
                    
                    var indexEntry = _readOnlySegmentIndices.FirstOrDefault(x => x.FilePath == file.File.FullName);
                    if (indexEntry is not null) {
                        _readOnlySegmentIndices.Remove(indexEntry);
                    }
                }
                catch (Exception e) {
                    LogSegmentFileDeleteFailed(file!.File.FullName, e);
                }
            }
        }

        if (_options.SegmentRetentionPeriod > TimeSpan.Zero) {
            for (; index < segmentFiles.Length; index++) {
                var file = segmentFiles[index];

                var age = _timeProvider.GetUtcNow() - file!.OriginTime;
                if (age <= _options.SegmentRetentionPeriod) {
                    continue;
                }

                try {
                    LogDeletingSegmentFileRetentionPeriodExceeded(file.File.FullName, age, _options.SegmentRetentionPeriod);
                    file.File.Delete();
                    
                    var indexEntry = _readOnlySegmentIndices.FirstOrDefault(x => x.FilePath == file.File.FullName);
                    if (indexEntry is not null) {
                        _readOnlySegmentIndices.Remove(indexEntry);
                    }
                }
                catch (Exception e) {
                    LogSegmentFileDeleteFailed(file.File.FullName, e);
                }
            }
        }
    }
    
    
    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }

        using var handle = await _writeLock.LockAsync().ConfigureAwait(false);

        await _disposedTokenSource.CancelAsync().ConfigureAwait(false);

        if (_writer is not null) {
            await _writer.DisposeAsync().ConfigureAwait(false);
            _writer = null;
        }

        _disposed = true;
    }


    [LoggerMessage(1, LogLevel.Debug, "Initialising log using data directory '{dataDirectory}'.")]
    partial void LogInitialising(string dataDirectory);
    
    [LoggerMessage(2, LogLevel.Debug, "Checking segment file '{filePath}'.")]
    partial void LogCheckingSegment(string filePath);
    
    [LoggerMessage(3, LogLevel.Warning, "Segment file '{filePath}' has an invalid header and will be skipped.")]
    partial void LogInvalidSegmentHeader(string filePath);
    
    [LoggerMessage(4, LogLevel.Trace, "Building segment index for file '{filePath}': read-only = {readOnly}")]
    partial void LogBuildingSegmentIndex(string filePath, bool readOnly);
    
    [LoggerMessage(5, LogLevel.Trace, "Loaded segment file '{filePath}': read-only = {readOnly}, message count = {messageCount}, sequence ID range: {startSequenceId} - {endSequenceId}, timestamp range: {startTimestamp} - {endTimestamp}")]
    partial void LogSegmentLoaded(string filePath, bool readOnly, long messageCount, ulong startSequenceId, ulong endSequenceId, long startTimestamp, long endTimestamp);
    
    [LoggerMessage(6, LogLevel.Information, "Rollover required: reason = {reason}, new segment file path = '{filePath}'")]
    partial void LogRolloverCompleted(RolloverReason reason, string filePath);
    
    [LoggerMessage(7, LogLevel.Trace, "Reading from position: operation ID = {operationId}, sequence ID = {sequenceId}, timestamp = {timestamp}, count = {count}, watch for changes = {watchForChanges}")]
    partial void LogRead(Guid operationId, ulong sequenceId, long timestamp, long count, bool watchForChanges);

    [LoggerMessage(8, LogLevel.Debug, "Starting log cleanup loop: interval = {interval}")]
    partial void LogCleanupLoopStarted(TimeSpan interval);
    
    [LoggerMessage(9, LogLevel.Debug, "Stopping log cleanup loop.")]
    partial void LogCleanupLoopStopped();
    
    [LoggerMessage(10, LogLevel.Debug, "Requesting log cleanup: trigger type = {trigger}")]
    partial void LogCleanupRequested(string trigger);
    
    [LoggerMessage(11, LogLevel.Debug, "Deleting segment file '{filePath}': reason = MaxSegmentCountExceeded, count = {count}, limit = {limit}")]
    partial void LogDeletingSegmentFileCountExceeded(string filePath, int count, int limit);
    
    [LoggerMessage(12, LogLevel.Debug, "Deleting segment file '{filePath}': reason = SegmentRetentionPeriodExceeded, age = {age}, retention period = {retentionPeriod}")]
    partial void LogDeletingSegmentFileRetentionPeriodExceeded(string filePath, TimeSpan age, TimeSpan retentionPeriod);
    
    [LoggerMessage(13, LogLevel.Error, "Failed to delete segment file '{filePath}'.")]
    partial void LogSegmentFileDeleteFailed(string filePath, Exception error);
    
    [LoggerMessage(14, LogLevel.Error, "An error occurred in the segment cleanup loop.")]
    partial void LogSegmentCleanupLoopError(Exception error);
    
    
    /// <summary>
    /// A wrapper for a segment index that includes the file path for the segment.
    /// </summary>
    /// <param name="FilePath">
    ///   The file path of the segment file.
    /// </param>
    /// <param name="Index">
    ///   The index for the segment.
    /// </param>
    private record SegmentIndexWrapper(string FilePath, SegmentIndex Index);


    /// <summary>
    /// Describes an in-progress read operation, including the segments being read and a mechanism
    /// to signal when new segments are available.
    /// </summary>
    /// <param name="Segments">
    ///   The initial segments to read from.
    /// </param>
    /// <param name="WatchForChanges">
    ///   When <see langword="true"/>, the operation will wait for new log entries to be added to
    ///   the latest segment when it reaches the end of the log stream. Otherwise, the operation
    ///   will complete when is reaches the end of the latest segment.
    /// </param>
    private record ReadOperationStatus(ConcurrentQueue<SegmentIndexWrapper> Segments, bool WatchForChanges) {
        
        /// <summary>
        /// The unique identifier for this read operation.
        /// </summary>
        public Guid Id { get; } = Guid.NewGuid();

        /// <summary>
        /// Notifies when new segments are available for reading.
        /// </summary>
        public Nito.AsyncEx.AsyncAutoResetEvent SegmentsAvailable { get; } = new Nito.AsyncEx.AsyncAutoResetEvent(!Segments.IsEmpty);
        
        
        /// <summary>
        /// Enqueues a new segment index to be processed by the operation.
        /// </summary>
        /// <param name="segment">
        ///   The segment to enqueue.
        /// </param>
        public void Enqueue(SegmentIndexWrapper segment) {
            Segments.Enqueue(segment);
            SegmentsAvailable.Set();
        }
        
    }

}
