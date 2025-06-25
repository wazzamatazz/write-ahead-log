using System.IO.Pipelines;
using System.Runtime.CompilerServices;

using Microsoft.Extensions.Logging;

using Nito.AsyncEx;

namespace Jaahas.WriteAheadLog.FileSystem.Internal;

/// <summary>
/// A reader for WAL segments stored in file system files.
/// </summary>
internal sealed partial class FileSegmentReader : SegmentReader {

    private readonly ILogger<FileSegmentReader> _logger;
    
    
    /// <summary>
    /// Creates a new <see cref="FileSegmentReader"/> instance.
    /// </summary>
    /// <param name="logger">
    ///   The logger for the reader.
    /// </param>
    public FileSegmentReader(ILogger<FileSegmentReader>? logger = null) {
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<FileSegmentReader>.Instance;
    }
    
    
    /// <summary>
    /// Reads the segment header from the specified file.
    /// </summary>
    /// <param name="filePath">
    ///   The segment file.
    /// </param>
    /// <returns>
    ///   A <see cref="SegmentHeader"/> instance containing the deserialized header information.
    /// </returns>
    /// <exception cref="ArgumentException">
    ///   <paramref name="filePath"/> is <see langword="null"/> or white space.
    /// </exception>
    /// <exception cref="InvalidDataException">
    ///   A valid segment header could not be read from the stream.
    /// </exception>
    public static SegmentHeader ReadHeader(string filePath) {
        using var stream = OpenFile(filePath);
        return ReadHeader(stream);
    }
    
    
    /// <summary>
    /// Opens a file for reading log entries.
    /// </summary>
    /// <param name="filePath">
    ///   The path to the file to open.
    /// </param>
    /// <returns>
    ///   A <see cref="Stream"/> for reading the file.
    /// </returns>
    internal static Stream OpenFile(string filePath) {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        
        return File.Open(filePath, new FileStreamOptions() {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
            Options = FileOptions.SequentialScan,
            BufferSize = 64 * 1024 // 64 KB buffer size
        });
    }
    
    
    /// <summary>
    /// Asynchronously reads log entries from the start of the specified file.
    /// </summary>
    /// <param name="filePath">
    ///   The path to the file to read log entries from.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   An asynchronous sequence of <see cref="LogEntry"/> instances read from the file.
    /// </returns>
    /// <remarks>
    ///
    /// <para>
    ///   Reading continues until one of the following conditions is met:
    /// </para>
    ///
    /// <list type="bullet">
    ///   <item>Cancellation is requested via the <paramref name="cancellationToken"/>.</item>
    ///   <item>The end of the file is reached.</item>
    /// </list>
    /// 
    /// </remarks>
    public IAsyncEnumerable<LogEntry> ReadLogEntriesAsync(string filePath, CancellationToken cancellationToken = default) 
        => ReadLogEntriesAsync(filePath, 0, SeekOrigin.Begin, cancellationToken);
    
    
    /// <summary>
    /// Asynchronously reads log entries from the specified file, starting from the given offset and origin.
    /// </summary>
    /// <param name="filePath">
    ///   The path to the file to read log entries from.
    /// </param>
    /// <param name="offset">
    ///   The offset in the file from which to start reading log entries.
    /// </param>
    /// <param name="origin">
    ///   The origin from which the offset is calculated.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   An asynchronous sequence of <see cref="LogEntry"/> instances read from the file.
    /// </returns>
    /// <remarks>
    ///
    /// <para>
    ///   Reading continues until one of the following conditions is met:
    /// </para>
    ///
    /// <list type="bullet">
    ///   <item>Cancellation is requested via the <paramref name="cancellationToken"/>.</item>
    ///   <item>The end of the file is reached.</item>
    /// </list>
    /// 
    /// </remarks>
    public async IAsyncEnumerable<LogEntry> ReadLogEntriesAsync(string filePath, long offset, SeekOrigin origin, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        LogReadingEntries(filePath, offset, origin, false);
        
        await using var stream = OpenFile(filePath);
        
        if (offset > 0) {
            stream.Seek(offset, origin);
        }
        
        var pipeReader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
        
        await foreach (var item in ReadLogEntriesAsync(pipeReader, cancellationToken).ConfigureAwait(false)) {
            yield return item;
        }
    }
    
    
    /// <summary>
    /// Asynchronously reads log entries from the specified file, starting from the given offset and origin.
    /// </summary>
    /// <param name="filePath">
    ///   The path to the file to read log entries from.
    /// </param>
    /// <param name="offset">
    ///   The offset in the file from which to start reading log entries.
    /// </param>
    /// <param name="origin">
    ///   The origin from which the offset is calculated.
    /// </param>
    /// <param name="watchForChanges">
    ///   If <see langword="true"/>, the reader will watch for changes in the file and continue
    ///   reading new entries as they are added. Changes are detected using a polling mechanism
    ///   that examines the length of the file at regular intervals.
    /// </param>
    /// <param name="pollingInterval">
    ///   The interval at which to poll the file for changes when <paramref name="watchForChanges"/>
    ///   is <see langword="true"/>. The default polling interval is 500 milliseconds.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   An asynchronous sequence of <see cref="LogEntry"/> instances read from the file.
    /// </returns>
    /// <remarks>
    ///
    /// <para>
    ///   Reading continues until one of the following conditions is met:
    /// </para>
    ///
    /// <list type="bullet">
    ///   <item>Cancellation is requested via the <paramref name="cancellationToken"/>.</item>
    ///   <item>The end of the file is reached and <paramref name="watchForChanges"/> is <see langword="false"/>.</item>
    /// </list>
    /// 
    /// </remarks>
    public async IAsyncEnumerable<LogEntry> ReadLogEntriesAsync(string filePath, long offset, SeekOrigin origin, bool watchForChanges, TimeSpan? pollingInterval = null, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        if (!watchForChanges) {
            await foreach (var item in ReadLogEntriesAsync(filePath, offset, origin, cancellationToken)) {
                yield return item;
            }
            yield break;
        }
        
        LogReadingEntries(filePath, offset, origin, true);
        
        var file = new FileInfo(filePath);
        if (!file.Exists) {
            yield break;
        }
        
        using var changeDetector = new ChangeDetector(file, pollingInterval);
        
        while (!cancellationToken.IsCancellationRequested) {
            file.Refresh();
            
            if (!file.Exists) {
                yield break;
            }

            // If the file is read-only, we assume it's the final iteration because no more data
            // will be written to the file.
            var finalIteration = file.IsReadOnly;
            
            await using var stream = OpenFile(file.FullName);

            if (offset > 0) {
                stream.Seek(offset, origin);
            }

            var pipeReader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));

            await foreach (var item in ReadLogEntriesAsync(pipeReader, cancellationToken)) {
                yield return item;
            }

            offset = stream.Position;
            origin = SeekOrigin.Begin; 
            
            // We reached the end of the file.
            if (finalIteration) {
                break;
            }
            
            await changeDetector.ChangesDetected.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }
    
    
    [LoggerMessage(200, LogLevel.Trace, "Reading entries from '{filePath}: offset = {offset}/{origin}, watch for changes = {watchForChanges}")]
    partial void LogReadingEntries(string filePath, long offset, SeekOrigin origin, bool watchForChanges);


    private class ChangeDetector : IDisposable {
        
        private bool _disposed;

        private readonly FileInfo _file;
        
        private readonly TimeSpan _pollingInterval;
        
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        
        public AsyncAutoResetEvent ChangesDetected { get; } = new AsyncAutoResetEvent();


        public ChangeDetector(FileInfo file, TimeSpan? pollingInterval) {
            _file = file;
            _pollingInterval = pollingInterval.HasValue && pollingInterval.Value > TimeSpan.Zero 
                ? pollingInterval.Value 
                : TimeSpan.FromMilliseconds(500);
            _ = RunAsync(_cancellationTokenSource.Token);
        }


        private async Task RunAsync(CancellationToken cancellationToken) {
            try {
                var length = _file.Length;
                while (!cancellationToken.IsCancellationRequested) {
                    await Task.Delay(_pollingInterval, cancellationToken).ConfigureAwait(false);
                    
                    _file.Refresh();
                    
                    if (!_file.Exists || _file.IsReadOnly) {
                        ChangesDetected.Set();
                        break;
                    }
                    
                    if (_file.Length == length) {
                        continue;
                    }

                    length = _file.Length;
                    ChangesDetected.Set();
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {}
        }
        

        public void Dispose() {
            if (_disposed) {
                return;
            }
            
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
            
            _disposed = true;
        }

    }

}
