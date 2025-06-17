using System.Buffers;
using System.IO.MemoryMappedFiles;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.Internal;

internal sealed partial class FileSegmentWriter : SegmentWriter {

    private bool _disposed;
    
    private readonly ILogger<FileSegmentWriter> _logger;
    
    private readonly MemoryMappedFile _mmf;
    
    private readonly MemoryMappedViewAccessor _headerAccessor;
    
    private readonly FileStream _fileStream;
    
    private readonly PipeWriter _fileStreamWriter;

    private int _headerFlushRequired;
    
    private int _tailFlushRequired;
    
    private readonly int _flushBatchSize;
    
    private bool _markFileAsReadOnlyOnDispose;
    
    public string FilePath => _fileStream.Name;
    

    public FileSegmentWriter(FileSegmentWriterOptions options, TimeProvider? timeProvider = null, ILogger<FileSegmentWriter>? logger = null) : base(options, timeProvider) {
        ArgumentNullException.ThrowIfNull(options);
        
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<FileSegmentWriter>.Instance;

        var isNewSegment = !File.Exists(options.FilePath);
        
        _fileStream = new FileStream(options.FilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        
        // Ensure minimum file size for header
        if (_fileStream.Length < SerializedSegmentHeaderSize) {
            _fileStream.SetLength(SerializedSegmentHeaderSize);
            isNewSegment = true;
        }
            
        _mmf = MemoryMappedFile.CreateFromFile(
            _fileStream, 
            mapName: null, 
            capacity: 0, // Use file size
            access: MemoryMappedFileAccess.ReadWrite,
            inheritability: HandleInheritability.None,
            leaveOpen: true);
            
        // Map only the header portion for frequent updates
        _headerAccessor = _mmf.CreateViewAccessor(
            offset: 0, 
            size: SerializedSegmentHeaderSize, 
            access: MemoryMappedFileAccess.ReadWrite);
    
        if (isNewSegment) {
            WriteMemoryMappedHeader();
        }
        else {
            Header = ReadMemoryMappedHeader();
        }

        if (Header.ReadOnly) {
            DisposeMemoryMappedResources();
            _fileStream.Dispose();
            throw new InvalidOperationException("Cannot write to a read-only segment.");
        }

        // Move to end of file stream to append new messages
        _fileStream.Seek(0, SeekOrigin.End);
        _fileStreamWriter = PipeWriter.Create(_fileStream, new StreamPipeWriterOptions(leaveOpen: true));

        _flushBatchSize = options.FlushBatchSize > 0
            ? options.FlushBatchSize
            : 0;
        
        if (options.FlushInterval.HasValue && options.FlushInterval.Value > TimeSpan.Zero) {
            _ = BackgroundFlushAsync(options.FlushInterval.Value, DisposedToken);
        }
    }


    private unsafe SegmentHeader ReadMemoryMappedHeader() {
        // Get a pointer to the mapped memory
        byte* ptr = null;
        _headerAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        try {
            var headerSpan = new Span<byte>(ptr, SerializedSegmentHeaderSize);
            return DeserializeHeader(headerSpan);
        }
        finally {
            _headerAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
    
    
    private unsafe void WriteMemoryMappedHeader() {
        // Get a pointer to the mapped memory
        byte* ptr = null; 
        _headerAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        try {
            // Create a span over the mapped memory
            var headerSpan = new Span<byte>(ptr, SerializedSegmentHeaderSize);
            SerializeHeader(Header, headerSpan);
        }
        finally {
            _headerAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        Interlocked.Exchange(ref _headerFlushRequired, 1);
    }


    /// <inheritdoc />
    protected override async ValueTask<long> WriteMessageCoreAsync(LogMessage message, ulong sequenceId, long timestamp) {
        var bytesWritten = LogEntry.Write(
            _fileStreamWriter, 
            sequenceId, 
            timestamp, 
            message.Stream?.GetReadOnlySequence() ?? new ReadOnlySequence<byte>(ReadOnlyMemory<byte>.Empty));
        
        LogAfterWriteMessage(sequenceId, timestamp, bytesWritten);
        WriteMemoryMappedHeader();
        
        if (_flushBatchSize > 0 && Header.MessageCount % _flushBatchSize == 0) {
            // Immediate flush if batch size is reached.
            await FlushCoreAsync().ConfigureAwait(false);
        }
        else {
            Interlocked.Exchange(ref _tailFlushRequired, 1);
        }
        
        return bytesWritten;
    }


    protected override ValueTask WriteHeaderCoreAsync() {
        WriteMemoryMappedHeader();
        return ValueTask.CompletedTask;
    }


    private async Task BackgroundFlushAsync(TimeSpan interval, CancellationToken cancellationToken) {
        try {
            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            await FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }
    
    
    protected override async ValueTask FlushCoreAsync() {
        if (_disposed) {
            return;
        }
        
        // Flush the header if required
        FlushHeader();
        
        // Flush the tail of the file stream
        await FlushTailAsync().ConfigureAwait(false);
    }
    
    
    private void FlushHeader() {
        if (Interlocked.CompareExchange(ref _headerFlushRequired, 0, 1) != 1) {
            return; // No flush required
        }

        LogFlushingHeader();
        _headerAccessor.Flush();
    }
    
    
    private async ValueTask FlushTailAsync() {
        if (Interlocked.CompareExchange(ref _tailFlushRequired, 0, 1) != 1) {
            return; // No flush required
        }

        LogFlushingTail();
        await _fileStreamWriter.FlushAsync().ConfigureAwait(false);
    }


    /// <inheritdoc />
    protected override ValueTask CloseSegmentCoreAsync() {
        LogClosingSegment(_fileStream.Name);
        WriteMemoryMappedHeader();
        _markFileAsReadOnlyOnDispose = true;
        return ValueTask.CompletedTask;
    }


    protected override async ValueTask DisposeCoreAsync() {
        if (_disposed) {
            return;
        }
        
        DisposeMemoryMappedResources();
        await _fileStream.DisposeAsync().ConfigureAwait(false);
        if (_markFileAsReadOnlyOnDispose) {
            // Set the file to read-only if it was marked as such
            File.SetAttributes(_fileStream.Name, FileAttributes.ReadOnly);
        }

        _disposed = true;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void DisposeMemoryMappedResources() {
        _headerAccessor.Dispose();
        _mmf.Dispose();
    }

    
    [LoggerMessage(1, LogLevel.Trace, "Wrote message: sequence ID = {sequenceId}, timestamp = {timestamp}, bytes written = {bytesWritten}")]
    partial void LogAfterWriteMessage(ulong sequenceId, long timestamp, long bytesWritten);

    [LoggerMessage(101, LogLevel.Trace, "Flushing header.")]
    partial void LogFlushingHeader();
    
    [LoggerMessage(102, LogLevel.Trace, "Flushing tail.")]
    partial void LogFlushingTail();
    
    [LoggerMessage(103, LogLevel.Information, "Closing segment and setting read-only attribute: {filePath}")]
    partial void LogClosingSegment(string filePath);

}
