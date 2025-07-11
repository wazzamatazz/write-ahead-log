using System.Buffers;
using System.Runtime.CompilerServices;

using Google.Protobuf;

using Grpc.Core;

namespace Jaahas.WriteAheadLog.Grpc;

/// <summary>
/// <see cref="GrpcWriteAheadLog"/> is an <see cref="IWriteAheadLog"/> implementation that uses
/// gRPC to communicate with a Write-Ahead Log (WAL) service.
/// </summary>
public sealed class GrpcWriteAheadLog : WriteAheadLog<GrpcWriteAheadLogOptions> {

    private bool _disposed;
    
    private readonly WriteAheadLog.WriteAheadLogClient _client;

    private bool _channelReady;
    
#if NETCOREAPP
    private AsyncDuplexStreamingCall<WriteToLogRequest, LogEntryPosition>? _writeStream;
#endif

    /// <summary>
    /// Creates a new <see cref="GrpcWriteAheadLog"/> instance.
    /// </summary>
    /// <param name="client">
    ///   The gRPC client to use for communication with the Write-Ahead Log service.
    /// </param>
    /// <param name="options">
    ///   The log options to use.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="client"/> is <see langword="null"/>.
    /// </exception>
    public GrpcWriteAheadLog(WriteAheadLog.WriteAheadLogClient client, GrpcWriteAheadLogOptions? options = null)
        : base(options) {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }
    

    protected override async ValueTask InitCoreAsync(CancellationToken cancellationToken) {
        if (_channelReady) {
            return;
        }

        double backoff = 1000;
        const int maxBackoff = 30_000;
        const double backoffMultiplier = 1.5;
        
        while (!cancellationToken.IsCancellationRequested) {
            try {
                await _client.ListAsync(new GetLogsRequest(), cancellationToken: cancellationToken).ConfigureAwait(false);
                _channelReady = true;
                break;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.Unavailable) {
                await Task.Delay(TimeSpan.FromMilliseconds(backoff), cancellationToken).ConfigureAwait(false);
                if (backoff < maxBackoff) {
                    backoff *= backoffMultiplier;
                    if (backoff > maxBackoff) {
                        backoff = maxBackoff;
                    }
                }
            } 
        }
    }


    /// <inheritdoc />
    protected override async ValueTask<WriteResult> WriteCoreAsync(ReadOnlySequence<byte> data, CancellationToken cancellationToken) {
        var request = new WriteToLogRequest() {
            LogName = Options.RemoteLogName ?? string.Empty, 
            Data = FromReadOnlySequence(data)
        };
        
#if NETCOREAPP
        LogEntryPosition result;

        if (Options.UseStreamingWrites) {
            _writeStream ??= _client.WriteStream(cancellationToken: LifecycleToken);
            await _writeStream.RequestStream.WriteAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
            result = await _writeStream.ResponseStream.MoveNext(cancellationToken: cancellationToken).ConfigureAwait(false)
                ? _writeStream.ResponseStream.Current
                : throw new InvalidOperationException("No response received from the write stream.");
        }
        else {
            result = await _client.WriteAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
#else
        var result = await _client.WriteAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
#endif

        return new WriteResult(result.SequenceId, result.Timestamp);
    }
    

    private static ByteString FromReadOnlySequence(ReadOnlySequence<byte> sequence) {
        if (sequence.IsSingleSegment) {
            return ByteString.CopyFrom(sequence.First.Span);
        }

        // Use a stackalloc'd span if we are not copying too long a sequence. Docs suggest that
        // 1024 bytes is a sensible threshold: https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/operators/stackalloc
        if (sequence.Length < 1024) {
            Span<byte> span = stackalloc byte[(int) sequence.Length];
            sequence.CopyTo(span);
            return ByteString.CopyFrom(span);
        }

        // Sequence is too long to stackalloc a span. We'll use a recyclable memory stream to
        // buffer the long message as efficiently as possible.
        using var stream = WriteAheadLogUtilities.RecyclableMemoryStreamManager.GetStream(
            id: Guid.NewGuid(),
            tag: null,
            requiredSize: sequence.Length);

        sequence.CopyTo(stream.GetSpan());
        if (sequence.Length <= int.MaxValue) {
            stream.Advance((int) sequence.Length);
        }
        else {
            // Advance in int.MaxValue chunks
            var remaining = sequence.Length;
            
            while (remaining > int.MaxValue) {
                stream.Advance(int.MaxValue);
                remaining -= int.MaxValue;
            }

            if (remaining > 0) {
                stream.Advance((int) remaining);
            }
        }

        stream.Position = 0;
        return ByteString.FromStream(stream);
    }
    

    /// <inheritdoc />
    protected override async IAsyncEnumerable<Jaahas.WriteAheadLog.LogEntry> ReadCoreAsync(LogReadOptions options, [EnumeratorCancellation] CancellationToken cancellationToken) {
        var request = new ReadFromLogRequest() {
            LogName = Options.RemoteLogName ?? string.Empty,
            WatchForChanges = options.WatchForChanges
        };

        if (options.Position.SequenceId.HasValue || options.Position.Timestamp.HasValue) {
            request.Position = new ReadFromLogPosition();
            if (options.Position.SequenceId.HasValue) {
                request.Position.SequenceId = options.Position.SequenceId.Value;
            }
            else if (options.Position.Timestamp.HasValue) {
                request.Position.Timestamp = options.Position.Timestamp.Value;
            }
        }
        
        if (options.Limit > 0) {
            request.Limit = options.Limit;
        }
        
        var stream = _client.ReadStream(request, cancellationToken: cancellationToken);

        await foreach (var item in stream.ResponseStream.ReadAllAsync(cancellationToken).ConfigureAwait(false)) {
            yield return Jaahas.WriteAheadLog.LogEntry.Create(item.Position.SequenceId, item.Position.Timestamp, item.Data.Span);
        }
    }


    /// <inheritdoc />
    protected override async ValueTask DisposeAsyncCore() {
        await base.DisposeAsyncCore().ConfigureAwait(false);
        
        if (_disposed) {
            return;
        }
        
#if NETCOREAPP
        _writeStream?.Dispose();
#endif
        
        _disposed = true;
    }

}
