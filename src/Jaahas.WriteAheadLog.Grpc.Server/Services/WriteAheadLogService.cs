using Google.Protobuf;

using Grpc.Core;

using Jaahas.WriteAheadLog.DependencyInjection;

using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.Grpc.Services;

public sealed partial class WriteAheadLogService : WriteAheadLog.Grpc.WriteAheadLogService.WriteAheadLogServiceBase {

    private readonly ILogger<WriteAheadLogService> _logger;
    
    private readonly WriteAheadLogFactory _walFactory;


    public WriteAheadLogService(WriteAheadLogFactory walFactory, ILogger<WriteAheadLogService>? logger) {
        _walFactory = walFactory ?? throw new ArgumentNullException(nameof(walFactory));
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WriteAheadLogService>.Instance;
    }


    /// <inheritdoc />
    public override Task<GetLogsResponse> List(GetLogsRequest request, ServerCallContext context) {
        var response = new GetLogsResponse();

        foreach (var name in _walFactory.GetNames()) {
            response.Logs.Add(new LogDescriptor() {
                LogName = name
                // TODO: other properties             
            });
        }
        
        return Task.FromResult(response);
    }


    /// <inheritdoc />
    public override async Task<LogEntryPosition> Write(WriteToLogRequest request, ServerCallContext context) {
        var logName = string.IsNullOrEmpty(request.LogName) 
            ? string.Empty 
            : request.LogName;
        var log = _walFactory.GetWriteAheadLog(logName);

        if (log is null) {
            throw new RpcException(new Status(StatusCode.NotFound, "Log not found."));
        }

        if (_logger.IsEnabled(LogLevel.Trace)) {
            var peerIdentity = context.AuthContext.IsPeerAuthenticated
                ? string.Join(',', context.AuthContext.PeerIdentity.Select(x => x.Value))
                : string.Empty;
            LogWriteToLogRequest(logName, context.AuthContext.IsPeerAuthenticated, peerIdentity, request.Data.Length);
        }

        var result = await log.WriteAsync(request.Data.Memory, context.CancellationToken).ConfigureAwait(false);

        return new LogEntryPosition() {
            SequenceId = result.SequenceId,
            Timestamp = result.Timestamp
        };
    }


    /// <inheritdoc />
    public override async Task WriteStream(IAsyncStreamReader<WriteToLogRequest> requestStream, IServerStreamWriter<LogEntryPosition> responseStream, ServerCallContext context) {
        string? previousLogName = null;
        IWriteAheadLog? previousLog = null;
        
        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false)) {
            var logName = string.IsNullOrEmpty(request.LogName) 
                ? string.Empty 
                : request.LogName;
            
            var log = logName.Equals(previousLogName, StringComparison.Ordinal)
                ? previousLog
                : _walFactory.GetWriteAheadLog(logName);

            if (log is null) {
                throw new RpcException(new Status(StatusCode.NotFound, "Log not found."));
            }

            if (log != previousLog) {
                previousLog = log;
                previousLogName = logName;
            }

            if (_logger.IsEnabled(LogLevel.Trace)) {
                var peerIdentity = context.AuthContext.IsPeerAuthenticated
                    ? string.Join(',', context.AuthContext.PeerIdentity.Select(x => x.Value))
                    : string.Empty;
                LogWriteToLogRequest(logName, context.AuthContext.IsPeerAuthenticated, peerIdentity, request.Data.Length);
            }

            var result = await log.WriteAsync(request.Data.Memory, context.CancellationToken).ConfigureAwait(false);

            await responseStream.WriteAsync(new LogEntryPosition() {
                SequenceId = result.SequenceId,
                Timestamp = result.Timestamp
            }, context.CancellationToken).ConfigureAwait(false);
        }
    }


    /// <inheritdoc />
    public override async Task ReadStream(ReadFromLogRequest request, IServerStreamWriter<LogEntry> responseStream, ServerCallContext context) {
        var log = string.IsNullOrEmpty(request.LogName)
            ? _walFactory.GetWriteAheadLog(string.Empty)
            : _walFactory.GetWriteAheadLog(request.LogName);

        if (log is null) {
            throw new RpcException(new Status(StatusCode.NotFound, "Log not found."));
        }
        
        var startPosition = (request.Position?.LogPositionTypeCase ?? ReadFromLogPosition.LogPositionTypeOneofCase.None) switch {
            ReadFromLogPosition.LogPositionTypeOneofCase.SequenceId => LogPosition.FromSequenceId(request.Position!.SequenceId),
            ReadFromLogPosition.LogPositionTypeOneofCase.Timestamp => LogPosition.FromTimestamp(request.Position!.Timestamp),
            _ => default
        };

        var readOptions = new LogReadOptions(
            Position: startPosition,
            Limit: request.HasLimit ? request.Limit : -1,
            WatchForChanges: request is { HasWatchForChanges: true, WatchForChanges: true });

        try {
            await foreach (var entry in log.ReadAsync(readOptions, context.CancellationToken).ConfigureAwait(false)) {
                try {
                    await responseStream.WriteAsync(new LogEntry() {
                        Position = new LogEntryPosition() {
                            SequenceId = entry.SequenceId, 
                            Timestamp = entry.Timestamp
                        }, 
                        // Use the entry payload directly without copying. This prevents a byte[]
                        // allocation under the hood.
                        Data = UnsafeByteOperations.UnsafeWrap(entry.Data)
                    }).ConfigureAwait(false);
                }
                finally {
                    entry.Dispose();
                }
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) {
            // The client has cancelled the request, so we just exit gracefully.
        }
    }


    [LoggerMessage(1, LogLevel.Trace, "Writing to log '{logName}': authenticated = {authenticated}, caller = {caller}, payload length = {length}", SkipEnabledCheck = true)]
    partial void LogWriteToLogRequest(string logName, bool authenticated, string caller, int length);

}
