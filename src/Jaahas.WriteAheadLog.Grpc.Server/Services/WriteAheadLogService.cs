using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.Grpc.Services;

public sealed partial class WriteAheadLogService : WriteAheadLog.Grpc.WriteAheadLogService.WriteAheadLogServiceBase {

    private readonly ILogger<WriteAheadLogService> _logger;
    
    private readonly IServiceProvider _serviceProvider;


    public WriteAheadLogService(IServiceProvider serviceProvider, ILogger<WriteAheadLogService>? logger) {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WriteAheadLogService>.Instance;
    }


    /// <inheritdoc />
    public override Task<GetLogsResponse> List(GetLogsRequest request, ServerCallContext context) => base.List(request, context);


    /// <inheritdoc />
    public override async Task<LogEntryPosition> Write(WriteToLogRequest request, ServerCallContext context) {
        var logName = string.IsNullOrEmpty(request.LogName) 
            ? string.Empty 
            : request.LogName;
        var log = _serviceProvider.GetKeyedService<IWriteAheadLog>(logName);

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
    public override async Task Read(ReadFromLogRequest request, IServerStreamWriter<LogEntry> responseStream, ServerCallContext context) {
        var log = string.IsNullOrEmpty(request.LogName)
            ? _serviceProvider.GetKeyedService<IWriteAheadLog>(string.Empty)
            : _serviceProvider.GetKeyedService<IWriteAheadLog>(request.LogName);

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
            await foreach (var entry in log.ReadAllAsync(readOptions, context.CancellationToken).ConfigureAwait(false)) {
                try {
                    await responseStream.WriteAsync(new LogEntry() {
                        Position = new LogEntryPosition() {
                            SequenceId = entry.SequenceId, 
                            Timestamp = entry.Timestamp
                        }, Data = ByteString.CopyFrom(entry.Data)
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
