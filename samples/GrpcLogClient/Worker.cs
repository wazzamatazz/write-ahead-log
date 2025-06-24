using System.Text;

using Jaahas.WriteAheadLog;

namespace GrpcLogClient;

public sealed partial class Worker : BackgroundService {

    private readonly ILogger<Worker> _logger;
    
    private readonly IWriteAheadLog _wal;
    
    private readonly ICheckpointStore _checkpointStore;

    private readonly IServiceProvider _serviceProvider;
    

    public Worker(ICheckpointStore checkpointStore, IServiceProvider serviceProvider, ILogger<Worker> logger) {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _checkpointStore = checkpointStore ?? throw new ArgumentNullException(nameof(checkpointStore));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _wal = _serviceProvider.GetRequiredKeyedService<IWriteAheadLog>(string.Empty);
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        await using var reader = ActivatorUtilities.CreateInstance<LogReader>(_serviceProvider, _wal, _checkpointStore);
        reader.ProcessEntry += args => {
            LogEntryReceived(args.Entry.SequenceId, args.Entry.Timestamp, Encoding.UTF8.GetString(args.Entry.Data));
            return Task.CompletedTask;
        };

        while (!stoppingToken.IsCancellationRequested) {
            try {
                await reader.RunAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception e) {
                LogReaderError(e);
            }
        }
    }


    [LoggerMessage(1, LogLevel.Information, "Received log entry: Sequence ID = {sequenceId}, Timestamp = {timestamp}, Data = {data}")]
    partial void LogEntryReceived(ulong sequenceId, long timestamp, string data);


    [LoggerMessage(2, LogLevel.Error, "Error while running log reader.")]
    partial void LogReaderError(Exception error);

}
