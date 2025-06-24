using Jaahas.WriteAheadLog;

namespace GrpcLogServer;

internal sealed class LogWriterService : BackgroundService {

    private bool _disposed;
    
    private readonly TimeProvider _timeProvider;

    private readonly IWriteAheadLog _wal;

    private readonly JsonLogWriter _jsonWriter = new JsonLogWriter();


    public LogWriterService(TimeProvider timeProvider, IWriteAheadLog wal) {
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
    }


    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        while (!stoppingToken.IsCancellationRequested) {
            await Task.Delay(5000, stoppingToken);
            await _jsonWriter.WriteToLogAsync(_wal, new {
                CurrentTime = _timeProvider.GetUtcNow(),
                Message = "Hello, World!",
                RandomNumber = Random.Shared.Next(1, 100)
            });
        }
        
    }


    /// <inheritdoc />
    public override void Dispose() {
        if (_disposed) {
            return;
        }

        _jsonWriter.Dispose();
        
        _disposed = true;
    }

}
