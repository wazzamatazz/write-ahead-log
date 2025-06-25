using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.DependencyInjection.Internal;

/// <summary>
/// Initialises all <see cref="IWriteAheadLog"/> instances registered with the service provider.
/// </summary>
internal sealed partial class LogInitService : BackgroundService {
    
    private readonly ILogger<LogInitService> _logger;
    
    private readonly IServiceProvider _provider;


    public LogInitService(IServiceProvider provider, ILogger<LogInitService>? logger = null) {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<LogInitService>.Instance;
    }
    
    
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var logs = _provider.GetServices<IWriteAheadLog>();

        foreach (var log in logs) {
            if (stoppingToken.IsCancellationRequested) {
                break;
            }
            
            if (log is null) {
                continue;
            }

            try {
                LogInitialisingInstance(null!);
                await log.InitAsync(stoppingToken).ConfigureAwait(false);
                LogInitialisedInstance(null!);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception e) {
                LogFailedToInitialiseInstance(null!, e);
            }
        }
    }


    [LoggerMessage(1, LogLevel.Debug, "Initialising Write-Ahead Log instance with key '{key}'")]
    partial void LogInitialisingInstance(string key);


    [LoggerMessage(2, LogLevel.Debug, "Initialised Write-Ahead Log instance with key '{key}'")]
    partial void LogInitialisedInstance(string key);
    
    
    [LoggerMessage(3, LogLevel.Error, "Failed to initialise Write-Ahead Log instance with key '{key}'.")]
    partial void LogFailedToInitialiseInstance(string key, Exception error);

}
