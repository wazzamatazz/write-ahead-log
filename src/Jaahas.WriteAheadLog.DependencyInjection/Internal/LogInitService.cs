using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.DependencyInjection.Internal;

/// <summary>
/// Initialises all <see cref="FileWriteAheadLog"/> instances registered with the service provider.
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
        var registrations = _provider.GetServices<KeyedLogRegistration>();

        foreach (var registration in registrations) {
            if (stoppingToken.IsCancellationRequested) {
                break;
            }
            
            var log = _provider.GetKeyedService<IWriteAheadLog>(registration.Key);
            if (log is null) {
                continue;
            }

            try {
                LogInitialisingInstance(registration.Key);
                await log.InitAsync(stoppingToken).ConfigureAwait(false);
                LogInitialisedInstance(registration.Key);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception e) {
                LogFailedToInitialiseInstance(registration.Key, e);
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
