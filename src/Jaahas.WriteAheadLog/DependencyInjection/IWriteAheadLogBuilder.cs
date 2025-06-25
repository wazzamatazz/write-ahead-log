using Microsoft.Extensions.DependencyInjection;

namespace Jaahas.WriteAheadLog.DependencyInjection;

/// <summary>
/// Builder for registering write-ahead log services.
/// </summary>
public interface IWriteAheadLogBuilder {
    
    /// <summary>
    /// The service collection.
    /// </summary>
    public IServiceCollection Services { get; }

}
