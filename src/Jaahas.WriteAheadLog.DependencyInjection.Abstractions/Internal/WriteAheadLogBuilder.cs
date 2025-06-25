using Microsoft.Extensions.DependencyInjection;

namespace Jaahas.WriteAheadLog.DependencyInjection.Internal;

internal class WriteAheadLogBuilder : IWriteAheadLogBuilder {

    /// <inheritdoc />
    public IServiceCollection Services { get; }


    public WriteAheadLogBuilder(IServiceCollection services) {
        Services = services;
    }

}
