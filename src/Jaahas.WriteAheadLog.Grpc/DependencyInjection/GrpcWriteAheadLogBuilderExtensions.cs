using Jaahas.WriteAheadLog.Grpc;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

// ReSharper disable once CheckNamespace
namespace Jaahas.WriteAheadLog.DependencyInjection;

public static class GrpcWriteAheadLogBuilderExtensions {

    public static IWriteAheadLogBuilder AddGrpc(this IWriteAheadLogBuilder builder, Action<GrpcWriteAheadLogOptions> configure) 
        => builder.AddGrpc(string.Empty, configure);


    public static IWriteAheadLogBuilder AddGrpc(this IWriteAheadLogBuilder builder, string name, Action<GrpcWriteAheadLogOptions> configure) {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);
        
        builder.Services.AddOptions<GrpcWriteAheadLogOptions>(name)
            .Configure(configure);
        
        return builder.AddLog<GrpcWriteAheadLog>(
            name, 
            provider => ActivatorUtilities.CreateInstance<GrpcWriteAheadLog>(
                provider, 
                provider.GetRequiredService<IOptionsMonitor<GrpcWriteAheadLogOptions>>().Get(name)));
    }

}
