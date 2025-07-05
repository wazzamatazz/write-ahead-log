using Jaahas.WriteAheadLog.Grpc;

using Microsoft.Extensions.DependencyInjection;

// ReSharper disable once CheckNamespace
namespace Jaahas.WriteAheadLog.DependencyInjection;

public static class GrpcWriteAheadLogBuilderExtensions {

    public static IWriteAheadLogBuilder AddGrpc(this IWriteAheadLogBuilder builder, Action<GrpcWriteAheadLogOptions> configure) 
        => builder.AddGrpc(string.Empty, configure);


    public static IWriteAheadLogBuilder AddGrpc(this IWriteAheadLogBuilder builder, string name, Action<GrpcWriteAheadLogOptions> configure) {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);
        
        return builder.AddLog<GrpcWriteAheadLog, GrpcWriteAheadLogOptions>(
            name, 
            configure, 
            (provider, options) => ActivatorUtilities.CreateInstance<GrpcWriteAheadLog>(provider, options));
    }

}
