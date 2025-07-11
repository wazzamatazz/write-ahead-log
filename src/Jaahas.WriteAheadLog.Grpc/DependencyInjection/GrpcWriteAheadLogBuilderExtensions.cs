using Jaahas.WriteAheadLog.Grpc;
using Jaahas.WriteAheadLog.Internal;

using Microsoft.Extensions.DependencyInjection;

// ReSharper disable once CheckNamespace
namespace Jaahas.WriteAheadLog.DependencyInjection;

public static class GrpcWriteAheadLogBuilderExtensions {

    public static IWriteAheadLogBuilder AddGrpc(this IWriteAheadLogBuilder builder, Action<GrpcWriteAheadLogOptions> configure) 
        => builder.AddGrpc(string.Empty, configure);


    public static IWriteAheadLogBuilder AddGrpc(this IWriteAheadLogBuilder builder, string name, Action<GrpcWriteAheadLogOptions> configure) {
        ExceptionHelper.ThrowIfNull(builder);
        ExceptionHelper.ThrowIfNull(name);
        ExceptionHelper.ThrowIfNull(configure);
        
        return builder.AddLog<GrpcWriteAheadLog, GrpcWriteAheadLogOptions>(
            name, 
            configure, 
            (provider, options) => ActivatorUtilities.CreateInstance<GrpcWriteAheadLog>(provider, options));
    }

}
