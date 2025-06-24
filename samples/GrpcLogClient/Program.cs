using GrpcLogClient;

using Jaahas.WriteAheadLog;
using Jaahas.WriteAheadLog.DependencyInjection;
using Jaahas.WriteAheadLog.Grpc;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

builder.Services.AddGrpcClient<WriteAheadLogService.WriteAheadLogServiceClient>(options => {
    options.Address = new Uri("https://localhost:7047");
    options.ChannelOptionsActions.Add(channel => {
        channel.MaxRetryAttempts = null;
        channel.MaxReconnectBackoff = TimeSpan.FromSeconds(30);
    });
});

builder.Services.AddSingleton<ICheckpointStore, FileCheckpointStore>();

builder.Services.AddWriteAheadLog(
    string.Empty,
    (provider, _) => ActivatorUtilities.CreateInstance<GrpcWriteAheadLog>(provider, new GrpcWriteAheadLogOptions()));

var host = builder.Build();
host.Run();
