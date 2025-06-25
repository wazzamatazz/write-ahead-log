using GrpcLogServer;

using Jaahas.WriteAheadLog.DependencyInjection;
using Jaahas.WriteAheadLog.Grpc.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

builder.Services.AddWriteAheadLogServices()
    .AddFileWriteAheadLog(string.Empty, options => {
        options.MaxSegmentMessageCount = 10;
        options.SegmentRetentionLimit = 5;
        options.SegmentCleanupInterval = TimeSpan.FromMinutes(1);
    });

builder.Services.AddHostedService<LogWriterService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<WriteAheadLogService>();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
