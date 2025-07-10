# About

Jaahas.WriteAheadLog.Grpc.Server provides a gRPC-based server that allows remote clients to write to a hosted `IWriteAheadLog` service, enabling distributed logging capabilities with high performance and reliability.


# Getting Started

To use the gRPC service, you must register one or more `IWriteAheadLog` services with your application's dependency injection container. The gRPC server will then expose these services over gRPC.

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

builder.Services.AddWriteAheadLogServices()
    .AddFile("file-1", options => {
        options.DataDirectory = "data-1/wal";
    })
    .AddFile("file-2", options => {
        options.DataDirectory = "data-2/wal";
    });

var app = builder.Build();

app.MapGrpcService<WriteAheadLogService>();

app.Run();
```
