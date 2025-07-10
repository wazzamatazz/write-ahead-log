# About

Jaahas.WriteAheadLog.Grpc.Client provides a gRPC-based client implementation for the Jaahas.WriteAheadLog.Grpc service. It allows .NET applications to interact with a remote write-ahead log service over gRPC, enabling distributed logging capabilities with high performance and reliability.


# Getting Started

To use the gRPC write-ahead log client, register it with your application's dependency injection container:

```csharp
services.AddGrpcClient<WriteAheadLog.WriteAheadLogClient>(options => {
    options.Address = new Uri("https://my-log-server:12345");
});
```

You can then use the client to write and read log entries:

```csharp
var client = serviceProvider.GetRequiredService<WriteAheadLog.WriteAheadLogClient>();

var request = new WriteToLogRequest() {
    // Use "" for default log
    LogName = "my-log",
    Data = ByteString.CopyFromUtf8("Hello, World!")
};

var response = await client.WriteAsync(request);
Console.WroteLine($"Sequence ID: {response.SequenceId}");
```
