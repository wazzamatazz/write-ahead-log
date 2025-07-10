# About

Jaahas.WriteAheadLog.Grpc provides a gRPC-based implementation of the `IWriteAheadLog` interface for .NET applications. It allows you to use a remote write-ahead log service over gRPC, enabling distributed logging capabilities with high performance and reliability.

# Features

- Remote write-ahead log service using gRPC
- Supports durable, append-only logging
- High-performance, low-latency communication
- Integration with dependency injection
- Supports multiple named logs with different configurations

# Getting Started

To use the gRPC write-ahead log, register it with your application's dependency injection container:

```csharp
services.AddGrpcClient<WriteAheadLog.WriteAheadLogClient>(options => {
    options.Address = new Uri("https://my-log-server:12345");
});

services.AddWriteAheadLogServices()
    .AddGrpc(options => {
        // Use "" for default log
        options.RemoteLogName = "my-log"; 
    });
```

You can then inject `IWriteAheadLog` into your components:

```csharp
var log = serviceProvider.GetRequiredService<IWriteAheadLog>();
```


# Writing Log Entries

Write entries using the `WriteAsync` method:

```csharp
await log.WriteAsync(Encoding.UTF8.GetBytes("Hello, World!"));
```


# Reading Log Entries

Read entries using the `ReadAsync` method or one of its extension method overloads. You must dispose of each `LogEntry` instance once no longer needed to release shared resources:

```csharp
await foreach (var entry in log.ReadAsync()) {
    try {
        var data = Encoding.UTF8.GetString(entry.Data);
        Console.WriteLine(data);
    }
    finally {
        // Dispose of the entry when done to release shared resources
        entry.Dispose();
    }
}
```
