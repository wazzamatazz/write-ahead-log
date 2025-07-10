# About

Jaahas.WriteAheadLog.FileSystem provides a durable, high-performance file-based implementation of the `IWriteAheadLog` interface for .NET applications. It is the default storage backend for the Jaahas Write-Ahead Log (WAL) system, supporting reliable, append-only logging with configurable segment rollover and retention policies.

# Features

- Durable, append-only file-based log segments
- Automatic segment rollover (by size, time, or message count)
- Configurable retention and cleanup policies
- High-throughput, low-latency logging
- Integration with dependency injection

# Getting Started

To use the file-based write-ahead log, register it with your application's dependency injection container:

```csharp
services.AddWriteAheadLogServices().AddFile(options => {
    options.DataDirectory = "data/wal";
});
```

You can then inject `IWriteAheadLog` into your components:

```csharp
var log = serviceProvider.GetRequiredService<IWriteAheadLog>();
```

# Registering Multiple Logs

You can register multiple named logs with different configurations:

```csharp
services.AddWriteAheadLogServices()
    .AddFile("log-1", options => { options.DataDirectory = "data/wal-1"; })
    .AddFile("log-2", options => { options.DataDirectory = "data/wal-2"; });
```

Resolve a specific log by name:

```csharp
var log1 = serviceProvider.GetRequiredKeyedService<IWriteAheadLog>("log-1");
```

Or use the factory:

```csharp
var log2 = serviceProvider.GetRequiredService<WriteAheadLogFactory>()
    .GetWriteAheadLog("log-2");
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


# Configuring Rollover and Retention

You can control segment rollover and retention using `FileWriteAheadLogOptions`:

```csharp
var options = new FileWriteAheadLogOptions {
    DataDirectory = "data/wal",
    MaxSegmentSizeBytes = 1024 * 1024, // 1 MB
    SegmentRetentionLimit = 100,
    SegmentRetentionPeriod = TimeSpan.FromDays(7),
    SegmentCleanupInterval = TimeSpan.FromHours(12)
};
```


# More Information

Please visit the [GitHub repository](https://github.com/wazzamatazz/write-ahead-log) for advanced usage, including using the `LogWriter` and `JsonLogWriter` types for writing log entries, and using the `LogReader` type for reading log entries and maintaining progress checkpoints.
