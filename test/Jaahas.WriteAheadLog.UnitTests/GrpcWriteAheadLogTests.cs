extern alias grpcserver;
extern alias grpcclient;

using Google.Protobuf;

using Grpc.Net.Client;

using Jaahas.WriteAheadLog.DependencyInjection;
using Jaahas.WriteAheadLog.FileSystem;

using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.TestHost;

using GrpcServerNS = grpcserver::Jaahas.WriteAheadLog.Grpc.Server;
using GrpcClientNS = grpcclient::Jaahas.WriteAheadLog.Grpc;

namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public class GrpcWriteAheadLogTests {
    
    private static string s_tempPath = null!;

    public TestContext TestContext { get; set; } = null!;


    [ClassInitialize]
    public static void ClassInitialize(TestContext testContext) {
        s_tempPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        new DirectoryInfo(s_tempPath).Create();
    }


    [ClassCleanup]
    public static void ClassCleanup() {
        if (!string.IsNullOrWhiteSpace(s_tempPath) && Directory.Exists(s_tempPath)) {
            Directory.Delete(s_tempPath, true);
        }
    }


    private static async Task<WebApplication> CreateWebApplicationAsync(TestContext testContext) {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddGrpc();
        builder.Services.AddWriteAheadLogServices()
            .AddFile(testContext.TestName!, options => {
                options.DataDirectory = Path.Combine(s_tempPath, testContext.TestName!);
            });
        
        builder.WebHost.UseTestServer();
        
        var app = builder.Build();
        app.MapGrpcService<GrpcServerNS.WriteAheadLogService>();
        
        await app.StartAsync(testContext.CancellationTokenSource.Token);
        return app;
    }
    

    private static GrpcClientNS.WriteAheadLog.WriteAheadLogClient CreateClient(WebApplication app) {
        var server = (TestServer) app.Services.GetRequiredService<IServer>();
        
        var httpClient = server.CreateClient();
        var channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions() {
            HttpClient = httpClient
        });
        
        return new GrpcClientNS.WriteAheadLog.WriteAheadLogClient(channel);
    }


    [TestMethod]
    public async Task GrpcClientShouldWriteMessageAsUnary() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        
        var result = await client.WriteAsync(new GrpcClientNS.WriteToLogRequest() {
            LogName = TestContext.TestName!,
            Data = ByteString.CopyFromUtf8("Hello, World!")
        }, cancellationToken: TestContext.CancellationTokenSource.Token);
        
        Assert.IsNotNull(result);
        Assert.AreEqual(1UL, result.SequenceId);
    }
    
    
    [TestMethod]
    public async Task GrpcClientShouldWriteMessagesAsStream() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        
        using var stream = client.WriteStream(cancellationToken: TestContext.CancellationTokenSource.Token);

        await stream.RequestStream.WriteAsync(
            new GrpcClientNS.WriteToLogRequest() {
                LogName = TestContext.TestName!,
                Data = ByteString.CopyFromUtf8("Hello, World! #1")
            },
            cancellationToken: TestContext.CancellationTokenSource.Token);
        
        await stream.RequestStream.WriteAsync(
            new GrpcClientNS.WriteToLogRequest() {
                LogName = TestContext.TestName!,
                Data = ByteString.CopyFromUtf8("Hello, World! #2")
            },
            cancellationToken: TestContext.CancellationTokenSource.Token);
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(cancellationToken: TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(1UL, stream.ResponseStream.Current.SequenceId);
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(cancellationToken: TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(2UL, stream.ResponseStream.Current.SequenceId);
    }

    
    [TestMethod]
    public async Task GrpcClientShouldReadAllMessages() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var msg1 = await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg2 = await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg3 = await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg4 = await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);
        
        using var stream = client.ReadStream(new GrpcClientNS.ReadFromLogRequest() {
            LogName = TestContext.TestName!
        }, cancellationToken: TestContext.CancellationTokenSource.Token);
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg1.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg1.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #1", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg2.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg2.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #2", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg3.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg3.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #3", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg4.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg4.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #4", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsFalse(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
    }
    

    [TestMethod]
    public async Task GrpcClientShouldReadMessagesFromSequenceId() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var msg1 = await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg2 = await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg3 = await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg4 = await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);
        
        using var stream = client.ReadStream(new GrpcClientNS.ReadFromLogRequest() {
            LogName = TestContext.TestName!,
            Position = new GrpcClientNS.ReadFromLogPosition() {
                SequenceId = msg2.SequenceId
            },
            Limit = 2
        }, cancellationToken: TestContext.CancellationTokenSource.Token);
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg2.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg2.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #2", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg3.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg3.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #3", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsFalse(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
    }
    
    
    [TestMethod]
    public async Task GrpcClientShouldReadMessagesFromTimestamp() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var msg1 = await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg2 = await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg3 = await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg4 = await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);
        
        using var stream = client.ReadStream(new GrpcClientNS.ReadFromLogRequest() {
            LogName = TestContext.TestName!,
            Position = new GrpcClientNS.ReadFromLogPosition() {
                Timestamp = msg2.Timestamp
            },
            Limit = 2
        }, cancellationToken: TestContext.CancellationTokenSource.Token);
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg2.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg2.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #2", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg3.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg3.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #3", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsFalse(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
    }
    
    
    [TestMethod]
    public async Task GrpcClientShouldWatchForNewMessages() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        
        // Start the streaming operation to read from the log.
        
        using var stream = client.ReadStream(new GrpcClientNS.ReadFromLogRequest() {
            LogName = TestContext.TestName!,
            WatchForChanges = true
        }, cancellationToken: TestContext.CancellationTokenSource.Token);
        
        // Now write some messages to the local log.
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var msg1 = await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg2 = await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg3 = await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        var msg4 = await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token);
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);
        
        // Assert that we receive the messages in the stream.
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg1.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg1.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #1", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg2.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg2.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #2", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg3.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg3.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #3", stream.ResponseStream.Current.Data.ToStringUtf8());
        
        Assert.IsTrue(await stream.ResponseStream.MoveNext(TestContext.CancellationTokenSource.Token));
        Assert.IsNotNull(stream.ResponseStream.Current);
        Assert.AreEqual(msg4.SequenceId, stream.ResponseStream.Current.Position.SequenceId);
        Assert.AreEqual(msg4.Timestamp, stream.ResponseStream.Current.Position.Timestamp);
        Assert.AreEqual("Hello, World! #4", stream.ResponseStream.Current.Data.ToStringUtf8());
    }
    
    
    [TestMethod]
    public async Task GrpcWriteAheadLogShouldWriteMessageAsUnary() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        await using var wal = new GrpcClientNS.GrpcWriteAheadLog(
            client,
            new GrpcClientNS.GrpcWriteAheadLogOptions() {
                LogName = TestContext.TestName!,
                UseStreamingWrites = false
            });
        
        var result = await client.WriteAsync(new GrpcClientNS.WriteToLogRequest() {
            LogName = TestContext.TestName!,
            Data = ByteString.CopyFromUtf8("Hello, World!")
        }, cancellationToken: TestContext.CancellationTokenSource.Token);
        
        Assert.IsNotNull(result);
        Assert.AreEqual(1UL, result.SequenceId);
    }
    
    
    [TestMethod]
    public async Task GrpcWriteAheadLogShouldWriteMessagesAsStream() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        await using var wal = new GrpcClientNS.GrpcWriteAheadLog(
            client,
            new GrpcClientNS.GrpcWriteAheadLogOptions() {
                LogName = TestContext.TestName!,
                UseStreamingWrites = true
            });
        
        var msg1 = await wal.WriteAsync("Hello, World! #1"u8.ToArray(), TestContext.CancellationTokenSource.Token);
        var msg2 = await wal.WriteAsync("Hello, World! #2"u8.ToArray(), TestContext.CancellationTokenSource.Token);
        
        Assert.AreEqual(1UL, msg1.SequenceId);
        Assert.AreEqual(2UL, msg2.SequenceId);
    }
    
    
    [TestMethod]
    public async Task GrpcWriteAheadLogShouldReadAllMessages() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        await using var wal = new GrpcClientNS.GrpcWriteAheadLog(
            client,
            new GrpcClientNS.GrpcWriteAheadLogOptions() {
                LogName = TestContext.TestName!
            });
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var writeResults = new List<WriteResult>() {
            await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token)
        };
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);

        var count = 0;
        TestContext.CancellationTokenSource.CancelAfter(10_000);
        await foreach (var item in wal.ReadAsync(cancellationToken: TestContext.CancellationTokenSource.Token)) {
            try {
                ++count;
                if (count > writeResults.Count) {
                    Assert.Fail("Read too many messages.");
                }

                var writeResult = writeResults[count - 1];
                Assert.AreEqual(writeResult.SequenceId, item.SequenceId);
                Assert.AreEqual(writeResult.Timestamp, item.Timestamp);
                Assert.AreEqual($"Hello, World! #{writeResult.SequenceId}", System.Text.Encoding.UTF8.GetString(item.Data));
            }
            finally {
                item.Dispose();
            }
        }
        
        Assert.AreEqual(writeResults.Count, count);
    }
    
    
    [TestMethod]
    public async Task GrpcWriteAheadLogShouldReadMessagesFromSequenceId() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        await using var wal = new GrpcClientNS.GrpcWriteAheadLog(
            client,
            new GrpcClientNS.GrpcWriteAheadLogOptions() {
                LogName = TestContext.TestName!
            });
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var writeResults = new List<WriteResult>() {
            await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token)
        };
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);

        var count = 0;
        TestContext.CancellationTokenSource.CancelAfter(10_000);
        await foreach (var item in wal.ReadAsync(position: LogPosition.FromSequenceId(writeResults[1].SequenceId), limit: 2, cancellationToken: TestContext.CancellationTokenSource.Token)) {
            try {
                ++count;
                if (count > 2) {
                    Assert.Fail("Read too many messages.");
                }

                var writeResult = writeResults[count];
                Assert.AreEqual(writeResult.SequenceId, item.SequenceId);
                Assert.AreEqual(writeResult.Timestamp, item.Timestamp);
                Assert.AreEqual($"Hello, World! #{writeResult.SequenceId}", System.Text.Encoding.UTF8.GetString(item.Data));
            }
            finally {
                item.Dispose();
            }
        }
        
        Assert.AreEqual(2, count);
    }
    
    
    [TestMethod]
    public async Task GrpcWriteAheadLogShouldReadMessagesFromTimestamp() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        await using var wal = new GrpcClientNS.GrpcWriteAheadLog(
            client,
            new GrpcClientNS.GrpcWriteAheadLogOptions() {
                LogName = TestContext.TestName!
            });
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var writeResults = new List<WriteResult>() {
            await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token)
        };
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);

        var count = 0;
        TestContext.CancellationTokenSource.CancelAfter(10_000);
        await foreach (var item in wal.ReadAsync(position: LogPosition.FromTimestamp(writeResults[1].Timestamp), limit: 2, cancellationToken: TestContext.CancellationTokenSource.Token)) {
            try {
                ++count;
                if (count > 2) {
                    Assert.Fail("Read too many messages.");
                }

                var writeResult = writeResults[count];
                Assert.AreEqual(writeResult.SequenceId, item.SequenceId);
                Assert.AreEqual(writeResult.Timestamp, item.Timestamp);
                Assert.AreEqual($"Hello, World! #{writeResult.SequenceId}", System.Text.Encoding.UTF8.GetString(item.Data));
            }
            finally {
                item.Dispose();
            }
        }
        
        Assert.AreEqual(2, count);
    }
    
    
    [TestMethod]
    public async Task GrpcWriteAheadLogShouldWatchForNewMessages() {
        await using var app = await CreateWebApplicationAsync(TestContext);
        var client = CreateClient(app);
        await using var wal = new GrpcClientNS.GrpcWriteAheadLog(
            client,
            new GrpcClientNS.GrpcWriteAheadLogOptions() {
                LogName = TestContext.TestName!
            });
        
        var localWal = app.Services.GetRequiredKeyedService<IWriteAheadLog>(TestContext.TestName!);
        var writeResults = new List<WriteResult>() {
            await localWal.WriteAsync("Hello, World! #1"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
            await localWal.WriteAsync("Hello, World! #2"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token),
        };
        await ((FileWriteAheadLog) localWal).FlushAsync(TestContext.CancellationTokenSource.Token);

        var count = 0;
        TestContext.CancellationTokenSource.CancelAfter(10_000);
        await foreach (var item in wal.ReadAsync(watchForChanges: true, cancellationToken: TestContext.CancellationTokenSource.Token)) {
            try {
                ++count;
                if (count > 4) {
                    Assert.Fail("Read too many messages.");
                }

                var writeResult = writeResults[count - 1];
                Assert.AreEqual(writeResult.SequenceId, item.SequenceId);
                Assert.AreEqual(writeResult.Timestamp, item.Timestamp);
                Assert.AreEqual($"Hello, World! #{writeResult.SequenceId}", System.Text.Encoding.UTF8.GetString(item.Data));

                if (count == 4) {
                    break;
                }
                
                if (count != 2) {
                    continue;
                }
                
                // Write some more messages to the local log after reading the initial messages.
                writeResults.Add(await localWal.WriteAsync("Hello, World! #3"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token));
                writeResults.Add(await localWal.WriteAsync("Hello, World! #4"u8.ToArray(), cancellationToken: TestContext.CancellationTokenSource.Token));
            }
            finally {
                item.Dispose();
            }
        }
        
        Assert.AreEqual(4, count);
    }

}
