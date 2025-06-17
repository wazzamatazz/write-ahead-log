using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public class LogTests {

    private static string s_tempPath = null!;
    
    private static IServiceProvider s_serviceProvider = null!;

    public TestContext? TestContext { get; set; }


    [ClassInitialize]
    public static void ClassInitialize(TestContext testContext) {
        s_tempPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        new DirectoryInfo(s_tempPath).Create();
        
        s_serviceProvider = new ServiceCollection()
            .AddSingleton(TimeProvider.System)
            .AddLogging(builder => {
                builder.AddDebug();
                builder.SetMinimumLevel(LogLevel.Trace);
            })
            .BuildServiceProvider();
    }


    [ClassCleanup]
    public static void ClassCleanup() {
        if (!string.IsNullOrWhiteSpace(s_tempPath) && Directory.Exists(s_tempPath)) {
            Directory.Delete(s_tempPath, true);
        }
    }

    
    [TestMethod]
    public async Task ShouldThrowOnWriteWhenNotInitialized() {
        await using var log = new Log(new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () => {
            using var msg = new LogMessage(Enumerable.Repeat((byte) 1, 64).ToArray());
            await log.WriteAsync(msg);
        });
    }
    
    
    [TestMethod]
    public async Task ShouldThrowOnReadFromPositionWhenNotInitialized() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () => {
            await foreach (var _ in log.ReadFromPositionAsync()) {
                // This will throw since the log is not initialized
            }
        });
    }
    
    
    [TestMethod]
    public async Task ShouldThrowOnReadFromTimestampWhenNotInitialized() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () => {
            await foreach (var _ in log.ReadFromTimestampAsync()) {
                // This will throw since the log is not initialized
            }
        });
    }
    

    [TestMethod]
    public async Task ShouldWriteMessages() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await log.InitAsync();
        
        using var msg1 = new LogMessage(Enumerable.Repeat((byte) 1, 64).ToArray());
        await log.WriteAsync(msg1);
        
        await Task.Delay(10);
        
        using var msg2 = new LogMessage(Enumerable.Repeat((byte) 2, 64).ToArray());
        await log.WriteAsync(msg2);
    }
    
    
    [TestMethod]
    public async Task ShouldReadMessagesFromActiveSegment() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await log.InitAsync();
        
        var writeResults = new List<WriteResult>();

        using var msg1 = new LogMessage(Enumerable.Repeat((byte) 1, 64).ToArray());
        writeResults.Add(await log.WriteAsync(msg1));
        
        await Task.Delay(10);
        
        using var msg2 = new LogMessage(Enumerable.Repeat((byte) 2, 64).ToArray());
        writeResults.Add(await log.WriteAsync(msg2));
        
        using var msg3 = new LogMessage(Enumerable.Repeat((byte) 3, 64).ToArray());
        writeResults.Add(await log.WriteAsync(msg3));

        await log.FlushAsync();
        
        TestContext.CancellationTokenSource.CancelAfter(10_000);

        await foreach (var msg in log.ReadFromPositionAsync(cancellationToken: TestContext.CancellationTokenSource.Token)) {
            if (writeResults.Count == 0) {
                Assert.Fail("No write results available to compare with.");
            }
            
            var expected = writeResults[0];
            writeResults.RemoveAt(0);
            
            Assert.AreEqual(expected.SequenceId, msg.SequenceId);
            Assert.AreEqual(expected.Timestamp, msg.Timestamp);
            Assert.AreEqual(64, msg.Data.Count);
            
            msg.Dispose();

            if (writeResults.Count == 0) {
                break;
            }
        }
    }


    [TestMethod]
    public async Task ShouldReadMessagesFromSegmentId() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await log.InitAsync();
        
        var writeResults = new List<WriteResult>();

        using var msg = new LogMessage();
        
        for (var i = 0; i < 5; i++) {
            msg.Reset();
            msg.GetSpan(64)[..64].Fill((byte) (i + 1));
            msg.Advance(64);
            writeResults.Add(await log.WriteAsync(msg));
        }

        await log.FlushAsync();
        
        var startSequenceId = writeResults[2].SequenceId;
        var expectedResults = writeResults[2..];
        
        //TestContext.CancellationTokenSource.CancelAfter(10_000);
        
        await foreach (var item in log.ReadFromPositionAsync(startSequenceId, cancellationToken: TestContext.CancellationTokenSource.Token)) {
            if (expectedResults.Count == 0) {
                Assert.Fail("No expected results available to compare with.");
            }
            
            var expected = expectedResults[0];
            expectedResults.RemoveAt(0);
            
            Assert.AreEqual(expected.SequenceId, item.SequenceId);
            Assert.AreEqual(expected.Timestamp, item.Timestamp);
            Assert.AreEqual(64, item.Data.Count);
            
            item.Dispose();

            if (expectedResults.Count == 0) {
                break;
            }
        }
    }
    
    
    [TestMethod]
    public async Task ShouldReadMessagesFromTimestamp() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await log.InitAsync();
        
        var writeResults = new List<WriteResult>();

        using var msg = new LogMessage();
        
        for (var i = 0; i < 5; i++) {
            msg.Reset();
            msg.GetSpan(64)[..64].Fill((byte) (i + 1));
            msg.Advance(64);
            writeResults.Add(await log.WriteAsync(msg));
            await Task.Delay(10);
        }

        await log.FlushAsync();
        
        var startTimestamp = writeResults[2].Timestamp;
        var expectedResults = writeResults[2..];
        
        TestContext.CancellationTokenSource.CancelAfter(10_000);
        
        await foreach (var item in log.ReadFromTimestampAsync(startTimestamp, cancellationToken: TestContext.CancellationTokenSource.Token)) {
            if (expectedResults.Count == 0) {
                Assert.Fail("No expected results available to compare with.");
            }
            
            var expected = expectedResults[0];
            expectedResults.RemoveAt(0);
            
            Assert.AreEqual(expected.SequenceId, item.SequenceId);
            Assert.AreEqual(expected.Timestamp, item.Timestamp);
            Assert.AreEqual(64, item.Data.Count);
            
            item.Dispose();

            if (expectedResults.Count == 0) {
                break;
            }
        }
    }


    [TestMethod]
    public async Task ManualRolloverShouldCreateNewSegment() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!)
        });

        await log.InitAsync();
        
        // Write a message to ensure the log has created the first segment
        using var msg = new LogMessage();
        msg.GetSpan(64)[..64].Fill(1);
        msg.Advance(64);
        await log.WriteAsync(msg);

        var segments = await log.GetSegmentsAsync(TestContext.CancellationTokenSource.Token);
        Assert.AreEqual(1, segments.Count);
        
        // Manually trigger a rollover
        await log.RolloverAsync(TestContext.CancellationTokenSource.Token);
        
        segments = await log.GetSegmentsAsync(TestContext.CancellationTokenSource.Token);
        Assert.AreEqual(2, segments.Count);
    }
    
    
    [TestMethod]
    public async Task CountBasedRolloverShouldCreateNewSegment() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!),
            MaxSegmentMessageCount = 5
        });

        await log.InitAsync();
        
        using var msg = new LogMessage();
        
        for (var i = 0; i < 9; i++) {
            msg.Reset();
            msg.GetSpan(64)[..64].Fill((byte) (i + 1));
            msg.Advance(64);
            await log.WriteAsync(msg);
        }

        var segments = await log.GetSegmentsAsync(TestContext.CancellationTokenSource.Token);
        Assert.AreEqual(2, segments.Count);
        
        // First segment should have 5 messages, second segment should have 4 messages
        var firstSegment = segments[0];
        Assert.AreEqual(5, firstSegment.MessageCount);
        Assert.AreEqual(1UL, firstSegment.FirstSequenceId);
        Assert.AreEqual(5UL, firstSegment.LastSequenceId);
        
        var secondSegment = segments[1];
        Assert.AreEqual(4, secondSegment.MessageCount);
        Assert.AreEqual(6UL, secondSegment.FirstSequenceId);
        Assert.AreEqual(9UL, secondSegment.LastSequenceId);
    }
    
    
    [TestMethod]
    public async Task SizeBasedRolloverShouldCreateNewSegment() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!),
            MaxSegmentSizeBytes = 64 + 5 * (24 + 64 + 4) // 64 bytes for segment header, 5 messages with: 24 bytes header, 64 bytes body, and 4 bytes checksum
        });

        await log.InitAsync();
        
        using var msg = new LogMessage();
        
        for (var i = 0; i < 9; i++) {
            msg.Reset();
            msg.GetSpan(64)[..64].Fill((byte) (i + 1));
            msg.Advance(64);
            await log.WriteAsync(msg);
        }

        var segments = await log.GetSegmentsAsync(TestContext.CancellationTokenSource.Token);
        Assert.AreEqual(2, segments.Count);
        
        // First segment should have 5 messages, second segment should have 4 messages
        var firstSegment = segments[0];
        Assert.AreEqual(5, firstSegment.MessageCount);
        Assert.AreEqual(1UL, firstSegment.FirstSequenceId);
        Assert.AreEqual(5UL, firstSegment.LastSequenceId);
        
        var secondSegment = segments[1];
        Assert.AreEqual(4, secondSegment.MessageCount);
        Assert.AreEqual(6UL, secondSegment.FirstSequenceId);
        Assert.AreEqual(9UL, secondSegment.LastSequenceId);
    }
    
    
    [TestMethod]
    public async Task TimeBasedRolloverShouldCreateNewSegment() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath!, TestContext!.TestName!),
            MaxSegmentTimeSpan = TimeSpan.FromSeconds(1)
        });

        await log.InitAsync();
        
        using var msg = new LogMessage();
        
        for (var i = 0; i < 5; i++) {
            msg.Reset();
            msg.GetSpan(64)[..64].Fill((byte) (i + 1));
            msg.Advance(64);
            await log.WriteAsync(msg);
        }
        
        // Wait for the segment to roll over.
        await Task.Delay(TimeSpan.FromSeconds(1));
        
        // Write more messages to the new segment

        for (var i = 5; i < 9; i++) {
            msg.Reset();
            msg.GetSpan(64)[..64].Fill((byte) (i + 1));
            msg.Advance(64);
            await log.WriteAsync(msg);
        }
        
        var segments = await log.GetSegmentsAsync(TestContext.CancellationTokenSource.Token);
        Assert.AreEqual(2, segments.Count);
        
        // First segment should have 5 messages, second segment should have 4 messages
        var firstSegment = segments[0];
        Assert.AreEqual(5, firstSegment.MessageCount);
        Assert.AreEqual(1UL, firstSegment.FirstSequenceId);
        Assert.AreEqual(5UL, firstSegment.LastSequenceId);
        
        var secondSegment = segments[1];
        Assert.AreEqual(4, secondSegment.MessageCount);
        Assert.AreEqual(6UL, secondSegment.FirstSequenceId);
        Assert.AreEqual(9UL, secondSegment.LastSequenceId);
    }

}
