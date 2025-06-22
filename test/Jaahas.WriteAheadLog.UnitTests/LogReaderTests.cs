using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public class LogReaderTests {

    private static string s_tempPath = null!;
    
    private static IServiceProvider s_serviceProvider = null!;

    public TestContext TestContext { get; set; } = null!;


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
    public async Task ShouldReadLogEntries() {
        await using var log = ActivatorUtilities.CreateInstance<Log>(s_serviceProvider, new LogOptions() {
            DataDirectory = Path.Combine(s_tempPath, TestContext.TestName!)
        });

        var checkpointStore = new InMemoryCheckpointStore();
        
        await using var reader = ActivatorUtilities.CreateInstance<LogReader>(s_serviceProvider, log, checkpointStore);

        var entryCount = 0;
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        
        reader.ProcessEntry += _ => {
            entryCount++;
            if (entryCount == 5) {
                tcs.SetResult();
            }
            return Task.CompletedTask;
        };

        await reader.StartAsync();

        await using var writer = new JsonLogWriter();
        var rnd = new Random();
        
        // Simulate writing entries to the log
        for (var i = 0; i < 5; i++) {
            await writer.WriteToLogAsync(log, rnd.NextDouble());
        }

        // Wait for the reader to process the entries
        TestContext.CancellationTokenSource.CancelAfter(10_000);
        await tcs.Task;

        Assert.AreEqual(5, entryCount);
    }


    private class InMemoryCheckpointStore : ICheckpointStore {

        private ulong _checkpoint = 0;


        public ValueTask<ulong> LoadCheckpointAsync(CancellationToken cancellationToken = default) {
            return ValueTask.FromResult(_checkpoint);
        }


        public ValueTask SaveCheckpointAsync(ulong sequenceId, CancellationToken cancellationToken = default) {
            _checkpoint = sequenceId;
            return ValueTask.CompletedTask;
        }

    }

}
