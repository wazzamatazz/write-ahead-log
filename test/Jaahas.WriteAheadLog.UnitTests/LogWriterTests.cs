using System.Text.Json;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public class LogWriterTests {

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
    public async Task ShouldWriteJsonMessage() {
        await using var log = ActivatorUtilities.CreateInstance<FileWriteAheadLog>(s_serviceProvider, new FileWriteAheadLogOptions() {
            DataDirectory = Path.Combine(s_tempPath, TestContext.TestName!)
        });
        
        await using var writer = new JsonLogWriter();

        var payload = JsonDocument.Parse("""{ "Message": "Hello, world!" }""").RootElement;
        await writer.WriteToLogAsync(log, payload);
        
        var segments = await log.GetSegmentsAsync();
        Assert.AreEqual(1, segments.Count);
        Assert.AreEqual(1, segments[0].MessageCount);

        TestContext.CancellationTokenSource.CancelAfter(10_000);
        
        await foreach (var item in log.ReadAllAsync(new LogReadOptions(Limit: 1), TestContext.CancellationTokenSource.Token)) {
            var json = JsonDocument.Parse(item.Data).RootElement;
            item.Dispose();
            
            Assert.IsTrue(json.TryGetProperty("Message", out var message));
            Assert.AreEqual(JsonValueKind.String, message.ValueKind);
            Assert.AreEqual("Hello, world!", message.GetString());
        }
    }
    
    
    [TestMethod]
    public async Task ShouldWriteMultipleJsonMessages() {
        await using var log = ActivatorUtilities.CreateInstance<FileWriteAheadLog>(s_serviceProvider, new FileWriteAheadLogOptions() {
            DataDirectory = Path.Combine(s_tempPath, TestContext.TestName!)
        });
        
        await using var writer = new JsonLogWriter();

        for (var i = 0; i < 10; i++) {
            var payload = JsonDocument.Parse($@"{{ ""Message"": ""Hello, world! {i + 1}"" }}").RootElement;
            await writer.WriteToLogAsync(log, payload);
        }

        var segments = await log.GetSegmentsAsync();
        Assert.AreEqual(1, segments.Count);
        Assert.AreEqual(10, segments[0].MessageCount);

        TestContext.CancellationTokenSource.CancelAfter(10_000);

        var count = 0;
        
        await foreach (var item in log.ReadAllAsync(new LogReadOptions(Limit: 10), TestContext.CancellationTokenSource.Token)) {
            ++count;
            var json = JsonDocument.Parse(item.Data).RootElement;
            item.Dispose();
            
            Assert.IsTrue(json.TryGetProperty("Message", out var message));
            Assert.AreEqual(JsonValueKind.String, message.ValueKind);
            Assert.AreEqual("Hello, world! " + count, message.GetString());
        }
    }

}
