using Jaahas.WriteAheadLog.Internal;

namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public class FileSegmentReaderTests {

    private static string? s_tempPath;

    public TestContext? TestContext { get; set; }


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


    [TestMethod]
    public async Task ShouldReadAllMessages() {
        var filePath = Path.Combine(s_tempPath!, $"{TestContext!.TestName}.wal");
        await using var segmentWriter = new FileSegmentWriter(new FileSegmentWriterOptions(TestContext.TestName!, filePath, null, TimeSpan.FromSeconds(1)));

        using var msg1 = new LogMessage(Enumerable.Repeat((byte) 1, 64).ToArray());
        await segmentWriter.WriteAsync(msg1, 1);

        await Task.Delay(10);
        
        using var msg2 = new LogMessage(Enumerable.Repeat((byte) 2, 64).ToArray());
        await segmentWriter.WriteAsync(msg2, 2);
        
        await segmentWriter.DisposeAsync();

        TestContext.CancellationTokenSource.CancelAfter(10_000);

        ulong expectedSequenceId = 0;
        
        await foreach (var item in new FileSegmentReader().ReadLogEntriesAsync(filePath, TestContext.CancellationTokenSource.Token)) {
            ++expectedSequenceId;
            try {
                Assert.AreEqual(expectedSequenceId, item.SequenceId);
                Assert.AreEqual(64, item.Data.Count);
                Assert.IsTrue(item.Data.SequenceEqual(Enumerable.Repeat((byte) item.SequenceId, 64)));
            }
            finally {
                item.Dispose();
            }
            
            if (item.SequenceId == 2) {
                break;
            }
        }

        Assert.AreEqual((ulong) 2, expectedSequenceId);
    }

}
