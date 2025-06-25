using Jaahas.WriteAheadLog.FileSystem.Internal;

using FileSegmentWriter = Jaahas.WriteAheadLog.FileSystem.Internal.FileSegmentWriter;

namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public sealed class FileSegmentWriterTests {

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
    public async Task ShouldWriteSegmentHeaderToNewSegment() {
        var filePath = Path.Combine(s_tempPath!, $"{TestContext!.TestName}.wal");
        var segmentWriter = new FileSegmentWriter(new FileSegmentWriterOptions(TestContext.TestName!, filePath, null, TimeSpan.FromSeconds(1)));
        await segmentWriter.DisposeAsync();
        
        Assert.IsTrue(File.Exists(filePath), "Segment file should be created.");
        var file = new FileInfo(filePath);
        Assert.AreEqual(SegmentWriter.SerializedSegmentHeaderSize, file.Length, $"Segment file should have a size of {SegmentWriter.SerializedSegmentHeaderSize} bytes (header size).");

        await using var fileStream = file.OpenRead();
        var header = SegmentWriter.ReadHeaderFromStream(fileStream);
        Assert.IsNotNull(header, "Segment header should be read successfully.");
    }


    [TestMethod]
    public async Task ShouldUpdateSegmentHeaderAfterWrite() {
        var filePath = Path.Combine(s_tempPath!, $"{TestContext!.TestName}.wal");
        await using var segmentWriter = new FileSegmentWriter(new FileSegmentWriterOptions(TestContext.TestName!, filePath, null, TimeSpan.FromSeconds(1)));
        
        await segmentWriter.WriteAsync(Enumerable.Repeat((byte) 1, 64).ToArray(), 1);

        await Task.Delay(10);
        
        await segmentWriter.WriteAsync(Enumerable.Repeat((byte) 2, 64).ToArray(), 2);
        
        Assert.AreEqual(2u, segmentWriter.Header.MessageCount);
        // 24 byte header + 64 byte body + 4 byte checksum x 2 messages
        Assert.AreEqual(2u * (24 + 64 + 4), segmentWriter.Header.Size);
        Assert.AreEqual(1u, segmentWriter.Header.FirstSequenceId);
        Assert.AreEqual(2u, segmentWriter.Header.LastSequenceId);
        Assert.IsTrue(segmentWriter.Header.FirstTimestamp > 0);
        Assert.IsTrue(segmentWriter.Header.LastTimestamp > segmentWriter.Header.FirstTimestamp);
        
        var writerHeader = segmentWriter.Header;
        await segmentWriter.DisposeAsync();

        var file = new FileInfo(filePath);
        await using var fileStream = file.OpenRead();
        var readerHeader = SegmentWriter.ReadHeaderFromStream(fileStream);

        Assert.AreEqual(writerHeader, readerHeader);
        Assert.AreEqual(file.Length - SegmentWriter.SerializedSegmentHeaderSize, readerHeader.Size, "File size should match the header size plus recorded message sizes.");
    }
    
}
