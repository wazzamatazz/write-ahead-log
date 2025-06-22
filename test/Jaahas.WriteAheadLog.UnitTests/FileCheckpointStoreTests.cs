namespace Jaahas.WriteAheadLog.UnitTests;

[TestClass]
public class FileCheckpointStoreTests {

    private static string s_tempPath = null!;

    public TestContext TestContext { get; set; } = null!;
    
    public CancellationToken TestCancellationToken => TestContext?.CancellationTokenSource?.Token ?? CancellationToken.None;


    [ClassInitialize]
    public static void GlobalInitialize(TestContext context) {
        s_tempPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        new DirectoryInfo(s_tempPath).Create();
    }


    [ClassCleanup]
    public static void GlobalCleanup() {
        if (!string.IsNullOrWhiteSpace(s_tempPath) && Directory.Exists(s_tempPath)) {
            Directory.Delete(s_tempPath, true);
        }
    }


    [TestMethod]
    public async Task CanWriteAndReadCheckpoint() {
        var options = new FileCheckpointStoreOptions { DataDirectory = s_tempPath, Name = TestContext.TestName! };
        await using var store = new FileCheckpointStore(options);
        
        await store.SaveCheckpointAsync(12345, TestCancellationToken).ConfigureAwait(false);
        var value = await store.LoadCheckpointAsync(TestCancellationToken).ConfigureAwait(false);
        
        Assert.AreEqual(12345UL, value);
    }


    [TestMethod]
    public async Task ReturnsZeroIfNoCheckpointExists() {
        var options = new FileCheckpointStoreOptions { DataDirectory = s_tempPath, Name = TestContext.TestName! };
        await using var store = new FileCheckpointStore(options);
        
        var value = await store.LoadCheckpointAsync(TestCancellationToken).ConfigureAwait(false);
        
        Assert.AreEqual(0UL, value);
    }
    
    
    [TestMethod]
    public async Task CheckpointIsPersistedAfterRestart() {
        var options = new FileCheckpointStoreOptions { DataDirectory = s_tempPath, Name = TestContext.TestName! };
        var store = new FileCheckpointStore(options);

        await store.SaveCheckpointAsync(12345, TestCancellationToken);
        await store.DisposeAsync();
        
        store = new FileCheckpointStore(options);
        
        var value = await store.LoadCheckpointAsync(TestCancellationToken).ConfigureAwait(false);
        Assert.AreEqual(12345UL, value);
    }
    
    
    [TestMethod]
    public async Task CheckpointIsManuallyPersisted() {
        var options = new FileCheckpointStoreOptions {
            DataDirectory = s_tempPath,
            Name = TestContext.TestName!,
            FlushInterval = TimeSpan.Zero // Disable automatic flushing
        };
        var store = new FileCheckpointStore(options);

        var file = new FileInfo(Path.Combine(s_tempPath, TestContext.TestName!));
        Assert.IsTrue(file.Exists);
        
        var lastModifiedTime = file.LastWriteTimeUtc;
        await store.SaveCheckpointAsync(12345, TestCancellationToken);
        await store.FlushAsync(TestCancellationToken);

        file.Refresh();
        var lastModifiedTimeAfterFlush = file.LastWriteTimeUtc;
        
        Assert.IsTrue(lastModifiedTimeAfterFlush > lastModifiedTime);
    }
    
    
    [TestMethod]
    public async Task CheckpointIsAutomaticallyPersisted() {
        var options = new FileCheckpointStoreOptions {
            DataDirectory = s_tempPath,
            Name = TestContext.TestName!,
            FlushInterval = TimeSpan.FromMilliseconds(50)
        };
        var store = new FileCheckpointStore(options);

        var file = new FileInfo(Path.Combine(s_tempPath, TestContext.TestName!));
        Assert.IsTrue(file.Exists);
        
        await store.SaveCheckpointAsync(12345, TestCancellationToken);

        TestContext.CancellationTokenSource.CancelAfter(10_000);
        await store.WaitForFlushAsync(TestCancellationToken);
    }
    
    
    [TestMethod]
    public async Task SaveCheckpointFailsAfterDispose() {
        var options = new FileCheckpointStoreOptions { DataDirectory = s_tempPath, Name = TestContext.TestName! };
        var store = new FileCheckpointStore(options);

        await store.DisposeAsync();
        
        await Assert.ThrowsAsync<ObjectDisposedException>(() => store.SaveCheckpointAsync(12345, TestCancellationToken).AsTask());
    }
    
    
    [TestMethod]
    public async Task LoadCheckpointFailsAfterDispose() {
        var options = new FileCheckpointStoreOptions { DataDirectory = s_tempPath, Name = TestContext.TestName! };
        var store = new FileCheckpointStore(options);

        await store.DisposeAsync();
        
        await Assert.ThrowsAsync<ObjectDisposedException>(() => store.LoadCheckpointAsync(TestCancellationToken).AsTask());
    }
    
    
    [TestMethod]
    public async Task FlushFailsAfterDispose() {
        var options = new FileCheckpointStoreOptions { DataDirectory = s_tempPath, Name = TestContext.TestName! };
        var store = new FileCheckpointStore(options);

        await store.DisposeAsync();
        
        await Assert.ThrowsAsync<ObjectDisposedException>(() => store.FlushAsync(TestCancellationToken).AsTask());
    }

}

