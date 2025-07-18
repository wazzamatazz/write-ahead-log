using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnostics.dotMemory;

using Jaahas.WriteAheadLog.FileSystem;

namespace Jaahas.WriteAheadLog.Benchmarks;

[MemoryDiagnoser]
// [DotMemoryDiagnoser]
public class LogWriteBenchmarks {
    
    private DirectoryInfo _dataDirectory = null!;
    
    private DirectoryInfo _iterationDirectory = null!;
    
    
    [GlobalSetup]
    public void GlobalSetup() {
        _dataDirectory = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()));
        _dataDirectory.Create();
    }


    [GlobalCleanup]
    public void GlobalCleanup() {
        if (_dataDirectory?.Exists ?? false) {
            _dataDirectory.Delete(true);
        }
    }
    
    
    [IterationSetup]
    public void IterationSetup() {
        _iterationDirectory = new DirectoryInfo(Path.Combine(_dataDirectory.FullName, Guid.NewGuid().ToString()));
        _iterationDirectory.Create();
    }


    [Benchmark]
    public async Task WriteToLog() {
        await using var log = new FileSystem.FileWriteAheadLog(new FileWriteAheadLogOptions() {
            DataDirectory = _iterationDirectory.FullName,
            FlushInterval = TimeSpan.Zero,
            FlushBatchSize = 1_000,
            MaxSegmentSizeBytes = -1,
            SparseIndexInterval = -1
        });
        
        await log.InitAsync();
        await using var publisher = new LogWriter();
        
        for (var i = 1; i <= 100_000; i++) {
            publisher.GetSpan(256)[..256].Fill(1);
            publisher.Advance(256);
            await publisher.WriteToLogAsync(log);
        }
    }

}
