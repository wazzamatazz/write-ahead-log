using BenchmarkDotNet.Attributes;

namespace Jaahas.WriteAheadLog.Benchmarks;

[MemoryDiagnoser]
public class SparseIndexBenchmarks {
    
    private const int MessageCount = 1_000_000;

    private const ulong SeekSequenceId = MessageCount / 2;

    private DirectoryInfo _dataDirectory = null!;

    private FileWriteAheadLog _log = null!;
    
    [Params(500, 1000, 10_000, 50_000, 100_000)]
    public int SparseIndexInterval { get; set; }
    
    
    [GlobalSetup]
    public async Task GlobalSetup() {
        _dataDirectory = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()));
        _dataDirectory.Create();
        
        _log = new FileWriteAheadLog(new FileWriteAheadLogOptions() {
            DataDirectory = _dataDirectory.FullName,
            FlushInterval = TimeSpan.Zero,
            FlushBatchSize = 1_000,
            MaxSegmentSizeBytes = -1,
            MaxSegmentMessageCount = -1,
            MaxSegmentTimeSpan = TimeSpan.Zero,
            SparseIndexInterval = SparseIndexInterval
        });
        
        await _log.InitAsync();
        await using var publisher = new LogWriter();
        
        for (var i = 1; i <= MessageCount; i++) {
            publisher.GetSpan(256)[..256].Fill(1);
            publisher.Advance(256);
            await publisher.WriteToLogAsync(_log);
        }

        await _log.RolloverAsync();
    }


    [GlobalCleanup]
    public void GlobalCleanup() {
        if (_log is not null) {
            _log.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _log = null!;
        }

        if (_dataDirectory?.Exists ?? false) {
            _dataDirectory.Delete(true);
        }
    }
    
    
    [Benchmark]
    public async Task GetMessageUsingSparseIndex() {
        await foreach (var item in _log.ReadAllAsync(position: SeekSequenceId, count: 1)) {
            item.Dispose();
            break;
        }
    }

}
