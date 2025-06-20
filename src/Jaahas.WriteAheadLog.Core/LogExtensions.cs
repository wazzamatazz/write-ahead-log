using System.Runtime.CompilerServices;

namespace Jaahas.WriteAheadLog;

public static class LogExtensions {

    public static async IAsyncEnumerable<LogEntry> ReadFromPositionAsync(this Log log, ulong sequenceId = 0, long count = -1, bool watchForChanges = false, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(log);
        await foreach (var entry in log.ReadAllAsync(new LogReadOptions(SequenceId: sequenceId, Count: count, WatchForChanges: watchForChanges), cancellationToken).ConfigureAwait(false)) {
            yield return entry;
        }
    }
    
    
    public static async IAsyncEnumerable<LogEntry> ReadFromTimestampAsync(this Log log, long timestamp = -1, long count = -1, bool watchForChanges = false, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        ArgumentNullException.ThrowIfNull(log);
        await foreach (var entry in log.ReadAllAsync(new LogReadOptions(Timestamp: timestamp, Count: count, WatchForChanges: watchForChanges), cancellationToken).ConfigureAwait(false)) {
            yield return entry;
        }
    }

}
