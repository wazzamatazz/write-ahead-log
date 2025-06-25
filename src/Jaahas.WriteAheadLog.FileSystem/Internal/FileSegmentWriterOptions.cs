namespace Jaahas.WriteAheadLog.FileSystem.Internal;

internal record FileSegmentWriterOptions(
    string Name,
    string FilePath,
    DateTimeOffset? NotAfter = null, 
    TimeSpan? FlushInterval = null, 
    int FlushBatchSize = -1
) : SegmentWriterOptions(Name, NotAfter);
