namespace Jaahas.WriteAheadLog.Internal;

internal record SegmentWriterOptions(string Name, DateTimeOffset? NotAfter = null);
