namespace Jaahas.WriteAheadLog.FileSystem.Internal;

internal record SegmentWriterOptions(string Name, DateTimeOffset? NotAfter = null);
