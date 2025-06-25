namespace Jaahas.WriteAheadLog.FileSystem;

/// <summary>
/// Describes the header of a segment in the Write-Ahead Log (WAL).
/// </summary>
public sealed record SegmentHeader {

    /// <summary>
    /// The version of the segment header format.
    /// </summary>
    public uint Version { get; internal init; }

    /// <summary>
    /// The first sequence ID in the segment.
    /// </summary>
    public ulong FirstSequenceId { get; internal set; }
    
    /// <summary>
    /// The last sequence ID in the segment.
    /// </summary>
    public ulong LastSequenceId { get; internal set; }
    
    /// <summary>
    /// The first timestamp in the segment, measured in ticks.
    /// </summary>
    public long FirstTimestamp { get; internal set; }
    
    /// <summary>
    /// The last timestamp in the segment, measured in ticks.
    /// </summary>
    public long LastTimestamp { get; internal set; }
    
    /// <summary>
    /// The number of messages in the segment.
    /// </summary>
    public long MessageCount { get; internal set; }
    
    /// <summary>
    /// The size of the segment in bytes, including the header and all messages.
    /// </summary>
    public long Size { get; internal set; }
    
    /// <summary>
    /// Specifies if the segment is read-only.
    /// </summary>
    public bool ReadOnly { get; internal set; }
    
    
    /// <summary>
    /// Creates a new <see cref="SegmentHeader"/> instance.
    /// </summary>
    public SegmentHeader() {
        Version = 1; // Current version
    }


    /// <summary>
    /// Creates a new <see cref="SegmentHeader"/> instance by copying values from another instance.
    /// </summary>
    /// <param name="other">
    ///   The <see cref="SegmentHeader"/> instance to copy values from.
    /// </param>
    /// <returns>
    ///   A new <see cref="SegmentHeader"/> instance with values copied from the specified instance.
    /// </returns>
    internal static SegmentHeader CopyFrom(SegmentHeader other) {
        return new SegmentHeader(other);
    }

}
