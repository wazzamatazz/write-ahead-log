namespace Jaahas.WriteAheadLog;

public sealed record SegmentHeader {

    public uint Version { get; internal init; }

    public ulong FirstSequenceId { get; internal set; }
    
    public ulong LastSequenceId { get; internal set; }
    
    public long FirstTimestamp { get; internal set; }
    
    public long LastTimestamp { get; internal set; }
    
    public long MessageCount { get; internal set; }
    
    public long Size { get; internal set; }
    
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
