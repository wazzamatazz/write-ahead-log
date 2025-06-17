namespace Jaahas.WriteAheadLog;

/// <summary>
/// Options for <see cref="Log"/>.
/// </summary>
public record LogOptions {

    /// <summary>
    /// The directory where the write-ahead log segments will be stored.
    /// </summary>
    public string DataDirectory { get; init; } = "wal";

    /// <summary>
    /// Maximum number of messages a single log segment can hold.
    /// </summary>
    public long MaxSegmentMessageCount { get; init; } = -1;
    
    /// <summary>
    /// Maximum size in bytes for a single log segment.
    /// </summary>
    public long MaxSegmentSizeBytes { get; init; } = 64 * 1024 * 1024; // 64 MB default

    /// <summary>
    /// Maximum time period a segment can contain log messages for.
    /// </summary>
    /// <remarks>
    ///   Specifying a value less than or equal to <see cref="TimeSpan.Zero"/> will disable
    ///   time-based rollover. Positive values less than one second are rounded up to one second.
    /// </remarks>
    public TimeSpan MaxSegmentTimeSpan { get; init; } = TimeSpan.FromDays(1);
    
    /// <summary>
    /// The interval at which the log will flush messages to disk.
    /// </summary>
    /// <remarks>
    ///   Specifying an interval less than or equal to <see cref="TimeSpan.Zero"/> will disable
    ///   background flushing. When background flushing is disabled, flushes will only occur when
    ///   the segment is closed or disposed, or when <see cref="Log.FlushAsync"/> is called.
    /// </remarks>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromSeconds(1);
    
    /// <summary>
    /// The maximum number of messages to write before automatically flushing the segment to
    /// disk.
    /// </summary>
    /// <remarks>
    ///   Specifying a value less than or equal to zero will disable automatic flushing based on
    ///   the number of messages written.
    /// </remarks>
    public int FlushBatchSize { get; init; } = 100;
    
    /// <summary>
    /// The number of messages between sparse index entries.
    /// </summary>
    public int SparseIndexInterval { get; init; } = 500;

}
