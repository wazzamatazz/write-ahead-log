namespace Jaahas.WriteAheadLog;

/// <summary>
/// Options for <see cref="Log"/>.
/// </summary>
public class LogOptions {

    /// <summary>
    /// The directory where the write-ahead log segments will be stored.
    /// </summary>
    public string DataDirectory { get; set; } = "wal";

    /// <summary>
    /// Maximum number of messages a single log segment can hold.
    /// </summary>
    public long MaxSegmentMessageCount { get; set; } = -1;
    
    /// <summary>
    /// Maximum size in bytes for a single log segment.
    /// </summary>
    public long MaxSegmentSizeBytes { get; set; } = 64 * 1024 * 1024; // 64 MB default

    /// <summary>
    /// Maximum time period a segment can contain log messages for.
    /// </summary>
    /// <remarks>
    ///   Specifying a value less than or equal to <see cref="TimeSpan.Zero"/> will disable
    ///   time-based rollover. Positive values less than one second are rounded up to one second.
    /// </remarks>
    public TimeSpan MaxSegmentTimeSpan { get; set; } = TimeSpan.FromDays(1);
    
    /// <summary>
    /// The interval at which the log will flush messages to disk.
    /// </summary>
    /// <remarks>
    ///   Specifying an interval less than or equal to <see cref="TimeSpan.Zero"/> will disable
    ///   background flushing. When background flushing is disabled, flushes will only occur when
    ///   the segment is closed or disposed, or when <see cref="Log.FlushAsync"/> is called.
    /// </remarks>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(1);
    
    /// <summary>
    /// The maximum number of messages to write before automatically flushing the segment to
    /// disk.
    /// </summary>
    /// <remarks>
    ///   Specifying a value less than or equal to zero will disable automatic flushing based on
    ///   the number of messages written.
    /// </remarks>
    public int FlushBatchSize { get; set; } = 100;
    
    /// <summary>
    /// The number of messages between sparse index entries.
    /// </summary>
    public int SparseIndexInterval { get; set; } = 500;
    
    /// <summary>
    /// The interval at which log readers will poll for new messages when reading from the active
    /// writer segment when change detection is requested for the operation (i.e. when
    /// <see cref="LogReadOptions.WatchForChanges"/> is <see langword="true"/>).
    /// </summary>
    public TimeSpan ReadPollingInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// The interval at which old WAL segment files will be checked and cleaned up.
    /// </summary>
    /// <remarks>
    ///   Specifying a value less than or equal to <see cref="TimeSpan.Zero"/> disables periodic cleanup.
    /// </remarks>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// The maximum age for a WAL segment file before it is eligible for deletion.
    /// </summary>
    /// <remarks>
    ///   Specifying a value less than or equal to <see cref="TimeSpan.Zero"/> disables age-based cleanup.
    /// </remarks>
    public TimeSpan SegmentRetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// The maximum number of WAL segment files to retain. Older segments will be deleted if this count is exceeded.
    /// </summary>
    /// <remarks>
    ///   Specifying a value less than or equal to zero disables count-based cleanup.
    /// </remarks>
    public int MaxSegmentCount { get; set; } = 0;

}
