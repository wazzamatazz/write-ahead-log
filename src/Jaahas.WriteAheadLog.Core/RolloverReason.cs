namespace Jaahas.WriteAheadLog;

/// <summary>
/// Describes the reason for a segment rollover in the Write-Ahead Log (WAL).
/// </summary>
public enum RolloverReason {

    /// <summary>
    /// Rollover was manually triggered.
    /// </summary>
    Manual,
    
    /// <summary>
    /// The Write-Ahead Log (WAL) has no writable segments available.
    /// </summary>
    NoWritableSegments,
    
    /// <summary>
    /// The segment size limit has been reached.
    /// </summary>
    SegmentSizeLimitReached,
    
    /// <summary>
    /// The segment time limit has been reached.
    /// </summary>
    SegmentTimeLimitReached,
    
    /// <summary>
    /// The message count limit for the segment has been reached.
    /// </summary>
    SegmentMessageCountLimitReached

}
