namespace Jaahas.WriteAheadLog;

/// <summary>
/// Base options for configuring a write-ahead log.
/// </summary>
public class WriteAheadLogOptions {
    
    /// <summary>
    /// The name of the write-ahead log.
    /// </summary>
    public string? Name { get; set; }
    
    /// <summary>
    /// The description for the write-ahead log.
    /// </summary>
    public string? Description { get; set; }
    
    /// <summary>
    /// The maximum size in bytes for a single log entry payload.
    /// </summary>
    /// <remarks>
    ///   Specify a value less than or equal to zero to disable the size limit.
    /// </remarks>
    public long MaxEntryPayloadSize { get; set; } = -1;

}
