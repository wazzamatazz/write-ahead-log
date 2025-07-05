namespace Jaahas.WriteAheadLog;

/// <summary>
/// Metadata associated with an <see cref="IWriteAheadLog"/>.
/// </summary>
public record WriteAheadLogMetadata {
    
    /// <summary>
    /// The name of the log.
    /// </summary>
    public required string Name { get; init; }
    
    /// <summary>
    /// The description for the log.
    /// </summary>
    public string? Description { get; init; }
    
    /// <summary>
    /// The maximum size in bytes for a single log entry.
    /// </summary>
    public long MaxEntryPayloadSize { get; init; } = -1;

}
