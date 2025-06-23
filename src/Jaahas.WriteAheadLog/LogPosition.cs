namespace Jaahas.WriteAheadLog;

/// <summary>
/// Describes a position in the write-ahead log describes using either a sequence ID or a timestamp.
/// </summary>
public readonly record struct LogPosition {

    /// <summary>
    /// The sequence ID of the log position.
    /// </summary>
    public ulong? SequenceId { get; }
    
    /// <summary>
    /// The timestamp of the log position, measured in ticks.
    /// </summary>
    public long? Timestamp { get; }


    /// <summary>
    /// Creates a new <see cref="LogPosition"/> instance using a sequence ID.
    /// </summary>
    /// <param name="sequenceId">
    ///   The sequence ID of the log position. Specifying a value of 0 will result in a <see langword="null"/>
    ///   <see cref="SequenceId"/>.
    /// </param>
    private LogPosition(ulong sequenceId) {
        SequenceId = sequenceId == 0 
            ? null 
            : sequenceId;
    }
    
    
    /// <summary>
    /// Creates a new <see cref="LogPosition"/> instance using a timestamp.
    /// </summary>
    /// <param name="timestamp">
    ///   The timestamp of the log position, measured in ticks. Specifying a value of 0 or less
    ///   will result in a <see langword="null"/> <see cref="SequenceId"/>.
    /// </param>
    private LogPosition(long timestamp) {
        Timestamp = timestamp;
    }
    
    
    /// <summary>
    /// Creates a new <see cref="LogPosition"/> instance using a sequence ID.
    /// </summary>
    /// <param name="sequenceId">
    ///   The sequence ID of the log position. Specifying a value of 0 will result in a <see langword="null"/>
    ///   <see cref="SequenceId"/>.
    /// </param>
    /// <returns>
    ///   A new <see cref="LogPosition"/> instance with the specified sequence ID.
    /// </returns>
    public static LogPosition FromSequenceId(ulong sequenceId) => new LogPosition(sequenceId);
    
    
    /// <summary>
    /// Creates a new <see cref="LogPosition"/> instance using a timestamp.
    /// </summary>
    /// <param name="timestamp">
    ///   The timestamp of the log position, measured in ticks. Specifying a value of 0 or less
    ///   will result in a <see langword="null"/> <see cref="SequenceId"/>.
    /// </param>
    /// <returns>
    ///   A new <see cref="LogPosition"/> instance with the specified timestamp.
    /// </returns>
    public static LogPosition FromTimestamp(long timestamp) => new LogPosition(timestamp);
    
    
    /// <summary>
    /// Creates a new <see cref="LogPosition"/> instance using a sequence ID.
    /// </summary>
    /// <param name="sequenceId">
    ///   The sequence ID of the log position. Specifying a value of 0 will result in a <see langword="null"/>
    ///   <see cref="SequenceId"/>.
    /// </param>
    /// <returns>
    ///   A new <see cref="LogPosition"/> instance with the specified sequence ID.
    /// </returns>
    public static implicit operator LogPosition(ulong sequenceId) => FromSequenceId(sequenceId);
    
    
    /// <summary>
    /// Creates a new <see cref="LogPosition"/> instance using a timestamp.
    /// </summary>
    /// <param name="timestamp">
    ///   The timestamp of the log position, measured in ticks. Specifying a value of 0 or less
    ///   will result in a <see langword="null"/> <see cref="SequenceId"/>.
    /// </param>
    /// <returns>
    ///   A new <see cref="LogPosition"/> instance with the specified timestamp.
    /// </returns>
    public static implicit operator LogPosition(long timestamp) => FromTimestamp(timestamp);

}
