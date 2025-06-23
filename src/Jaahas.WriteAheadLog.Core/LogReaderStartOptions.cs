namespace Jaahas.WriteAheadLog;

/// <summary>
/// Options for <see cref="LogReader.StartAsync"/>.
/// </summary>
/// <param name="Position">
///   The position in the log to start reading from.
/// </param>
/// <param name="StartBehaviour">
///   Specifies if the reader should use the last persisted checkpoint position if available
///   instead of the provided <paramref name="Position"/>.
/// </param>
public readonly record struct LogReaderStartOptions(LogPosition Position = default, LogReaderStartBehaviour StartBehaviour = LogReaderStartBehaviour.UseCheckpointIfAvailable) {

    /// <summary>
    /// Creates a new <see cref="LogReaderStartOptions"/> instance.
    /// </summary>
    /// <param name="position">
    ///   The position in the log to start reading from.
    /// </param>
    /// <returns>
    ///   A new <see cref="LogReaderStartOptions"/> instance with the specified position that will
    ///   use an existing checkpoint instead of the <paramref name="position"/> if available.
    /// </returns>
    public static implicit operator LogReaderStartOptions(LogPosition position) => new LogReaderStartOptions(position);

}


/// <summary>
/// Specifies how a <see cref="LogReader"/> should start treat the provided <see cref="LogReaderStartOptions.Position"/>
/// when starting the reader.
/// </summary>
public enum LogReaderStartBehaviour {
    
    /// <summary>
    /// The reader should use an existing checkpoint if available, otherwise it will use the
    /// provided <see cref="LogReaderStartOptions.Position"/>.
    /// </summary>
    UseCheckpointIfAvailable,

    /// <summary>
    /// The reader should always use the provided <see cref="LogReaderStartOptions.Position"/> to
    /// start reading from the log, even if a checkpoint is available.
    /// </summary>
    OverrideCheckpoint

}
