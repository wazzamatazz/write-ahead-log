namespace Jaahas.WriteAheadLog;

/// <summary>
/// Options for reading from the Write-Ahead Log (WAL).
/// </summary>
/// <param name="SequenceId">
///   The sequence ID to start reading from.
/// </param>
/// <param name="Timestamp">
///   The timestamp to start reading from, measured in ticks.
/// </param>
/// <param name="Count">
///   The maximum number of messages to read.
/// </param>
/// <param name="WatchForChanges">
///   When <see langword="true"/>, the operation will wait for new log entries to be added to
///   the latest segment when it reaches the end of the log stream. Otherwise, the operation
///   will complete when is reaches the end of the latest segment.
/// </param>
/// <remarks>
///   If both <paramref name="SequenceId"/> and <paramref name="Timestamp"/> are specified,
///   <paramref name="Timestamp"/> takes precedence.
/// </remarks>
public readonly record struct LogReadOptions(ulong SequenceId = 0UL, long Timestamp = -1, long Count = -1, bool WatchForChanges = false);
