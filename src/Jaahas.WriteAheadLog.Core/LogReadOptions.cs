namespace Jaahas.WriteAheadLog;

/// <summary>
/// Options for reading from the Write-Ahead Log (WAL).
/// </summary>
/// <param name="Position">
///   The position in the log to start reading from. This can be specified using either a sequence
///   ID or a timestamp.
/// </param>
/// <param name="Limit">
///   The maximum number of messages to read.
/// </param>
/// <param name="WatchForChanges">
///   When <see langword="true"/>, the operation will wait for new log entries to be added to
///   the latest segment when it reaches the end of the log stream. Otherwise, the operation
///   will complete when is reaches the end of the latest segment.
/// </param>
public readonly record struct LogReadOptions(LogPosition Position = default, long Limit = -1, bool WatchForChanges = false);
