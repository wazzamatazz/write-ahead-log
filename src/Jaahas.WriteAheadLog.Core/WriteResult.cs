namespace Jaahas.WriteAheadLog;

/// <summary>
/// Describes the result of a write operation to the Write-Ahead Log (WAL).
/// </summary>
/// <param name="SequenceId">
///   The sequence ID of the message that was written.
/// </param>
/// <param name="Timestamp">
///   The timestamp of the message that was written, measured in ticks.
/// </param>
public readonly record struct WriteResult(ulong SequenceId, long Timestamp);
