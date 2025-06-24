namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// An entry in the write-ahead log's sparse message index.
/// </summary>
/// <param name="SequenceId">
///   The sequence ID of the message.
/// </param>
/// <param name="Timestamp">
///   The timestamp of the message.
/// </param>
/// <param name="Offset">
///   The offset of the message within the segment, in bytes.
/// </param>
public readonly record struct MessageIndexEntry(ulong SequenceId, long Timestamp, long Offset);
