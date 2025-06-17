namespace Jaahas.WriteAheadLog;

public readonly record struct WriteResult(ulong SequenceId, long Timestamp);
