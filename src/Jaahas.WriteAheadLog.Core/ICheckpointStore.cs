namespace Jaahas.WriteAheadLog;

/// <summary>
/// Provides an abstraction for persisting and retrieving a log reader's progress.
/// </summary>
public interface ICheckpointStore {

    /// <summary>
    /// Persists the checkpoint sequence ID for the log reader.
    /// </summary>
    /// <param name="sequenceId">
    ///   The sequence ID to persist as the checkpoint.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="ValueTask"/> representing the asynchronous operation.
    /// </returns>
    ValueTask SaveCheckpointAsync(ulong sequenceId, CancellationToken cancellationToken = default);


    /// <summary>
    /// Retrieves the last persisted checkpoint (sequence ID) for the log reader.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   The last persisted sequence ID, or zero if no checkpoint exists.
    /// </returns>
    ValueTask<ulong> LoadCheckpointAsync(CancellationToken cancellationToken = default);

}
