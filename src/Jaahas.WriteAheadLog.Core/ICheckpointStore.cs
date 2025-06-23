namespace Jaahas.WriteAheadLog;

/// <summary>
/// Provides an abstraction for persisting and retrieving a log reader's progress.
/// </summary>
public interface ICheckpointStore {

    /// <summary>
    /// Persists the checkpoint position for the log reader.
    /// </summary>
    /// <param name="position">
    ///   The log position to persist as the checkpoint.
    /// </param>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   A <see cref="ValueTask"/> representing the asynchronous operation.
    /// </returns>
    ValueTask SaveCheckpointAsync(LogPosition position, CancellationToken cancellationToken = default);


    /// <summary>
    /// Retrieves the last persisted checkpoint position for the log reader.
    /// </summary>
    /// <param name="cancellationToken">
    ///   The cancellation token for the operation.
    /// </param>
    /// <returns>
    ///   The last persisted log position.
    /// </returns>
    ValueTask<LogPosition> LoadCheckpointAsync(CancellationToken cancellationToken = default);

}
