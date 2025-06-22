namespace Jaahas.WriteAheadLog;

/// <summary>
/// Options for <see cref="FileCheckpointStore"/>.
/// </summary>
public class FileCheckpointStoreOptions {

    /// <summary>
    /// The directory where the checkpoint file will be stored.
    /// </summary>
    public string DataDirectory { get; set; } = ".wal-checkpoints";

    /// <summary>
    /// The name of the checkpoint file.
    /// </summary>
    public string Name { get; set; } = ".checkpoint";

}
