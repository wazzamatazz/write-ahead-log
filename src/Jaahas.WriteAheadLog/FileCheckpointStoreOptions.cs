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
    
    /// <summary>
    /// The interval at which the current checkpoint will be flushed to disk.
    /// </summary>
    /// <remarks>
    ///   Specifying an interval less than or equal to <see cref="TimeSpan.Zero"/> will disable
    ///   automatic flushing. The checkpoint can be manually flushed by calling <see cref="FileCheckpointStore.FlushAsync"/>.
    /// </remarks>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromMinutes(1);

}
