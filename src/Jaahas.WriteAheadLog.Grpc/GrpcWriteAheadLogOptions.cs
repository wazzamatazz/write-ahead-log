namespace Jaahas.WriteAheadLog.Grpc;

/// <summary>
/// Options for <see cref="GrpcWriteAheadLog"/>.
/// </summary>
public class GrpcWriteAheadLogOptions() {

    /// <summary>
    /// The name of the remote log to connect to.
    /// </summary>
    public string? LogName { get; set; }
    
    /// <summary>
    /// Specifies whether to use streaming writes for writing to the log.
    /// </summary>
    /// <remarks>
    ///   Streaming writes use bidirectional streaming to write log entries whereas non-streaming
    ///   writes use unary RPCs for each log entry.
    /// </remarks>
    public bool UseStreamingWrites { get; set; } = true;

}
