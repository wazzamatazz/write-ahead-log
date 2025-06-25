using Microsoft.IO;

namespace Jaahas.WriteAheadLog;

/// <summary>
/// Common utilities.
/// </summary>
public static class WriteAheadLogUtilities {
    
    /// <summary>
    /// A <see cref="Microsoft.IO.RecyclableMemoryStreamManager"/> that can be used to obtain
    /// pooled memory streams for efficient buffering.
    /// </summary>
    public static RecyclableMemoryStreamManager RecyclableMemoryStreamManager { get; } = new RecyclableMemoryStreamManager();
    
}
