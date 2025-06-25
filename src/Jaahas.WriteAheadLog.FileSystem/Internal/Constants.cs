using System.Text;

namespace Jaahas.WriteAheadLog.FileSystem.Internal;

internal static class Constants {

    public static ReadOnlyMemory<byte> SegmentMagicBytes = Encoding.ASCII.GetBytes("WAL!");
    
    public static ReadOnlyMemory<byte> MessageMagicBytes = Encoding.ASCII.GetBytes("MSG!");

}
