using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;

using Jaahas.WriteAheadLog.Internal;

namespace Jaahas.WriteAheadLog;

public class LogEntry : IDisposable {
    
    private const int HeaderSize = 24; // 4 + 4 + 8 + 8 (magic number, message body length, sequence ID, timestamp)

    private bool _disposed;
    
    private readonly byte[] _buffer;

    public ulong SequenceId { get; }
    
    public long Timestamp { get; }

    public ArraySegment<byte> Data {
        get {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return field;
        }
    }
    
    /// <summary>
    /// The total size of the log entry in bytes, including the header, body, and checksum.
    /// </summary>
    public int TotalSize { get; }

    
    private LogEntry(ulong sequenceId, long timestamp, byte[] buffer, int offset, int count, int totalSize) {
        SequenceId = sequenceId;
        Timestamp = timestamp;
        
        _buffer = buffer;
        Data = new ArraySegment<byte>(_buffer, offset, count);
        
        TotalSize = totalSize;
    }

    
    public static long GetSerializedSize(long messageLength) {
        ArgumentOutOfRangeException.ThrowIfLessThan(messageLength, 0);
        
        // 4 extra footer bytes for checksum
        return HeaderSize + messageLength + 4;
    }
    

    public static long Write(IBufferWriter<byte> writer, ulong sequenceId, long timestamp, ReadOnlySequence<byte> message) {
        ArgumentNullException.ThrowIfNull(writer);
        
        var messageLengthAsInt = (int) message.Length;
        
        var buffer = writer.GetSpan((int) GetSerializedSize(message.Length));
        Constants.MessageMagicBytes.Span.CopyTo(buffer);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[4..], messageLengthAsInt);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[8..], sequenceId);
        BinaryPrimitives.WriteInt64LittleEndian(buffer[16..], timestamp);

        message.CopyTo(buffer[HeaderSize..]);
        var footerStartPosition = HeaderSize + messageLengthAsInt;
        
        var checksum = Crc32.HashToUInt32(buffer[..footerStartPosition]);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[footerStartPosition..], checksum);
        var serializedSize = footerStartPosition + 4; // 4 bytes for checksum
        writer.Advance(serializedSize);
        
        return serializedSize;
    }
    

    public static bool TryRead(ref ReadOnlySequence<byte> sequence, [NotNullWhen(true)] out LogEntry? entry) {
        entry = null;

        if (sequence.Length < HeaderSize) {
            // Not enough data for a header so definitely not a valid entry.
            return false;
        }
        
        var reader = new SequenceReader<byte>(sequence);
        if (!TryAdvanceToMagicNumber(ref reader, Constants.MessageMagicBytes.Span)) {
            return false;
        }
        
        if (!reader.TryReadLittleEndian(out int messageLength)) {
            // Not enough data for the message length, rewind to the start of the magic number
            reader.Rewind(4);
            return false;
        }

        // 16 + messageLength + 4 = sequence ID (8) + timestamp (8) + message length + checksum (4)
        if (reader.Remaining < 16 + messageLength + 4) {
            // Not enough data for the full message, rewind to the start of the magic number
            reader.Rewind(4 + 4); // 4 for the magic number and 4 for the message length
            return false;
        }
        
        // Get a buffer to hold the message header, body and checksum.
        var buffer = ArrayPool<byte>.Shared.Rent(24 + messageLength + 4);
        var bufferSpan = new Span<byte>(buffer, 0, 24 + messageLength + 4);

        // Rewind back to the start of the message.
        reader.Rewind(4 + 4); // 4 for the magic number and 4 for the message length
        
        // Copy the header, message body and checksum into the buffer
        reader.Sequence.Slice(reader.Position, 24 + messageLength + 4).CopyTo(buffer);
        reader.Advance(24 + messageLength + 4);
        
        // Move the sequence past the end of the entry - at this point we have a complete log entry
        // and the only remaining decision is whether it is valid or not.
        sequence = sequence.Slice(reader.Position);
        
        var checksumActual = BinaryPrimitives.ReadUInt32LittleEndian(bufferSpan[(24 + messageLength)..]);
        var checksumExpected = Crc32.HashToUInt32(bufferSpan[..(24 + messageLength)]);
        if (checksumActual != checksumExpected) {
            // Checksum mismatch, this is not a valid log entry.
            ArrayPool<byte>.Shared.Return(buffer);
            return false;
        }
        
        // Checksum matches, create the log entry.
        
        // [0,4) = magic number
        // [4,8) = message length
        // [8,16) = sequence ID
        // [16,24) = timestamp
        var sequenceId = BinaryPrimitives.ReadUInt64LittleEndian(bufferSpan[8..]);
        var timestamp = BinaryPrimitives.ReadInt64LittleEndian(bufferSpan[16..]);
        
        entry = new LogEntry(sequenceId, timestamp, buffer, 24, messageLength, 24 + messageLength + 4);
        return true;
    }


    private static bool TryAdvanceToMagicNumber(ref SequenceReader<byte> reader, ReadOnlySpan<byte> magicBytes) {
        while (!reader.End) {
            var currentSpan = reader.CurrentSpan;

            if (currentSpan.Length >= magicBytes.Length) {
                // Search for the magic number in the current span.
                var index = currentSpan.IndexOf(magicBytes[0]);

                while (index >= 0 && index <= currentSpan.Length - magicBytes.Length) {
                    var candidate = currentSpan.Slice(index, magicBytes.Length);
                    if (candidate.SequenceEqual(magicBytes)) {
                        // Found the magic number; advance to the position after it.
                        reader.Advance(index + magicBytes.Length);
                        return true;
                    }
                    
                    // Look for the next possible occurrence of the magic number.
                    var remainingSpan = currentSpan[(index + 1)..];
                    var nextIndex = remainingSpan.IndexOf(magicBytes[0]);
                    index = nextIndex >= 0 
                        ? index + 1 + nextIndex 
                        : -1;
                }
                
                // Advance to the next segment but retain the last few bytes in case the magic
                // number spans segments.
                var advanceCount = Math.Max(0, currentSpan.Length - magicBytes.Length + 1);
                reader.Advance(advanceCount);
            }
            else {
                // Fall back to byte-by-byte search for small spans or segment boundaries
                return TryAdvanceToMagicNumberSlow(ref reader, magicBytes);
            }
        }

        return false;
    }


    private static bool TryAdvanceToMagicNumberSlow(ref SequenceReader<byte> reader, ReadOnlySpan<byte> magicBytes) {
        while (reader.Remaining >= magicBytes.Length) {
            var startPosition = reader.Position;
            var isMatch = true;

            foreach (var magicByte in magicBytes) {
                if (reader.TryRead(out var b) && b == magicByte) {
                    continue;
                }

                isMatch = false;
                break;
            }

            if (isMatch) {
                return true;
            }

            // Reset and advance by one byte
            reader.Rewind(reader.Position.GetInteger() - startPosition.GetInteger());
            reader.Advance(1);
        }

        return false;
    }


    public void Dispose() {
        if (_disposed) {
            return;
        }
        
        ArrayPool<byte>.Shared.Return(_buffer);
        
        _disposed = true;
    }
    
}
