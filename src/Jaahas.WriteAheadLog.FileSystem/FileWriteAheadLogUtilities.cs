using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

using Jaahas.WriteAheadLog.Internal;

namespace Jaahas.WriteAheadLog;

public static class FileWriteAheadLogUtilities {

    /// <summary>
    /// The size of the serialized log entry header in bytes.
    /// </summary>
    private const int LogEntryHeaderSize = 24; // 4 + 4 + 8 + 8 (magic number, message body length, sequence ID, timestamp)
    
    /// <summary>
    /// Gets the serialized size of a log entry based on the length of the message body.
    /// </summary>
    /// <param name="messageLength">
    ///   The length of the message body in bytes.
    /// </param>
    /// <returns>
    ///   The total size of the serialized log entry in bytes, including the header and checksum.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetSerializedLogEntrySize(long messageLength) {
        ArgumentOutOfRangeException.ThrowIfLessThan(messageLength, 0);
        
        // 4 extra footer bytes for checksum
        return LogEntryHeaderSize + messageLength + 4;
    }
    

    /// <summary>
    /// Writes a log entry to the specified buffer writer.
    /// </summary>
    /// <param name="writer">
    ///   The buffer writer to write the log entry to.
    /// </param>
    /// <param name="sequenceId">
    ///   The sequence ID for the log entry.
    /// </param>
    /// <param name="timestamp">
    ///   The timestamp for the log entry, measured in ticks.
    /// </param>
    /// <param name="message">
    ///   The message body of the log entry.
    /// </param>
    /// <returns>
    ///   The total size of the serialized log entry in bytes, including the header, body, and checksum.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="writer"/> is <see langword="null"/>.
    /// </exception>
    public static long WriteLogEntryToBuffer(IBufferWriter<byte> writer, ulong sequenceId, long timestamp, ReadOnlySequence<byte> message) {
        ArgumentNullException.ThrowIfNull(writer);
        
        var messageLengthAsInt = (int) message.Length;
        
        var buffer = writer.GetSpan((int) GetSerializedLogEntrySize(message.Length));
        Constants.MessageMagicBytes.Span.CopyTo(buffer);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[4..], messageLengthAsInt);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[8..], sequenceId);
        BinaryPrimitives.WriteInt64LittleEndian(buffer[16..], timestamp);

        message.CopyTo(buffer[LogEntryHeaderSize..]);
        var footerStartPosition = LogEntryHeaderSize + messageLengthAsInt;
        
        var checksum = Crc32.HashToUInt32(buffer[..footerStartPosition]);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[footerStartPosition..], checksum);
        var serializedSize = footerStartPosition + 4; // 4 bytes for checksum
        writer.Advance(serializedSize);
        
        return serializedSize;
    }
    
    
    /// <summary>
    /// Reads a log entry from the specified read-only byte sequence.
    /// </summary>
    /// <param name="sequence">
    ///   The byte sequence to read the log entry from.
    /// </param>
    /// <param name="entry">
    ///   The log entry that was read, or <see langword="null"/> if the sequence does not contain
    ///   a valid log entry.
    /// </param>
    /// <returns>
    ///   <see langword="true"/> if a valid log entry was read; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    ///   When a log entry is successfully read, the <paramref name="sequence"/> is advanced to the
    ///   position immediately after the end of the log entry.
    /// </remarks>
    public static bool TryReadLogEntry(ref ReadOnlySequence<byte> sequence, [NotNullWhen(true)] out LogEntry? entry) {
        entry = null;

        if (sequence.Length < LogEntryHeaderSize) {
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
        
        // Rent a buffer to hold the message header, body and checksum.
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
        
        // Checksum matches, create the log entry instance.
        
        // [0,4) = magic number
        // [4,8) = message length
        // [8,16) = sequence ID
        // [16,24) = timestamp
        var sequenceId = BinaryPrimitives.ReadUInt64LittleEndian(bufferSpan[8..]);
        var timestamp = BinaryPrimitives.ReadInt64LittleEndian(bufferSpan[16..]);

        entry = LogEntry.Create(sequenceId, timestamp, buffer, 24, messageLength, ArrayPool<byte>.Shared);
        
        return true;
    }


    /// <summary>
    /// Advances the <paramref name="reader"/> to the next occurrence of the specified magic
    /// bytes.
    /// </summary>
    /// <param name="reader">
    ///   The <see cref="SequenceReader{T}"/> to advance.
    /// </param>
    /// <param name="magicBytes">
    ///   The magic bytes to search for in the sequence.
    /// </param>
    /// <returns>
    ///   <see langword="true"/> if the magic bytes were found and the reader was advanced past
    ///   them; otherwise, <see langword="false"/>.
    /// </returns>
    private static bool TryAdvanceToMagicNumber(ref SequenceReader<byte> reader, ReadOnlySpan<byte> magicBytes) {
        while (!reader.End) {
            var currentSpan = reader.CurrentSpan;

            if (currentSpan.Length >= magicBytes.Length) {
                // Fast path; search for the magic bytes in the current span since the span is at
                // least as long as the magic bytes sequence.
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
                if (advanceCount > reader.Remaining) {
                    advanceCount = (int) reader.Remaining;
                }
                reader.Advance(advanceCount);
            }
            else {
                // Fall back to byte-by-byte search for small spans or segment boundaries
                return TryAdvanceToMagicNumberSlow(ref reader, magicBytes);
            }
        }

        return false;
    }


    /// <summary>
    /// Slow path for finding the specified magic bytes in the sequence reader that is used when
    /// the magic bytes span multiple segments in the <see cref="ReadOnlySequence{T}"/>.
    /// </summary>
    /// <param name="reader">
    ///   The <see cref="SequenceReader{T}"/> to advance.
    /// </param>
    /// <param name="magicBytes">
    ///   The magic bytes to search for in the sequence.
    /// </param>
    /// <returns>
    ///   <see langword="true"/> if the magic bytes were found and the reader was advanced past
    ///   them; otherwise, <see langword="false"/>.
    /// </returns>
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

}
