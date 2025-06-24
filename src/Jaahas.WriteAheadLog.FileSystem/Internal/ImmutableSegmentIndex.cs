namespace Jaahas.WriteAheadLog.Internal;

/// <summary>
/// An index for a read-only segment.
/// </summary>
internal sealed class ImmutableSegmentIndex : SegmentIndex {

    /// <summary>
    /// Creates a new instance of <see cref="ImmutableSegmentIndex"/> with the specified header and entries.
    /// </summary>
    /// <param name="header">
    ///   The segment header containing metadata about the segment.
    /// </param>
    /// <param name="entries">
    ///   The collection of message index entries to be added to the segment index.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="header"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="entries"/> is <see langword="null"/>.
    /// </exception>
    public ImmutableSegmentIndex(SegmentHeader header, IEnumerable<MessageIndexEntry> entries) : base(header, entries) {
        ArgumentNullException.ThrowIfNull(entries);
    }
    
    
    /// <summary>
    /// Creates a new instance of <see cref="ImmutableSegmentIndex"/> with the specified header and entries.
    /// </summary>
    /// <param name="header">
    ///   The segment header containing metadata about the segment.
    /// </param>
    /// <param name="entries">
    ///   The collection of message index entries to be added to the segment index.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///   <paramref name="header"/> is <see langword="null"/>.
    /// </exception>
    public ImmutableSegmentIndex(SegmentHeader header, params ReadOnlySpan<MessageIndexEntry> entries) : base(header, entries) { }

}
