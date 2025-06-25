using System.Collections.Immutable;

namespace Jaahas.WriteAheadLog.FileSystem.Internal;

internal abstract class SegmentIndex {
    
    public SegmentHeader Header { get; }
    
    private ImmutableArray<MessageIndexEntry> _entries = ImmutableArray<MessageIndexEntry>.Empty;

    public IReadOnlyList<MessageIndexEntry> Entries => _entries;


    protected SegmentIndex(SegmentHeader header, IEnumerable<MessageIndexEntry>? entries) {
        Header = header ?? throw new ArgumentNullException(nameof(header));
        
        if (entries is not null) {
            AddEntriesCore(entries);
        }
    }
    
    
    protected SegmentIndex(SegmentHeader header, params ReadOnlySpan<MessageIndexEntry> entries) {
        Header = header ?? throw new ArgumentNullException(nameof(header));
        
        if (entries.Length > 0) {
            AddEntriesCore(entries);
        }
    }
    
    
    protected void AddEntryCore(MessageIndexEntry entries) {
        _entries = _entries.Add(entries);
    }


    protected void AddEntriesCore(IEnumerable<MessageIndexEntry> entries) {
        ArgumentNullException.ThrowIfNull(entries);
        
        var builder = _entries.ToBuilder();
        builder.AddRange(entries);

        if (builder.Count > _entries.Length) {
            _entries = builder.ToImmutable();
        }
    }
    

    protected void AddEntriesCore(params ReadOnlySpan<MessageIndexEntry> entries) {
        var builder = _entries.ToBuilder();
        builder.AddRange(entries);
            
        if (builder.Count > _entries.Length) {
            _entries = builder.ToImmutable();
        }
    }


    internal ImmutableSegmentIndex ToImmutable() {
        return this as ImmutableSegmentIndex ?? new ImmutableSegmentIndex(Header, _entries);
    }

}
