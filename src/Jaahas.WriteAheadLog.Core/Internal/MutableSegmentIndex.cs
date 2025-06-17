namespace Jaahas.WriteAheadLog.Internal;

internal sealed class MutableSegmentIndex : SegmentIndex {

    public MutableSegmentIndex(SegmentHeader header, IEnumerable<MessageIndexEntry>? entries) : base(header, entries) { }


    public MutableSegmentIndex(SegmentHeader header, params ReadOnlySpan<MessageIndexEntry> entries) : base(header, entries) { }
    
    
    public void AddEntry(MessageIndexEntry entry) {
        AddEntryCore(entry);
    }
    
    
    public void AddEntries(IEnumerable<MessageIndexEntry> entries) {
        ArgumentNullException.ThrowIfNull(entries);
        AddEntriesCore(entries);
    }
    
    
    public void AddEntries(params ReadOnlySpan<MessageIndexEntry> entries) {
        AddEntriesCore(entries);
    }
    
}
