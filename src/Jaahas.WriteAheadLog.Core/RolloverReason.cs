namespace Jaahas.WriteAheadLog;

public enum RolloverReason {

    Manual,
    
    NoWritableSegments,
    
    SegmentSizeLimitReached,
    
    SegmentTimeLimitReached,
    
    SegmentMessageCountLimitReached

}
