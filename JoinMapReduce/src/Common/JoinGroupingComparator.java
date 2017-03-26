package Common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinGroupingComparator extends WritableComparator {
    public JoinGroupingComparator() {
    	super((Class<? extends WritableComparable>) RecordIdKey.class, true);
    }                             

    @Override
    public int compare (WritableComparable a, WritableComparable b){
    	RecordIdKey first = (RecordIdKey) a;
    	RecordIdKey second = (RecordIdKey) b;
                    
        return first.RecordId.compareTo(second.RecordId);
    }
}
