package Common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class RecordIdKey implements WritableComparable<RecordIdKey> {
	
	public IntWritable RecordId = new IntWritable();
	public IntWritable RecordType = new IntWritable();
	
	public RecordIdKey(){}
	public RecordIdKey(int recordId, IntWritable recordType) {
	    this.RecordId.set(recordId);
	    this.RecordType = recordType;
	}

	public void write(DataOutput out) throws IOException {
	    this.RecordId.write(out);
	    this.RecordType.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    this.RecordId.readFields(in);
	    this.RecordType.readFields(in); 
	}
	
	public int compareTo(RecordIdKey other) {
	    if (this.RecordId.equals(other.RecordId )) {
	        return this.RecordType.compareTo(other.RecordType);
	    } else {
	        return this.RecordId.compareTo(other.RecordId);
	    }
	}
	
	public boolean equals (RecordIdKey other) {
	    return this.RecordId.equals(other.RecordId) && this.RecordType.equals(other.RecordType );

	}

	public int hashCode() {
	    return this.RecordId.hashCode();
	}
	
	public static final IntWritable MOVIE_RECORD = new IntWritable(0);
	public static final IntWritable RATING_RECORD = new IntWritable(1);
	public static final IntWritable MOVIERATING_RECORD = new IntWritable(2);
}
