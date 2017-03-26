package Q1;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;


import Common.JoinGenericWritable;
import Common.MovieRecord;
import Common.RecordIdKey;

public class JoinReducerFilter extends Reducer<RecordIdKey, JoinGenericWritable, Text, IntWritable> {
	public void reduce(RecordIdKey key, Iterable<JoinGenericWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		MovieRecord movie = null;
		
		for (JoinGenericWritable v : values) {
			Writable record = v.get();
			if (record.getClass() == Common.MovieRecord.class) {
				movie = (MovieRecord) record;
			} else {
				count++;
			}
		}
		if (movie != null && count > 0)
			context.write(movie.Title, new IntWritable(count));
	}

}
