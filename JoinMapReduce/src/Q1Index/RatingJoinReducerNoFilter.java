package Q1Index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Common.JoinGenericWritable;
import Common.MovieRateRecord;
import Common.MovieRecord;
import Common.RecordIdKey;

public class RatingJoinReducerNoFilter extends Reducer<RecordIdKey, JoinGenericWritable, Text, IntWritable> {
	public void reduce(RecordIdKey key, Iterable<JoinGenericWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		

		Text title = null;
		for (JoinGenericWritable v : values) {
			Writable record = v.get();
			MovieRateRecord pRecord = (MovieRateRecord) record;
			
			
			if(!(pRecord.Rating.get() >= 4.0  && pRecord.Genres.toString().toUpperCase().contains("COMEDY")))
				continue;
			if (title == null) {
				title = pRecord.Title;
			}	
			
			count++;
			
		}
		if (count > 0)
			context.write(title, new IntWritable(count));
	}

}
