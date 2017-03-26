package Q3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Common.JoinGenericWritable;
import Common.MovieRateRecord;
import Common.MovieRecord;
import Common.RecordIdKey;

public class RatingJoinReducer extends Reducer<Text, JoinGenericWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<JoinGenericWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		double sum = 0;

		Text title = null;
		for (JoinGenericWritable v : values) {
			Writable record = v.get();
			MovieRateRecord pRecord = (MovieRateRecord) record;
			
			if (title == null) {
				title = pRecord.Title;
			}	
			
			sum += pRecord.Rating.get();
			count++;
			
		}
		if (count > 0)
			context.write(title, new DoubleWritable(sum / count));
	}

}
