package Q1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Common.JoinGenericWritable;
import Common.MovieRecord;
import Common.RatingRecord;
import Common.RecordIdKey;

public class JoinReducerNoFilter extends Reducer<RecordIdKey, JoinGenericWritable, Text, IntWritable> {
	public void reduce(RecordIdKey key, Iterable<JoinGenericWritable> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder output = new StringBuilder();
		int count = 0;

		MovieRecord movie = null;

		for (JoinGenericWritable value : values) {
			Writable r = value.get();
			if (r.getClass() == Common.MovieRecord.class) {
				MovieRecord m = (MovieRecord) r;
				if (m.Genres.toString().toUpperCase().contains("COMEDY")) {
					movie = m;
					break;
				}

			}
		}

		if (movie == null)
			return;

	
		for (JoinGenericWritable v : values) {
			Writable record = v.get();
			if (record.getClass() == Common.RatingRecord.class) {
				RatingRecord rating = (RatingRecord) record;
				if (rating.Rating.get() >= 4)
					count++;
			}
		}

		if (count > 0)
			context.write(movie.Title, new IntWritable(count));

	}
}