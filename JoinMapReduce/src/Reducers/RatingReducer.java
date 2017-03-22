package Reducers;

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

public class RatingReducer extends Reducer<RecordIdKey, JoinGenericWritable, Text, IntWritable>{
    public void reduce(RecordIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
        StringBuilder output = new StringBuilder();
        int count = 0;
        
                                     
        for (JoinGenericWritable v : values) {
            Writable record = v.get();
            if (output.length() == 0){
            	MovieRateRecord pRecord = (MovieRateRecord)record;
                output.append(Integer.parseInt(key.RecordId.toString())).append(", ");
                output.append(pRecord.Title.toString()).append(", ");
                output.append(pRecord.Genres.toString()).append(", ");
                count++;
            } else {
                //RatingRecord ratingRecord  = (RatingRecord)record;
                count++;
            }
        }
        if(count> 0)
        
            context.write(new Text(output.toString()), new IntWritable(count));
       }

}
