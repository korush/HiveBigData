package Reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import Common.JoinGenericWritable;
import Common.MovieRecord;
import Common.RatingRecord;
import Common.RecordIdKey;


public class JoinRecuder extends Reducer<RecordIdKey, JoinGenericWritable, Text, IntWritable>{
    public void reduce(RecordIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
        StringBuilder output = new StringBuilder();
        int count = 0;
        
        System.out.println("**** "+ key.RecordType + "  ******");
                               
        for (JoinGenericWritable v : values) {
        	
            Writable record = v.get();
            System.out.println(record.getClass());
            if (record.getClass() ==  Common.MovieRecord.class){// key.RecordType.equals(RecordIdKey.MOVIE_RECORD)){
                MovieRecord pRecord = (MovieRecord)record;
                System.out.println(pRecord.Genres.toString());
                output.append(Integer.parseInt(key.RecordId.toString())).append(", ");
                output.append(pRecord.Title.toString()).append(", ");
                output.append(pRecord.Genres.toString()).append(", ");
            } else {
                //RatingRecord ratingRecord  = (RatingRecord)record;
                count++;
            }
        }
        if(count> 0)
        
            context.write(new Text(output.toString()), new IntWritable(count));
       }
}