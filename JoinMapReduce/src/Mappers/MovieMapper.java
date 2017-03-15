package Mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import Common.JoinGenericWritable;
import Common.MovieRecord;
import Common.RecordIdKey;

public class MovieMapper extends Mapper<LongWritable, Text, RecordIdKey, JoinGenericWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      try
      {
    	String[] recordFields = value.toString().split(",");
        int movieId = Integer.parseInt(recordFields[0]);
        String title = recordFields[1];
        String genre = recordFields[2];
                                               
        RecordIdKey recordKey = new RecordIdKey(movieId, RecordIdKey.MOVIE_RECORD);
        MovieRecord record = new MovieRecord(title, genre);
        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
      }
      catch(Exception e)
      {
      }
      
    }
}
