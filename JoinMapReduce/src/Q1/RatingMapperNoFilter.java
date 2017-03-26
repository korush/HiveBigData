package Q1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import Common.JoinGenericWritable;
import Common.RatingRecord;
import Common.RecordIdKey;

public class RatingMapperNoFilter extends Mapper<LongWritable, Text, RecordIdKey, JoinGenericWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	try
    	{
        String[] recordFields = value.toString().split(",");
        int movieId = Integer.parseInt(recordFields[1]);
        
        double rating = Double.parseDouble(recordFields[2]);
                                               
        RecordIdKey recordKey = new RecordIdKey(movieId, RecordIdKey.RATING_RECORD);
        RatingRecord record = new RatingRecord(rating);
                                               
        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
    catch(Exception e)
    {
    }
    }
}