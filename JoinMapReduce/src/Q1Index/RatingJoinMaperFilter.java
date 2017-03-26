package Q1Index;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import Common.JoinGenericWritable;
import Common.MovieRateRecord;
import Common.RecordIdKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class RatingJoinMaperFilter extends Mapper<LongWritable, Text, RecordIdKey, JoinGenericWritable> {
    
	private MapFile.Reader reader = null;
    
    public void setup(Context context) throws IOException{
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(context.getCacheFiles()[0]);
        this.reader = new MapFile.Reader(fs, dir.toString(), conf);
    }
        
    private Text findKey(IntWritable key) throws IOException{
        Text value = new Text();
        this.reader.get(key, value);
        return value;
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
    	
    	String[] recordFields = value.toString().split(",");
        try
        {
        	int movieId = Integer.parseInt(recordFields[1]);
        
        double rating = Double.parseDouble(recordFields[2]);
        if(rating < 4)
       	 return;
        
        Text genres = findKey(new IntWritable(movieId));
        if(!genres.toString().toUpperCase().contains("COMEDY"))
        	return;
        
        RecordIdKey recordKey = new RecordIdKey(movieId, RecordIdKey.MOVIERATING_RECORD);
        MovieRateRecord movieRecord = new MovieRateRecord(null, genres.toString(), rating);
        
                                               
        JoinGenericWritable genericRecord = new JoinGenericWritable(movieRecord);
        context.write(recordKey, genericRecord);
        }
        catch(Exception e)
        {
       	 System.out.println(e.getMessage());
        }
            
    }
        
    public void cleanup(Context context) throws IOException{
        reader.close();
    }
                               
  


}
