package Q1;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Common.MovieRecord;
import Common.RatingRecord;
import Common.JoinGenericWritable;
import Common.MovieRateRecord;
import Common.RecordIdKey;


public class RatingJoinMaperNoFilter extends Mapper<LongWritable, Text, RecordIdKey, JoinGenericWritable> {
    
	private HashMap<Integer, MovieRateRecord> movies = new HashMap<Integer, MovieRateRecord>();
                               
    private void readMoviesFile(URI uri) throws IOException{
        List<String> lines = FileUtils.readLines(new File(uri));
        for (String line : lines) {
            String[] recordFields = line.split(",");
            try
            {
            int key = Integer.parseInt(recordFields[0]);
            MovieRateRecord movieRecord = new MovieRateRecord();
            movieRecord.Title.set(recordFields[1]);
            movieRecord.Genres.set(recordFields[2]);
            
            movies.put(key, movieRecord);
            }
            catch(Exception e)
            {
            	
            }
        }
    }
                               
    public void setup(Context context) throws IOException{
        URI[] uris = context.getCacheFiles();
        readMoviesFile(uris[0]);
    }
                               
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
    	 String[] recordFields = value.toString().split(",");
         try
         {
    	 int movieId = Integer.parseInt(recordFields[1]);
         
         double rating = Double.parseDouble(recordFields[2]);
                                             
         RecordIdKey recordKey = new RecordIdKey(movieId, RecordIdKey.MOVIERATING_RECORD);
         MovieRateRecord movieRecord = movies.get(movieId);
         movieRecord.Rating.set(rating);
                                                
         JoinGenericWritable genericRecord = new JoinGenericWritable(movieRecord);
         context.write(recordKey, genericRecord);
         }
         catch(Exception e)
         {
        	 System.out.println(e.getMessage());
         }
    	
    }

}
