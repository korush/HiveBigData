package Common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MovieRecord implements Writable{

	
	 public Text Title = new Text();
	 public Text Genres = new Text();
	 
	 public MovieRecord(){}              

	    public MovieRecord(String title, String genres) {
	        
	        this.Title.set(title);
	        this.Genres.set(genres);
	    }

	    public void write(DataOutput out) throws IOException {
	    	
	        this.Title.write(out);
	        this.Genres.write(out);
	    }

	    public void readFields(DataInput in) throws IOException {
	    	
	        this.Title.readFields(in);
	        this.Genres.readFields(in);
	    }
}
