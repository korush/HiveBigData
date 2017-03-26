package Common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MovieRateRecord implements Writable {
	
	 public Text Title = new Text();
	 public Text Genres = new Text();
	 public DoubleWritable Rating = new DoubleWritable();
	 
	 public MovieRateRecord(){}              

	    public MovieRateRecord(String title, String genres, double rating) {
	        
	        this.Title.set(title);
	        this.Genres.set(genres);
	        this.Rating.set(rating);
	    }

	    public void write(DataOutput out) throws IOException {
	    	
	        this.Title.write(out);
	        this.Genres.write(out);
	        this.Rating.write(out);
	    }

	    public void readFields(DataInput in) throws IOException {
	    	
	        this.Title.readFields(in);
	        this.Genres.readFields(in);
	        this.Rating.readFields(in);
	    }
	    

	    @Override
	    public String toString()
	    {
	    	return this.Title.toString() + " " + this.Genres.toString()  + " " + this.Rating;
	    }
	    

}
