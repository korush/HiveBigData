package Common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RatingRecord implements Writable{

	 //public IntWritable MovieId = new IntWritable();
	 public DoubleWritable Rating = new DoubleWritable();
	 
	// public MovieRecord Movie; 
	 
	 public RatingRecord(){}              

	    public RatingRecord(double rating) {
	        //this.MovieId.set(id);
	        this.Rating.set(rating);
	       
	    }
	    
	  //  public RatingRecord(double rating, MovieRecord movie) {
	        //this.MovieId.set(id);
	   //     this.Rating.set(rating);
	        //this.Movie = movie;
	       
	  //  }

	    public void write(DataOutput out) throws IOException {
	    	//this.MovieId.write(out);
	        this.Rating.write(out);
	       // this.Movie.write(out);
	      
	    }

	    public void readFields(DataInput in) throws IOException {
	    	//this.MovieId.readFields(in);
	        this.Rating.readFields(in);
	       // this.Movie.readFields(in);
	       
	    }
}
