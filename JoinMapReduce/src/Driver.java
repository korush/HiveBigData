
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import Common.JoinGenericWritable;
import Common.JoinGroupingComparator;
import Common.JoinSortingComparator;
import Common.RecordIdKey;
import Mappers.MovieMapper;
import Mappers.RatingJoinMaper;
import Mappers.RatingMapper;
import Reducers.JoinRecuder;
import Reducers.RatingReducer;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class Driver {
//extends org.apache.hadoop.conf.Configured {
	
	public static int run(String[] args) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		
	    //String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	                               
	    //Job job = Job.getInstance(getConf());
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JoinJob");
	    job.setJarByClass(Driver.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(RecordIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
	                              
	    job.setReducerClass(JoinRecuder.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }             
	}

public static int runInMapJoin(String[] args) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		
	    
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JoinJob");
	    job.setJarByClass(Driver.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(RecordIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                          
	    job.addCacheFile(new File(args[1]).toURI());
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingJoinMaper.class);
	    
	                              
	    job.setReducerClass(RatingReducer.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }             
	}

	public static void main(String[] args) throws Exception{                               
	    //Configuration conf = new Configuration();
	    //int res = ToolRunner.run(new Driver(), args);
		//run(args);
		runInMapJoin(args);
	}
}
