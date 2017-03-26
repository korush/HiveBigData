
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
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
import Q1.MovieMapperNoFilter;
import Q1.RatingMapperNoFilter;
import Reducers.JoinRecuder;
import Reducers.RatingReducer;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Mapper;

public class Driver {
	// extends org.apache.hadoop.conf.Configured {

	private static boolean runJob(String jobName, String input1, String input2, String output,
			Class<? extends Mapper> mapperClass1, Class<? extends Mapper> mapperClass2,
			Class<? extends Reducer> reducerClass, Class<? extends WritableComparator> groupClass,
			Class<? extends WritableComparator> sortClass)
			throws Exception, IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		Job job = Job.getInstance(conf, "JoinJob");
		job.setJarByClass(Driver.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(RecordIdKey.class);
		job.setMapOutputValueClass(JoinGenericWritable.class);

		MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, mapperClass1);
		MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, mapperClass2);

		job.setReducerClass(reducerClass);

		if (sortClass != null)
			job.setSortComparatorClass(sortClass);

		if (groupClass != null)
			job.setGroupingComparatorClass(groupClass);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true);
	}

	private static boolean runJob(String jobName, String input1, String input2, String output,
			Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass,
			Class<? extends WritableComparator> groupClass, Class<? extends WritableComparator> sortClass)
			throws Exception, IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		Job job = Job.getInstance(conf, "JoinJob");
		job.setJarByClass(Driver.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(RecordIdKey.class);
		job.setMapOutputValueClass(JoinGenericWritable.class);

		job.addCacheFile(new File(input2).toURI());
		MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, mapperClass);

		job.setReducerClass(reducerClass);

		if (sortClass != null)
			job.setSortComparatorClass(sortClass);

		if (groupClass != null)
			job.setGroupingComparatorClass(groupClass);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {

		 ///No Sort (Q1)
		 runJob("Q1FilterInReducer", args[0], args[1], args[2],
		 Q1.RatingMapperNoFilter.class, Q1.MovieMapperNoFilter.class,
		 Q1.JoinReducerNoFilter.class, JoinGroupingComparator.class, null);

		 runJob("Q1FilterInMappers", args[0], args[1], args[2],
		 Q1.RatingMapperFilter.class, Q1.MovieMapperFilter.class,
		 Q1.JoinReducerFilter.class, JoinGroupingComparator.class, null);

		 ///Sort (Q2)
		 runJob("Q1FilterInReducer", args[0], args[1], args[2],
		 Q1.RatingMapperNoFilter.class, Q1.MovieMapperNoFilter.class,
		 Q1.JoinReducerNoFilter.class, JoinGroupingComparator.class,
		 JoinSortingComparator.class);

		 runJob("Q1FilterInMappers", args[0], args[1], args[2],
		 Q1.RatingMapperFilter.class, Q1.MovieMapperFilter.class,
		 Q1.JoinReducerFilter.class, JoinGroupingComparator.class,
		 JoinSortingComparator.class);

		 ///No Sort (Q1)
		 runJob("Q1FilterInReducerJoinInReducer", args[0], args[1], args[2],
		 Q1.RatingJoinMaperNoFilter.class, Q1.RatingJoinReducerNoFilter.class,
		 JoinGroupingComparator.class, null);

		 runJob("Q1FilterInMappersJoinInReducer", args[0], args[1], args[2],
		 Q1.RatingJoinMaperFilter.class, Q1.RatingJoinReducerFilter.class,
		 JoinGroupingComparator.class, null);

		 ///Sort(Q2)
		 runJob("Q1FilterInReducerJoinInReducer", args[0], args[1], args[2],
		 Q1.RatingJoinMaperNoFilter.class, Q1.RatingJoinReducerNoFilter.class,
		 JoinGroupingComparator.class, JoinSortingComparator.class);

		 runJob("Q1FilterInMappersJoinInReducer", args[0], args[1], args[2],
		 Q1.RatingJoinMaperFilter.class, Q1.RatingJoinReducerFilter.class,
		 JoinGroupingComparator.class, JoinSortingComparator.class);

		 ///No Sort (Q3)
		 runJob("Q3GroupBy", args[0], args[1], args[2],
		 Q3.RatingJoinMaper.class, Q3.RatingJoinReducer.class,
		 JoinGroupingComparator.class, null);

		 ///Sort (Q3)
		 runJob("Q3GroupBy", args[0], args[1], args[2],
		 Q3.RatingJoinMaper.class, Q3.RatingJoinReducer.class,
		 JoinGroupingComparator.class, JoinSortingComparator.class);

		 ///No Sort (Q4)
		 runJob("Q3GroupByFilterInReducer", args[0], args[1], args[2],
		 Q4.RatingJoinMaperNoFilter.class,
		 Q4.RatingJoinReducerNoFilter.class,
		 JoinGroupingComparator.class, null);
		
		 runJob("Q3GroupByFilterInMapper", args[0], args[1], args[2],
		 Q4.RatingJoinMaperFilter.class,
		 Q4.RatingJoinReducerFilter.class,
		 JoinGroupingComparator.class, null);

		 ///Sort (Q3)
		 runJob("Q3GroupByFilterInReducer", args[0], args[1], args[2],
		 Q4.RatingJoinMaperNoFilter.class,
		 Q4.RatingJoinReducerNoFilter.class,
		 JoinGroupingComparator.class, JoinSortingComparator.class);
		
		 runJob("Q3GroupByFilterInMapper", args[0], args[1], args[2],
		 Q4.RatingJoinMaperFilter.class,
		 Q4.RatingJoinReducerFilter.class,
		 JoinGroupingComparator.class, JoinSortingComparator.class);

	}

	

}
