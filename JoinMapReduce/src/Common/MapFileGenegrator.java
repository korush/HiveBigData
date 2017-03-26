package Common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile.Writer.Option;

public class MapFileGenegrator {
	
	public static void Generate(Configuration conf, String inputFilePath, String outputFilePath, int keyIndex, int valueIndex) throws IOException {
	    Path outputLocation = new Path(outputFilePath);
	    
	    Option keyClass = (Option)MapFile.Writer.keyClass(IntWritable.class);
	    SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);
	    MapFile.Writer.setIndexInterval(conf, 1);
	    MapFile.Writer writer = new MapFile.Writer(conf, outputLocation, keyClass, valueClass);
	        
	    File file = new File(inputFilePath);
	    FileInputStream fis = new FileInputStream(file);
	    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	    String line;
	    br.readLine();
	    int i = 0;
	    while ((line = br.readLine()) != null){
	        String[] lineItems = line.split(",");
	        IntWritable key = new IntWritable(Integer.parseInt(lineItems[keyIndex]));
	        Text value = new Text(lineItems[valueIndex]);
	        writer.append(key, value);
	        i++;
	    }
	    br.close();
	    writer.close();
	}

}
