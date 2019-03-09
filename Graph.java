import java.io.*;
import java.util.Scanner;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/*
	Name: Vaibhav Murkute
	Project: MapReduce-Grouping Nodes based on number of neighbors
	Date: 02/18/2019
*/
public class Graph {
	public static class CntNeighborMapper extends Mapper<Object,Text,LongWritable,LongWritable> {
        	@Override
        	public void map ( Object key, Text value, Context context )
                	        throws IOException, InterruptedException {
			// Just the value 1
			LongWritable one = new LongWritable(1);

            		String[] line = value.toString().split(",");
            		long x = Long.parseLong(line[0]);
            		//int y = Integer.parseInt(line[1]);
	            	context.write(new LongWritable(x),one);
        
        }
    }

    public static class CntNeighborReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v: values) {
                sum += v.get();
            };
            context.write(key,new LongWritable(sum));
        }
    }


	public static class GrpNeighborMapper extends Mapper<Object,LongWritable,LongWritable,LongWritable> {
        	@Override
        	public void map ( Object key, LongWritable value, Context context )
                	        throws IOException, InterruptedException {
			// Just the value 1
			LongWritable one = new LongWritable(1);

            		String[] line = value.toString().split("\t");
			
			//Output of 1st job contains multiple spaces between key and value
            		long x = Long.parseLong(line[(line.length - 1)]);
	            	context.write(new LongWritable(x),one);
        
        }
    }

    public static class GrpNeighborReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v: values) {
                sum += v.get();
            };
            context.write(key,new LongWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {

	String TEMP_DIRECTORY = "temp";

	Job job1 = Job.getInstance();
        job1.setJobName("CountNeighborJob");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setMapperClass(CntNeighborMapper.class);
        job1.setReducerClass(CntNeighborReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(TEMP_DIRECTORY));
        boolean status = job1.waitForCompletion(true);

	if(status){
		Job job2 = Job.getInstance();
        	job2.setJobName("GroupNeighborJob");
        	job2.setJarByClass(Graph.class);
        	job2.setOutputKeyClass(LongWritable.class);
        	job2.setOutputValueClass(LongWritable.class);
        	job2.setMapOutputKeyClass(LongWritable.class);
        	job2.setMapOutputValueClass(LongWritable.class);
        	job2.setMapperClass(GrpNeighborMapper.class);
        	job2.setReducerClass(GrpNeighborReducer.class);
        	job2.setInputFormatClass(SequenceFileInputFormat.class);
        	job2.setOutputFormatClass(TextOutputFormat.class);
        	FileInputFormat.setInputPaths(job2,new Path(TEMP_DIRECTORY));
        	FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        	status = job2.waitForCompletion(true);
	}

	// deleting temporary directory	
	if(status){
		File temp_dir = new File(TEMP_DIRECTORY);
		if(temp_dir.exists() && temp_dir.isDirectory()){
			File[] files = temp_dir.listFiles();
			if(files != null){
				for(File f : files){
					f.delete();
				}
			}
			temp_dir.delete();
		}
	}
   }
}
