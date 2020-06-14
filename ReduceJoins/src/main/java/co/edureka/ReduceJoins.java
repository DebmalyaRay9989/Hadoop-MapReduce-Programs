package co.edureka;

// package com;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;

public class ReduceJoins {

	//INPUT Key = 0 , value = 3451::Three Colors: Red (1994)::Drama
	//OUTPUT KEY = 3451		value = Drama
	public static class MovieMapper extends Mapper<LongWritable,Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] columns = value.toString().split("::");
			outkey.set(columns[0]);
			outvalue.set("movie\t"+columns[2]);
			System.out.println("key = "+outkey+" & value = "+outvalue);
			context.write(outkey, outvalue);
		}
	}
	
	//INPUT KEY = 0, value = 2::3451::4::978298924
	//OUTPUT KEY = 3451		value = 4
	public static class RatingMapper extends Mapper<LongWritable,Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] columns = value.toString().split("::");
			outkey.set(columns[1]);
			outvalue.set("rating\t"+columns[2]);
			System.out.println("key = "+outkey+" & value = "+outvalue);
			context.write(outkey, outvalue);
		}
	}
	
	
	//INPUT KEY = 3451, value = (rating\t4,rating\t1,rating\t1,rating\t3,rating\t5,movie\tDrama)
	//OUTPUT KEY = 3451	 value = Drama
	public static class MovieReviewReducer extends Reducer<Text,Text,Text,Text>{
		Text outvalue = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			int count = 0;
			String genre = null;
			for(Text value:values){
				String[] parts = value.toString().split("\t");
				if("rating".equals(parts[0])){
					if(parts[1].equals("1")){
						count++;
					}
				}else if("movie".equals(parts[0])){
					genre = parts[1];
				}
			}
			outvalue.set("Genre = "+genre);
			
			System.out.println("key = "+key+" & value = "+outvalue);
			if(count > 10){
				context.write(key, outvalue);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Reduce side join Example for Movie Review");
	    
	    job.setJarByClass( ReduceJoins.class );
	    job.setReducerClass( MovieReviewReducer.class );
	    job.setOutputKeyClass( Text.class );	    
	    job.setOutputValueClass( Text.class );
	    
	    MultipleInputs.addInputPath(job, new Path( args[0] ), TextInputFormat.class, MovieMapper.class);
	    MultipleInputs.addInputPath(job, new Path( args[1] ), TextInputFormat.class, RatingMapper.class);
	    
	    Path outputPath = new Path(args[2]);
	    FileOutputFormat.setOutputPath(job, outputPath);
	    System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}

}
