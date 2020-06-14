package co.edureka;

//package com.kapil.earthquake.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EarthquakeExecute {
	/**
	 * Application entry point.
	 * 
	 * @param args
	 * @throws Exception
	 *             - Bad idea but produces less cluttered code.
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * if (args.length != 2) {
		 * System.err.println("Usage: hadoopex <input path> <output path>");
		 * System.exit(-1); }
		 */

		// Create the job specification object
		Job job = Job.getInstance();
		job.setJarByClass(EarthquakeExecute.class);
		job.setJobName("Earthquake Measurment");

		// Setup input and output paths
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//provide output folder path below
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Set the Mapper and Reducer classes
		job.setMapperClass(EarthquakeMapper.class);
		job.setReducerClass(EarthquakeReducer.class);

		// Specify the type of output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Wait for the job to finish before terminating
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
