package co.edureka;

import java.io.IOException;

import org.apache.avro.specific.AvroGenerated;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import co.edureka.weather.*;
//import co.edureka.weather.ReducerClass;

public class WeatherRecords
{
	
	public static class WeatherRecordsMapper extends Mapper<Object, Text, Text, Text>
	{
		
		
		public void map(Object key, Text record, Context con) throws IOException,InterruptedException
		{
			//str-- 10-05-2020,Pune,35
			String[] info = record.toString().split(",");
			
			
			String Country = info[4]; // country will be the key
  			String Rest = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6]; // sex will be the value
   			con.write(new Text(Country), new Text(Rest));
			
		
			
		}
	}
	
	public static class WeatherRecordsReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Iterable<Text> key, Text record, Context con) throws IOException,InterruptedException
		{
			//int male_count=0, female_count=0;
			String country ;
			String out;
   			for (Text var : key) 
			{
					country=var.toString();
					String[] info = record.toString().split(",");
					if(country.equals("India")) {
						out = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6];
						//con.write(key, new Text(out));
						con.write(new Text(country), new Text(out));
					}
					
   			}
   			//String out = "Total Male: " + male_count + " :: " + "Total Female: " + female_count;
   			
			
		}
	}
	
	public static class WeatherRecordsReducerRest extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Iterable<Text> key, Text record, Context con) throws IOException,InterruptedException
		{
			//int male_count=0, female_count=0;
			String country ;
			String out;
   			for (Text var : key) 
			{
					country=var.toString();
					String[] info = record.toString().split(",");
					if(country.equals(("India"))) {
						//out = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6];
						//con.write(key, new Text(out));
						//con.write(new Text(country), new Text(out));
					}
					else {
						out = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6];
						con.write(new Text(country), new Text(out));
					}
					
   			}
   			//String out = "Total Male: " + male_count + " :: " + "Total Female: " + female_count;
   			
			
		}
	}

	
	
	
	
	
	public static void main(String[] args) throws Exception
	{
		   Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Weather Records Country (india vs rest)");
		    job.setJarByClass(WeatherRecords.class);
			job.setMapperClass(WeatherRecordsMapper.class);
    		job.setReducerClass(WeatherRecordsReducer.class);

    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(Text.class);
			
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);

		 //   job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0])); //--> input file-- data set(HDFS)
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));// --> place where my out put will be stored(HDFS)
		    
		    job.setReducerClass(WeatherRecordsReducerRest.class);
		    job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(Text.class);
			
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);
    		FileOutputFormat.setOutputPath(job, new Path(args[2])); // Rest
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
