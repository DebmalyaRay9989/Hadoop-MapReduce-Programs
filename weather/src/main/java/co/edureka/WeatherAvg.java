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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAvg
{
	
	public static class WeatherAvgMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		
		
		public void map(LongWritable key,Text value, Context ctx) throws IOException,InterruptedException
		{
			//str-- 10-05-2020,Pune,35
			String str=value.toString();
			
			String[] data=str.split(",");
			//data-- [0]-dear [1]- bear
			
			Text city=new Text(data[1]);
			int t=Integer.parseInt(data[2]);
			
			IntWritable temp=new IntWritable(t);
			
			ctx.write(city, temp);
			
		
			
		}
	}
	
	public static class WeatherAvgReducer extends Reducer<Text, IntWritable, Text, FloatWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException,InterruptedException
		{
			//key --> Pune
			//value--> <35,30,23>
			
			//expected output--> pune 28.5
			
			int sum=0,count=0;
			
			for(IntWritable iw:values)
			{
				count++;
				int i=iw.get();
				
				sum=sum+i;
			}
			
			float avg=sum/count;
			
			FloatWritable avgTemp=new FloatWritable(avg);
			
			ctx.write(key,avgTemp);
			
		}
	}

	
	
	
	
	
	public static void main(String[] args) throws Exception
	{
		   Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Weather Average Edureka");
		    job.setJarByClass(WeatherAvg.class);
		    job.setMapperClass(WeatherAvgMapper.class);
		    
		    job.setReducerClass(WeatherAvgReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    
		    job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0])); //--> input file-- data set(HDFS)
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));// --> place where my out put will be stored(HDFS)
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
