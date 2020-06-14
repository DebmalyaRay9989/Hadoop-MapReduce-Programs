package co.edureka;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		IntWritable one=new IntWritable(1);
		Text word=new Text();
		
		public void map(LongWritable key,Text value, Context ctx) throws IOException,InterruptedException
		{
			//str-- dear bear
			String str=value.toString();
			
			String[] data=str.split(" ");
			//data-- [0]-dear [1]- bear
			
			for(String s:data)
			{
				word.set(s);
				ctx.write(word, one);
			}
			
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException,InterruptedException
		{
			//key --> dear
			//value--> <1,1>
			
			//expected output--> dear 2
			
			int sum=0;
			
			for(IntWritable iw:values)
			{
				int i=iw.get();
				sum=sum+i;
			}
			
			ctx.write(key, new IntWritable(sum));
			
		}
	}

	
	
	
	
	
	public static void main(String[] args) throws Exception
	{
		   Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "WordCount Edureka");
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(WordCountMapper.class);
		    
		    job.setReducerClass(WordCountReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0])); //--> input file-- data set(HDFS)
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));// --> place where my out put will be stored(HDFS)
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
