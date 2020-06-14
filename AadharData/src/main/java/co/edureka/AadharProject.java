package co.edureka;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AadharProject
{

	public static class MapperClass extends Mapper<Object, Text, Text, Text> 
	{

 		public void map(Object key, Text record, Context con) 
			throws IOException, InterruptedException 
		{
  			String[] info = record.toString().split(",");
			String state = info[2]; // state will be the key
  			String sex = info[6]; // sex will be the value
   			con.write(new Text(state), new Text(sex));
 		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> 
	{

 		public void reduce(Text key, Iterable<Text> valueList, 
			Context con) throws IOException, InterruptedException 
		{
   			int male_count=0, female_count=0;
			String sex;
   			for (Text var : valueList) 
			{
					sex=var.toString();
					if(sex.equals("M"))
						male_count++;
					else
						female_count++;
   			}
   			String out = "Total Male: " + male_count + " :: " + "Total Female: " + female_count;
   			con.write(key, new Text(out));
 		}
	}

	public static void main(String[] args) throws Exception 
	{
    		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "AadharProject");
    		job.setJarByClass(AadharProject.class);
			
		job.setMapperClass(MapperClass.class);
    		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
