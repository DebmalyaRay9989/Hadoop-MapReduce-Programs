package co.edureka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
			String dt;
			int subdt;
			String fileName1 = "fileName1.txt";
			String fileName2 = "fileName2.txt";
			String fileName3 = "fileName3.txt";
			String fileName4 = "fileName4.txt";
			
			
			/*File fileName1 = new File("fileName1.txt");
			if (!fileName1.exists()) {
				fileName1.createNewFile();
            }
			File fileName2 = new File("fileName2.txt");
			if (!fileName2.exists()) {
				fileName2.createNewFile();
            }
			File fileName3 = new File("fileName3.txt");
			if (!fileName3.exists()) {
				fileName3.createNewFile();
            }
			File fileName4 = new File("fileName4.txt");
			if (!fileName4.exists()) {
				fileName4.createNewFile();
            }*/
			
			
			try {
				
				
				FileWriter filewriter1 = new FileWriter(fileName1,true);
				FileWriter filewriter2 = new FileWriter(fileName2,true);
				FileWriter filewriter3 = new FileWriter(fileName3,true);
				FileWriter filewriter4 = new FileWriter(fileName4,true);
				BufferedWriter bufferedwriter1 = new BufferedWriter(filewriter1);
				BufferedWriter bufferedwriter2 = new BufferedWriter(filewriter2);
				BufferedWriter bufferedwriter3 = new BufferedWriter(filewriter3);
				BufferedWriter bufferedwriter4 = new BufferedWriter(filewriter4);
				
   			for (Text var : key) 
			{
					country=var.toString();
					String[] info = record.toString().split(",");
					if(country.matches("India")) {
						dt = info[0];
						subdt = Integer.parseInt(dt.substring(0, 3));
						if ((subdt >= 1700)  && (subdt <= 1799)) {
							out = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6];
							bufferedwriter1.write(out);
							con.write(new Text(country), new Text(out));
							
						}
						if ((subdt >= 1800)  && (subdt <= 1899)) {
							out = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6];
							bufferedwriter2.write(out);	
							con.write(new Text(country), new Text(out));
						}
						if ((subdt >= 1900)  && (subdt <= 1999)) {
							out = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6];
							bufferedwriter3.write(out);	
							con.write(new Text(country), new Text(out));
						}
						if (subdt >= 2000) {
							out = info[0]+","+info[1]+","+info[2]+","+info[3]+","+info[5]+","+info[6];
							bufferedwriter4.write(out);	
							con.write(new Text(country), new Text(out));
						}
						
						//con.write(key, new Text(out));
						
					}
					//con.write(new Text(country), new Text(out));
					
   			}
   			
   			//String out = "Total Male: " + male_count + " :: " + "Total Female: " + female_count;
   			bufferedwriter1.close();
   			bufferedwriter2.close();
   			bufferedwriter3.close();
   			bufferedwriter4.close();
			
		}
		
	    catch(IOException ex) {
	    	System.out.println("Error in writing the file");
	    }
			
		}
	}
	

	
	
	public static void main(String[] args) throws Exception
	{
		   Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Weather Records Country ");
		    
		    //   job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0])); //--> input file-- data set(HDFS)
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));// --> place where my out put will be stored(HDFS)
		   
		    job.setJarByClass(WeatherRecords.class);
			job.setMapperClass(WeatherRecordsMapper.class);
    		job.setReducerClass(WeatherRecordsReducer.class);

    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(Text.class);
			
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);

		
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
