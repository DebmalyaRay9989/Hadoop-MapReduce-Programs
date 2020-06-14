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

//import co.edureka.MaxSalaryEmployee.Mymap;
//import co.edureka.MaxSalaryEmployee.MyRed;



public class MaxStockPrice
{
	

public static class Mymap extends Mapper<LongWritable,Text,Text,Text>    
 {  
  public void map(LongWritable k,Text v, Context con)throws IOException, InterruptedException  
  {  
   String line = v.toString();  
   String[] w = line.split(",");  
   float high_price = 0;
   try {
   high_price = Float.parseFloat(w[4]);  
   }
   catch (NumberFormatException e) {
	    // s is not a valid integer
	}
 
   
   String exchange = w[0];
   String symbol = w[1];
   //String DeptNo = w[7];
   
 
   con.write(new Text(exchange), new Text(exchange+","+high_price+","+symbol));  
  
   
   }  
 } 

 public static class MyRed extends Reducer<Text,Text,FloatWritable,Text>  
 {  
  public void reduce(Text k, Iterable<Text> vlist, Context con) throws IOException , InterruptedException
     {  
      float max=0; 
      String rest = null;
      
      for(Text v:vlist)  
      {
        String line = v.toString();  
        String[] w=line.split(","); 
        float high_price = 0;
        try {
        	high_price=Float.parseFloat(w[1]);
        }
        catch (NumberFormatException e) {
    	    // s is not a valid integer
    	}
        
        //max=Math.max(max, sal);
       //max=Math.max(max, high_price);
        max = Math.max(max, high_price);
        rest = w[0]+","+w[2];
      }  
     

   con.write(new FloatWritable(max), new Text(rest));  
  }

 }
 
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "All Time High Price of each stock :");
		job.setJarByClass(MaxStockPrice.class);
		job.setMapperClass(Mymap.class);
 		job.setReducerClass(MyRed.class);

 		job.setMapOutputKeyClass(Text.class);
 		job.setMapOutputValueClass(Text.class);
			
 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(Text.class);

		 //   job.setOutputValueClass(FloatWritable.class);
 		
		FileInputFormat.addInputPath(job, new Path(args[0])); //--> input file-- data set(HDFS)
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// --> place where my out put will be stored(HDFS)
		    
		    
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}