package co.edureka;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxOccuredCounter extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Configuration(), new MaxOccuredCounter(), args);
	    System.exit(exitCode);
	  }

 
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.println("Usage: Record Check <input dir> <output dir>\n");
      return -1;
    }
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Counter By Dialogues:");
   // job.setJarByClass(CounterDriver.class);
    job.setJobName("Custom Counter Job");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // This is a map-only job, so we do not call setReducerClass.
    job.setJarByClass(MaxOccuredCounter.class);
	job.setMapperClass(Counter.class);
	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(NullWritable.class);
		
    /*
     * Set the number of reduce tasks to 0. 
     */
    job.setNumReduceTasks(0);

    boolean success = job.waitForCompletion(true);
    if (success) {

      Counters counters = job.getCounters();
	  System.out.println("Total Number of DUNCAN records = "+counters.findCounter(Counter.SENTENCES_COUNTER.DUNCAN));
	  System.out.println("Total Number of MALCOLM records = "+counters.findCounter(Counter.SENTENCES_COUNTER.MALCOLM));
	  
      return 0;
    } else
      return 1;
  }

  
}


