

package co.edureka;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class Counter extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	enum SENTENCES_COUNTER {
		DUNCAN,
		MALCOLM
		}
	
    
    Text outkey = new Text();
	IntWritable outvalue = new IntWritable();

 // @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    if(value.toString().contains("DUNCAN")){
        context.getCounter(SENTENCES_COUNTER.DUNCAN).increment(1);
    }else if(value.toString().contains("MALCOLM")){
        context.getCounter(SENTENCES_COUNTER.MALCOLM).increment(1);
    }
    context.write(NullWritable.get(),NullWritable.get());
    
  }
}

