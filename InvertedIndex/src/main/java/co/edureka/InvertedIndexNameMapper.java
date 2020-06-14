package co.edureka;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndexNameMapper extends Mapper<Object, Text, Text, Text> {
private Text nameKey = new Text();
private Text fileNameValue = new Text();
@Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
String data = value.toString();
String[] field = data.split(",", -1);
String firstName = null;
if (null != field && field.length == 9 && field[0].length() > 0) {
firstName=field[0];
String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
nameKey.set(firstName);
fileNameValue.set(fileName);
context.write(nameKey, fileNameValue);

}
}
}
