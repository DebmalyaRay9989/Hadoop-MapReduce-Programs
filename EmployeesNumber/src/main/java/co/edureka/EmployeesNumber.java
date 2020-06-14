package co.edureka;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.*;
import java.io.*;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmployeesNumber {


    public static class EmployeesMapper extends Mapper < Object, Text, Text, Text > {
        String DEPTNO = null;
        String EMPNO = null;
        String SALARY = null;

        public void DepartmentMapper(Object key,Text value,Context context) throws IOException,InterruptedException 
        {
        	    String FILE_TAG = "F1";
        	    String line = value.toString();
                String splitarray[] = line.split(",");
                DEPTNO = splitarray[7].trim();
                EMPNO = splitarray[0].trim();
                SALARY = splitarray[5].trim();
                context.write(new Text(FILE_TAG+DEPTNO),new Text(EMPNO + "," + SALARY));
            }
    }

    public static class DepartmentMapper2 extends Mapper < Object, Text, Text, Text > {
    	String DEPTNO2 = null;
    	String LOCATION = null;
     //   String tag = "Delivery~";

        public void map(Object key,Text value,Context context) throws IOException,InterruptedException 
        {
        	    String FILE_TAG = "F2";
        	    String line = value.toString();
                String splitarray[] = line.split(",");
                DEPTNO2 = splitarray[0].trim();
                LOCATION = splitarray[2].trim();
                context.write(new Text(FILE_TAG+DEPTNO2),new Text(LOCATION));
            }
    }

    public static class EmployeesReducer extends Reducer < Text, Text, Text, Text > {
    //    private String customerName;
    //    private String deliveryReport;
        int EMPCOUNT = 0;
        int TOTALSAL = 0;

        public void reduce(Text key,Iterable < Text > values,Context context) throws IOException,InterruptedException
        {
        	HashMap<Integer, String> hm = new HashMap<Integer, String>();
        	HashMap<Integer, String> hm1 = new HashMap<Integer, String>();
        	
            String Dept = null;
            String FILE_TAG1 = "F1";
            String FILE_TAG2 = "F2";
        	for(Text t : values) {
                String currValue = t.toString();
                String valueSplitted[] = currValue.split(",");
                Dept = valueSplitted[0];
                
               // String SubDept = Dept.substring(0, 1);
                
                if (Dept.contains(FILE_TAG1))
                {

                	int lastindex = Dept.lastIndexOf(Dept);
                	int SubStrDept = Integer.parseInt(Dept.substring(2, lastindex));
                	String EmpIDSalary = valueSplitted[1]+","+valueSplitted[2];		
                	
                	hm.put(SubStrDept, EmpIDSalary);
                	
                	
                }
                
                else if (Dept.contains(FILE_TAG2)) 
                {
                	int lastindex2 = Dept.lastIndexOf(Dept);
                	int SubStrDept2 = Integer.parseInt(Dept.substring(2, lastindex2));
                	String location = valueSplitted[1];
                			
                	
                	hm1.put(SubStrDept2, location);
                	
                }
                                     
                
            }
        	
        //  Iterator usage....	
        	Iterator<Integer> it = hm.keySet().iterator();
        	Iterator<Integer> it1 = hm1.keySet().iterator();
        	
        	while (it.hasNext()) {
        		int p = it.next();
        		
        		while (it1.hasNext()) {
        			int p1 = it.next();
        			
        			if (p == p1) {
        				
        				
        				String EmpSal = hm.get(p);
        				String EMPSAL [] = EmpSal.split(",");
        				String EMPID  = EMPSAL[0];
        				EMPCOUNT ++;
        				int SAL  = Integer.parseInt(EMPSAL[1]);
        				TOTALSAL   += SAL;
        				
        				
        			}
        		}
        		context.write(new Text(key), new Text(EMPCOUNT+","+TOTALSAL));
        	}
        	
        	
            
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
      //  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
    /*    if (otherArgs.length != 3) {
            System.err.println("Usage: TimeDifference <in1> <in2> <out>");
            System.exit(2);
        }  */
        
        Job job = new Job(conf, "Employees Count and Total Salary ");
        job.setJarByClass(EmployeesNumber.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, EmployeesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, EmployeesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setReducerClass(EmployeesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

