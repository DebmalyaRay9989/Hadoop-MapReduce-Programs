import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.ArrayList;

import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class HdfsJavaApiDemo
{

 	public static void addFile(String source, String dest, Configuration conf) throws IOException
  	{
   		FileSystem fileSystem = FileSystem.get(conf);
    	String filename = source.substring(source.lastIndexOf('/') + 1,source.length());
    	if (dest.charAt(dest.length() - 1) != '/')
      		dest = dest + "/" + filename;
		else
      		dest = dest + filename;
    	Path path = new Path(dest);
    	if (fileSystem.exists(path))
		{
      		System.out.println("File " + dest + " already exists");
      		return;
    	}
    	FSDataOutputStream out = fileSystem.create(path);
    	InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));
    	byte[] b = new byte[1024];
    	int numBytes = 0;
    	while ((numBytes = in.read(b)) > 0)
      		out.write(b, 0, numBytes);
    	in.close();
    	out.close();
    	fileSystem.close();
	}

	public static void readFile(String file, Configuration conf) throws IOException
  	{
    	FileSystem fileSystem = FileSystem.get(conf);
    	Path path = new Path(file);
    	if (!fileSystem.exists(path))
		{
      		System.out.println("File " + file + " does not exists");
      		return;
    	}
   		FSDataInputStream in = fileSystem.open(path);
    	String filename = file.substring(file.lastIndexOf('/') + 1,file.length());
    	OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));
    	byte[] b = new byte[1024];
    	int numBytes = 0;
    	while ((numBytes = in.read(b)) > 0)
      		out.write(b, 0, numBytes);
   		in.close();
    	out.close();
   		fileSystem.close();
  	}
	

	public static void deleteFile(String file, Configuration conf) throws IOException
  	{
    	FileSystem fileSystem = FileSystem.get(conf);
    	Path path = new Path(file);
    	if (!fileSystem.exists(path))
		{
      		System.out.println("File " + file + " does not exists");
      		return;
    	}
    	fileSystem.delete(new Path(file), true);
    	fileSystem.close();
  	}

}

public class HdfsJavaApiTest
{
	public static void main( String [] a) throws Exception
	{

    	String hdfsPath = "hdfs://nameservice1", source="", dest="";

    	Configuration conf;

		int choice;

		BufferedReader br=new BufferedReader(new InputStreamReader(System.in));

		while(true)
		{
			//
			System.out.println("Enter 1 for Local to HDFS");
			System.out.println("Enter 2 for HDFS to local");
			System.out.println("Enter 3 for deletion from HDFS");
			System.out.println("Enter 4 for exit...");

			choice=Integer.parseInt(br.readLine());
	
			switch(choice)
			{
				case 1:
					System.out.println("Enter local source and HDFS destination paths...");
					source=br.readLine();
					dest=br.readLine();	
					conf = new Configuration();
    					conf.set("fs.default.name", hdfsPath);
					HdfsJavaApiDemo.addFile(source, dest, conf);
					break;	
				case 2:
					System.out.println("Enter HDFS source...");
					source=br.readLine();	
					conf = new Configuration();
    					conf.set("fs.default.name", hdfsPath);
					HdfsJavaApiDemo.readFile(source, conf);
					break;
				case 3:
					System.out.println("Enter HDFS source to be deleted...");
					source=br.readLine();	
					conf = new Configuration();
    					conf.set("fs.default.name", hdfsPath);
					HdfsJavaApiDemo.deleteFile(source, conf);
					break;
				default:
					System.out.println("Exiting...");
					return;

			}
		}


		
	}
}




