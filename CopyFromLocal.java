package hadoop.training.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyFromLocal {

	public void copyFromLocal(String source, String destination) throws IOException{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		Path sourcePath = new Path(source);
		Path destinationPath = new Path(destination);
		String fileName = source.substring(source.lastIndexOf('/') + 1, source.length());
		if(fs.exists(destinationPath)){
			System.out.println("File already exists");
			return;
		}
		try{
			fs.copyFromLocalFile(sourcePath, destinationPath);
			System.out.println("File copied from " + sourcePath + " to " + destinationPath);
		}catch(Exception e){
			System.err.println("Exception caught ");
			System.exit(1);
		}
		finally{
			fs.close();
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CopyFromLocal local = new CopyFromLocal();
		String localPath = args[0];
		String hdfsPath = args[1];
		try {
			local.copyFromLocal(localPath, hdfsPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
