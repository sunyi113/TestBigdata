package io.sunyi.testbigdata.hadoop.test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author sunyi
 */
public class TestMapReduce {


	public static void main(String args[]) throws IOException {

		String uri = "hdfs://192.168.1.127:9000/";
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), config);


		// 列出 hdfs 上/user/sunyi/目录下的所有文件和目录

		Path home = new Path("/test/");

		if (!fs.exists(home)) {
			fs.mkdirs(home);
		}
//
//		FileStatus[] statuses = fs.listStatus(home);
//		for (FileStatus status : statuses) {
//			System.out.println(status);
//		}
//
//
		// 在hdfs的/user/sunyi目录下创建一个文件，并写入一行文本
		FSDataOutputStream os = fs.create(new Path(home + "/15.log"));
		os.write("Hello World!".getBytes());
		os.flush();


		fs.close();

//		Path file = new Path(home + "/03.log");
//
////		FSDataOutputStream os = fs.append(file);
//
//		FSDataOutputStream os = fs.create(file);
//
//		os.write("Hello World!".getBytes());
//		os.writeUTF("你好！！");
//
//
//
//		fs.close();


//		 显示在hdfs的/user/sunyi下指定文件的内容
//		InputStream is = fs.open(new Path(home + "/a"));
//		IOUtils.copyBytes(is, System.out, 1024, true);


	}


}