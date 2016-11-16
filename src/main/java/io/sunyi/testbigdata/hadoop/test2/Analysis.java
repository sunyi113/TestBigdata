package io.sunyi.testbigdata.hadoop.test2;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author sunyi
 */
public class Analysis {

	public static final Logger logger = LoggerFactory.getLogger(Analysis.class);


	public static class AnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split(" ");
			String user = split[0];
			String amount = split[1];

			context.write(new Text(user), new IntWritable(Integer.valueOf(amount)));
		}
	}


	public static class AnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			IntWritable max = new IntWritable(0);

			for (IntWritable intWritable : values) {
				if (intWritable.compareTo(max) > 0) {
					max = intWritable;
				}
			}

			logger.info(key.toString() + ": " + max.get());

			context.write(key, max);


		}
	}

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("HADOOP_USER_NAME", "root");

		// hadoop 配置 core-site.xml
		//<configuration>
		//    <property>
		//        <name>fs.defaultFS</name>
		//        <value>hdfs://dev127:9000</value>
		//    </property>
		//</configuration>
		//


		//输入路径
		String dst = "hdfs://dev127:9000/test/2/input/data.txt";

		//输出路径，必须是不存在的，空文件夹也不行。
		String dstOut = "hdfs://dev127:9000/test/2/output/01";

		Configuration hadoopConfig = new Configuration();

		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		Job job = Job.getInstance(hadoopConfig, "Analysis");

		FileInputFormat.addInputPath(job, new Path(dst));
		FileOutputFormat.setOutputPath(job, new Path(dstOut));

		job.setMapperClass(AnalysisMapper.class);
		job.setReducerClass(AnalysisReducer.class);

		//设置最后输出结果的Key和Value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//执行job，直到完成
		job.waitForCompletion(true);

		System.out.println("Finished");
	}
}