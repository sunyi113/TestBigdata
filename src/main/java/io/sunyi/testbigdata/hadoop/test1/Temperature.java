package io.sunyi.testbigdata.hadoop.test1;

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

import java.io.IOException;


public class Temperature {

	/**
	 * 四个泛型类型分别代表：
	 * <p/>
	 * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
	 * <p/>
	 * ValueIn      Mapper的输入数据的Value，这里是每行文字
	 * <p/>
	 * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
	 * <p/>
	 * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
	 */
	static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String year = line.substring(0, 4);

			int temperature = Integer.parseInt(line.substring(8));

			context.write(new Text(year), new IntWritable(temperature));

		}
	}


	/**
	 * 四个泛型类型分别代表：
	 * <p/>
	 * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
	 * <p/>
	 * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
	 * <p/>
	 * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
	 * <p/>
	 * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
	 */
	static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int maxValue = Integer.MIN_VALUE;

			//取values的最大值
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}

			context.write(key, new IntWritable(maxValue));

		}
	}


	public static void main(String[] args) throws Exception {

		//输入路径
		String dst = "hdfs://192.168.1.127:9000/temperature/input/data01.txt";

		//输出路径，必须是不存在的，空文件夹也不行。
		String dstOut = "hdfs://localhost:9000/temperature/output/01";

		Configuration hadoopConfig = new Configuration();

		hadoopConfig.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		Job job = Job.getInstance(hadoopConfig, "Temperature");


		FileInputFormat.addInputPath(job, new Path(dst));
		FileOutputFormat.setOutputPath(job, new Path(dstOut));


		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);


		//设置最后输出结果的Key和Value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);


		//执行job，直到完成
		job.waitForCompletion(true);


		System.out.println("Finished");

	}
}