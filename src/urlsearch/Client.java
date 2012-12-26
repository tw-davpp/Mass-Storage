package urlsearch;

import java.io.File;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Client {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] params = new String[2];
		params[0] = "./input";
		params[1] = "./output";

		FileManager.checkOutputFile(new File(params[1]));
		
		String[] otherArgs = new GenericOptionsParser(conf, params)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: urlsearch <in> <out>");
			System.exit(2);
		}

		Scanner sc = new Scanner(System.in);
		Filter filter = new UrlFilter(sc.nextLine());
		UrlMapper.setFilter(filter);
		
		Job job = new Job(conf, "url search");
		job.setJarByClass(Client.class);
		job.setMapperClass(UrlMapper.class);
		job.setCombinerClass(UrlReducer.class);
		job.setReducerClass(UrlReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
