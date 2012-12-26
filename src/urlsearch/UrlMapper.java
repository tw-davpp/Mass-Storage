package urlsearch;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UrlMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private static Filter filter;
	
	public static void setFilter(Filter filter){
		UrlMapper.filter = filter;
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Scanner sc = new Scanner(value.toString());
		while (sc.hasNext()) {
			String str = sc.next();
			if (filter.remain(str)) {
				word.set(str);
				context.write(word, one);
			}
		}
	}

}
