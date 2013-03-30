import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce job that takes "referrer WalrusSeperator adID" and whether or not it was clicked
 * and outputs "[page_url, ad_id] click_rate"
 */
public class MapReduceTwo extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapReduceTwo(), args);
		System.exit(res);
	}


	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = new Job(conf, "MapReduceTwo");

		job.setJarByClass(MapReduceTwo.class);
		job.setMapperClass(MapReduceTwo.MapperTwo.class);
		job.setReducerClass(MapReduceTwo.ReducerTwo.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		
		int ret = job.waitForCompletion(true) ? 0 : 1;
		FileSystem.get(conf).delete(new Path(args[0]),true);
		return ret;
	}

	/**
	 * map: (LongWritable, Text) --> (Text, LongWritable)
	 */
	public static class MapperTwo extends
	Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			String[] stuff = val.toString().split("\\s");
			
			Text outKey = new Text(stuff[0]);
			LongWritable outVal = new LongWritable("0".equals(stuff[1])?0:1);

			context.write(outKey, outVal);
		}
	}

	/**
	 * reduce: (Text, LongWritable[]) --> (Text, Text)
	 */
	public static class ReducerTwo extends
	Reducer<Text, LongWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> value,
				Context context) throws IOException, InterruptedException {
			long numberViews=0;
			long numberClicks=0;
			for (LongWritable val : value) {
				numberViews++;
				if(val.get()==1){
					numberClicks++;
				}
			}
			String[] splitKey = key.toString().split("WalrusSeparator");
			Text outKey = new Text("["+splitKey[0]+", "+splitKey[1]+"]");
			double clickRate = ((double)numberClicks)/((double)numberViews);
			Text outVal = new Text(""+clickRate);
			context.write(outKey,outVal);
		}
		
	}

}