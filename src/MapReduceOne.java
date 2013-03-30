import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * MapReduce job that takes Impressions and Clicks files
 * and outputs {referrer WalrusSeperator adID, (0/1) WasClicked}
 */
public class MapReduceOne extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapReduceOne(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = new Job(conf, "MapReduceOne");

		job.setJarByClass(MapReduceOne.class);
		job.setMapperClass(MapReduceOne.MapperOne.class);
		job.setReducerClass(MapReduceOne.ReducerOne.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}


	/**
	 * map: (LongWritable, Text) --> (Text, Text)
	 */
	public static class MapperOne extends
	Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			JSONObject array = (JSONObject)JSONValue.parse(val.toString());
			String impId,adId,ref;
			impId = (String) array.get("impressionId");
			adId = (String) array.get("adId");
			ref = (String) array.get("referrer");
			ref = ref==null?"NOREF":ref;
			
			Text outKey = new Text(impId);
			Text outVal = new Text(ref+"WalrusSeparator"+adId);
			
			context.write(outKey, outVal);
		}
	}

	/**
	 * reduce: (Text, Text[]) --> (Text, Text)
	 */
	public static class ReducerOne extends
	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			Text ref=new Text();
			Text adId=new Text();
			int clicks = 0;
			int views = 0;
			
			for (Text val : value) {
				String[] s = val.toString().split("WalrusSeparator");
				if("NOREF".equals(s[0])){
					clicks++;
				}else{
					adId.set(s[1]);
					ref.set(s[0]);
					views++;
				}
			}
			final Text outKey = new Text(ref.toString()+"WalrusSeparator"+adId.toString());
			final Text ZERO = new Text("0");
			final Text ONE = new Text("1");
			for(int i=0;i<clicks;i++){
				context.write(outKey,ONE);
			}
			for(int i=0;i<(views-clicks);i++){
				context.write(outKey,ZERO);
			}
			
		}
	}
}