import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


public class ClickRate {
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Error: Wrong number of parameters.");
			System.exit(1);
		}
		String tempDir = "temp";
		String outDir = args[2];
		args[2]=tempDir;
		int res = ToolRunner.run(new Configuration(), new MapReduceOne(), args);
		if(res==0){
			args[0]=tempDir;
			args[2]=outDir;
			res = ToolRunner.run(new Configuration(), new MapReduceTwo(), args);
		}
		System.exit(res);
	}
}
