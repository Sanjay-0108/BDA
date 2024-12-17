import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

	public class NetworkTraffic {

	    // Mapper Class
	    public static class TrafficMapper extends Mapper<Object, Text, Text, Text> {
	        private Text activityType = new Text();
	        private Text ipVolumePair = new Text();

	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	            String line = value.toString();
	            String[] fields = line.split("\\s+");

	            if (fields.length == 4) {
	                String ip = fields[0];
	                String dataVolume = fields[2];
	                String activity = fields[3];

	                activityType.set(activity);
	                ipVolumePair.set(ip + "," + dataVolume);
	                context.write(activityType, ipVolumePair);
	            }
	        }
	    }

	    // Reducer Class
	    public static class TrafficReducer extends Reducer<Text, Text, Text, IntWritable> {
	        public void reduce(Text key, Iterable<Text> values, Context context)
	                throws IOException, InterruptedException {
	            java.util.Map<String, Integer> ipDataMap = new java.util.HashMap<>();

	            for (Text val : values) {
	                String[] ipVolume = val.toString().split(",");
	                String ip = ipVolume[0];
	                int volume = Integer.parseInt(ipVolume[1]);

	                ipDataMap.put(ip, ipDataMap.getOrDefault(ip, 0) + volume);
	            }

	            for (java.util.Map.Entry<String, Integer> entry : ipDataMap.entrySet()) {
	                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
	            }
	        }
	    }

	    // Partitioner Class
	    public static class TrafficPartitioner extends Partitioner<Text, Text> {
	        @Override
	        public int getPartition(Text key, Text value, int numPartitions) {
	            if (key.toString().equalsIgnoreCase("download")) {
	                return 0; // File 1: Download
	            } else {
	                return 1; // File 2: Upload
	            }
	        }
	    }

	    // Driver Class
	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "network traffic");

	        job.setJarByClass(NetworkTraffic.class);
	        job.setMapperClass(TrafficMapper.class);
	        job.setReducerClass(TrafficReducer.class);
	        job.setPartitionerClass(TrafficPartitioner.class);

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);

	        job.setNumReduceTasks(2); // Two reducers for download and upload

	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	}
