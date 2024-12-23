import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class AprioriDriver {

    public static class Phase1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("BillNo")) return;

            String[] fields = line.split(";");
            if (fields.length < 7) return;

            String country = fields[6].trim();
            String[] items = fields[1].trim().split(",");
            for (String item : items) {
                String cleanItem = item.trim();
                if (!cleanItem.isEmpty()) {
                    // Key format: country:item
                    context.write(new Text(country + ":" + cleanItem), ONE);
                }
            }
        }
    }

    public static class Phase1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int minSupport;

        @Override
        protected void setup(Context context) {
            minSupport = context.getConfiguration().getInt("minSupport", 2);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= minSupport) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static class Phase2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("BillNo")) return;

            String[] fields = line.split(";");
            if (fields.length < 7) return;

            String country = fields[6].trim();
            String[] items = fields[1].trim().split(",");
            List<String> cleanItems = new ArrayList<>();
            
            for (String item : items) {
                String cleanItem = item.trim();
                if (!cleanItem.isEmpty()) {
                    cleanItems.add(cleanItem);
                }
            }

            if (cleanItems.size() >= 2) {
                Set<Set<String>> combinations = generateCombinations(cleanItems, 2);
                for (Set<String> itemset : combinations) {
                    List<String> sortedItems = new ArrayList<>(itemset);
                    Collections.sort(sortedItems);
                    context.write(new Text(country + ":" + String.join(",", sortedItems)), ONE);
                }
            }
        }

        private Set<Set<String>> generateCombinations(List<String> items, int k) {
            Set<Set<String>> combinations = new HashSet<>();
            generateCombinationsHelper(items, new HashSet<>(), 0, k, combinations);
            return combinations;
        }

        private void generateCombinationsHelper(List<String> items, Set<String> current, int start, int k, 
                Set<Set<String>> combinations) {
            if (current.size() == k) {
                combinations.add(new HashSet<>(current));
                return;
            }
            for (int i = start; i < items.size(); i++) {
                current.add(items.get(i));
                generateCombinationsHelper(items, current, i + 1, k, combinations);
                current.remove(items.get(i));
            }
        }
    }

    public static class Phase2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int minSupport;

        @Override
        protected void setup(Context context) {
            minSupport = context.getConfiguration().getInt("minSupport", 2);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= minSupport) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static class LocationPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String[] parts = key.toString().split(":");
            String country = parts[0];

            // Assign partitions based on country
            return Math.abs(country.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AprioriDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("minSupport", 8);

        // Job 1: Frequent 1-itemsets
        Job job1 = Job.getInstance(conf, "Frequent 1-itemsets with Partitioning");
        job1.setJarByClass(AprioriDriver.class);

        job1.setMapperClass(Phase1Mapper.class);
        job1.setPartitionerClass(LocationPartitioner.class);
        job1.setReducerClass(Phase1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path phase1Output = new Path(args[1] + "/frequent1");
        FileOutputFormat.setOutputPath(job1, phase1Output);

        job1.setNumReduceTasks(3); // Adjust this based on your cluster

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Frequent k-itemsets
        conf.setInt("k", 2);
        Job job2 = Job.getInstance(conf, "Frequent k-itemsets with Partitioning");
        job2.setJarByClass(AprioriDriver.class);

        job2.setMapperClass(Phase2Mapper.class);
        job2.setPartitionerClass(LocationPartitioner.class);
        job2.setReducerClass(Phase2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/frequent2"));

        job2.setNumReduceTasks(3); // Adjust this based on your cluster

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

