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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
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

            String[] items = fields[1].trim().split(",");
            for (String item : items) {
                String cleanItem = item.trim();
                if (!cleanItem.isEmpty()) {
                    // Key format: item
                    context.write(new Text(cleanItem), ONE);
                }
            }
        }
    }

    public static class Phase1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int minSupport;
        private int totalTransactions = 43328;

        @Override
        protected void setup(Context context) {
            minSupport = context.getConfiguration().getInt("minSupport", 2);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Count total transactions
//            totalTransactions++;

            if (sum >= minSupport) {
                context.write(key, new IntWritable(sum));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getConfiguration().setInt("totalTransactions", totalTransactions);
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
                    context.write(new Text(String.join(",", sortedItems)), ONE);
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
        private int totalTransactions;
        private Map<String, Integer> itemSupportMap = new HashMap<>();

        @Override
        protected void setup(Context context) {
            minSupport = context.getConfiguration().getInt("minSupport", 2);
            totalTransactions = context.getConfiguration().getInt("totalTransactions", 43328);

            // Load item support from Phase 1 output into the map
            try {
                Path phase1OutputPath = new Path("hdfs://localhost:9000/mba/output_assoc/frequent1"); 
                FileSystem fs = FileSystem.get(context.getConfiguration());
                FileStatus[] statuses = fs.listStatus(phase1OutputPath);
                for (FileStatus status : statuses) {
                    if (status.isFile()) {
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                        String line;
                        while ((line = br.readLine()) != null) {
                            // Split the line by space(s)
                            String[] parts = line.split("\t");

                            if (parts.length >= 2) {
                                String item = parts[0].trim();
//                                System.out.println(item);// Key part like "10"
                                int support = Integer.parseInt(parts[1]); 
//                                System.out.println(support);// Last part is the support count

                                itemSupportMap.put(item, support);
                            }
                        }
                        br.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
//                System.out.println(sum);
            }
           

            if (sum >= minSupport) {
                // Support calculation
                double support = (double) sum / totalTransactions;

                // Confidence and Lift calculation
                String[] items = key.toString().split(",");
                if (items.length > 1) {
                    String itemset = String.join(",", items);
                    double confidence = getConfidence(items[0], support);
                    double lift = getLift(items[0], items[1], confidence);

                    // Avoid Infinity by checking for zero values in support
                    if (Double.isInfinite(confidence) || Double.isInfinite(lift)) {
                        confidence = 0.0; // or handle as per your requirement
                        lift = 0.0; // or handle as per your requirement
                    }

                    context.write(new Text(itemset + " | Support: " + support + " | Confidence: " + confidence + " | Lift: " + lift), new IntWritable(sum));
                }
            }
        }

        private double getConfidence(String item, double itemsetSupport) {
            Integer itemSupport = itemSupportMap.get(item);
            if (itemSupport != null && itemSupport > 0) {
                return itemsetSupport / (double) itemSupport;
            }
            return 0.0; // If item support is zero or not found, return zero to avoid division by zero
        }

        private double getLift(String item1, String item2, double confidence) {
            Integer item1Support = itemSupportMap.get(item1);
            Integer item2Support = itemSupportMap.get(item2);

            if (item1Support != null && item2Support != null && item1Support > 0 && item2Support > 0) {
                double lift = confidence / ((double) item1Support / totalTransactions * (double) item2Support / totalTransactions);
                return lift;
            }

            return 0.0; // If support for either item is zero, return zero
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
        Job job1 = Job.getInstance(conf, "Frequent 1-itemsets");
        job1.setJarByClass(AprioriDriver.class);

        job1.setMapperClass(Phase1Mapper.class);
        job1.setReducerClass(Phase1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path phase1Output = new Path(args[1] + "/frequent1");
        FileOutputFormat.setOutputPath(job1, phase1Output);

        job1.setNumReduceTasks(1); // One reducer for phase 1

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Frequent k-itemsets
        Job job2 = Job.getInstance(conf, "Frequent k-itemsets");
        job2.setJarByClass(AprioriDriver.class);

        job2.setMapperClass(Phase2Mapper.class);
        job2.setReducerClass(Phase2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        Path phase2Output = new Path(args[1] + "/frequent2");
        FileOutputFormat.setOutputPath(job2, phase2Output);

        job2.setNumReduceTasks(1); // Only one reducer for phase 2

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

