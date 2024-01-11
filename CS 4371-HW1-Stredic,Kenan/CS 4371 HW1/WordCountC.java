import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountC {

    public static class Mappy
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            line = line.replaceAll("&.*?\\w+;", " ")
                    .replaceAll("[^a-zA-Z0-9 ]", " ")
                    .replaceAll("\\s+", " ");
            if (line != null && !line.trim().isEmpty()) {
                String[] mydata = line.split(" ");   // split the text to words

                for (String data : mydata) {
                    word.set(data); // set word as each input keyword
                    context.write(word, one); // create a pair <keyword, 1>
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private Map<Text, Integer> topWordCounts = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result); // create a pair <keyword, number of occurrences>

            Text word = new Text(key);
            topWordCounts.put(word, sum);

            if (topWordCounts.size() > 10) {
                // Remove the word with the smallest count to maintain the top 10.
                int smallestCount = Integer.MAX_VALUE;
                Text wordToRemove = null;

                for (Map.Entry<Text, Integer> entry : topWordCounts.entrySet()) {
                    if (entry.getValue() < smallestCount) {
                        smallestCount = entry.getValue();
                        wordToRemove = entry.getKey();
                    }
                }

                topWordCounts.remove(wordToRemove);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, Integer> entry : topWordCounts.entrySet()) {
                Text word = entry.getKey();
                int count = entry.getValue();
                context.write(word, new IntWritable(count));
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCountC <in> <out>");
            System.exit(2);
        }

        // create a job with name "wordcount"
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCountC.class);
        job.setMapperClass(Mappy.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(IntWritable.class);
        // set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        // Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
