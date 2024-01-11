import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AsiaAvgTemp {
	
	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	     // Check if the line contains headers and skip it
	        if (line.contains("AvgTemperature")) {
	            return;
	        }

	        String[] parts = line.split(",");

	        if (parts.length == 8) {
	            String region = parts[0];
	            String country = parts[1];
	            String year = parts[6];

	            try {
	                double avgTemperature = Double.parseDouble(parts[7]);

	                // Only consider countries in the "Asia" region
	                if ("Asia".equals(region)) {
	                    // Emit the country and year as the key and the average temperature as the value
	                    context.write(new Text(country + "_" + year), new DoubleWritable(avgTemperature));
	                }
	            } catch (NumberFormatException e) {
	                // Handle the case where the value cannot be parsed as a double
	                // You can choose to skip, log, or handle such cases as needed.
	            }
	        }
	    }
	}

	public static class TemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    @Override
	    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	            throws IOException, InterruptedException {
	        String[] parts = key.toString().split("_");

	        if (parts.length == 2) {
	            String country = parts[0];
	            String year = parts[1];

	            double sum = 0.0;
	            int count = 0;

	            // Calculate the average temperature for the country and year
	            for (DoubleWritable value : values) {
	                sum += value.get();
	                count++;
	            }

	            if (count > 0) {
	                double averageTemperature = sum / count;
	                // Emit the country, year, and average temperature as the value
	                context.write(new Text(country + "_" + year), new DoubleWritable(averageTemperature));
	            }
	        }
	    }
	}


  // Driver program
  public static void main(String[] args) throws Exception {
      if (args.length != 2) {
          System.err.println("Usage: AsiaAvgTemp <input path> <output path>");
          System.exit(-1);
      }

      // Create a Hadoop configuration
      Configuration conf = new Configuration();

      // Create a job
      Job job = Job.getInstance(conf, "Temperature Analysis");
      job.setJarByClass(AsiaAvgTemp.class);

      // Set the Mapper and Reducer classes
      job.setMapperClass(TemperatureMapper.class);
      job.setReducerClass(TemperatureReducer.class);

      // Set the output key and value classes for the Mapper and Reducer
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(DoubleWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);

      // Set input and output paths
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // Run the job and wait for completion
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
