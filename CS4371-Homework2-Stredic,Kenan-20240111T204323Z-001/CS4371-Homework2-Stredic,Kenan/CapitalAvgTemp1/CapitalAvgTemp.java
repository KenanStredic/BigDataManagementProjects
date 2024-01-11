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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CapitalAvgTemp {
	
	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
		private boolean firstRow = true;
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        if (firstRow) {
        		firstRow = false;
        		return;
        	}

	        String[] parts = line.split(",");
	        String city = parts[3];
	        String country = parts[1];
	        double avgTemperature = Double.valueOf(parts[7]);

	        context.write(new Text(country), new Text(city + "#" + avgTemperature));
	            
	        }
	    }
	
	
	public static class TemperatureMapper2 extends Mapper<LongWritable, Text, Text, Text>{
		private boolean firstRow = true;
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	     
	        if (firstRow) {
        		firstRow = false;
        		return;
        	}
	        String[] parts = line.split(",");

	        	String capital = parts[1];
			    capital = capital.replace("\"", "");
			    String country = parts[0];
			    country = country.replace("\"", "");
	         
	            context.write(new Text(country), new Text(capital));
	        }
	    }

	public static class TemperatureReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	    @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
	        double sum = 0.0;
	        Double totalAvg = 0.0;
	        int count = 0;

	        String capital = "";
	           
	           for (Text val : values) {
	        	   if (val.toString().indexOf("#") == -1) {
	        		   capital = val.toString().toLowerCase().trim();
	        		   break;
	        	   }
	           }
	           
	           for (Text val : values) {
	        	   if (val.toString().indexOf("#") != -1) {
	        		   String[] words = val.toString().split("#");
	        		   String city = words[0];
	        		   if (city.toLowerCase().trim().equals(capital)) {
	        			   Double temp = Double.valueOf(words[1]);
	        			   sum = sum + temp;
	        			   count = count + 1;
	        		   }
	        	   }
	           }
	           
	           String output = key + "," + capital;
	           totalAvg = sum / count;
	           if (!totalAvg.isNaN()) {
	        	   context.write(new Text(output), new DoubleWritable(totalAvg));
	           }
	           
	        }
	    }

  // Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	      // Create a job
	      Job job = Job.getInstance(conf, "Temperature Analysis");
	      job.setJarByClass(CapitalAvgTemp.class);

	      // Set the Mapper and Reducer classes
	      //job.setMapperClass(TemperatureMapper.class);
	      job.setReducerClass(TemperatureReducer.class);

	      // Set the output key and value classes for the Mapper and Reducer
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TemperatureMapper.class);
	      MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, TemperatureMapper2.class);
	      // Set output path
	      FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	     
	      // Run the job and wait for completion
	      System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}
