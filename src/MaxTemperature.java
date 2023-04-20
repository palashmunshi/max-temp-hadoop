import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class MaxTemperature {

  // The mapper class reads each line of the input file, splits it into fields, and emits a key-value pair.
  public static class MaxTemperatureMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text city = new Text();
    private DoubleWritable temperature = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Split the line into fields using comma as the delimiter
      String[] fields = value.toString().split(",");
      if (fields.length == 2) {
        // The first field is the city name, and the second field is the temperature value
        city.set(fields[0]);
        temperature.set(Double.parseDouble(fields[1]));
        // Emit the city as the key and the temperature as the value
        context.write(city, temperature);
      }
    }
  }

  // The reducer class receives a key-value pair, where the key is a city name, and the value is a list of temperatures
  // associated with that city. It computes the maximum temperature for each city and emits the result.
  public static class MaxTemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double maxTemperature = Double.NEGATIVE_INFINITY;
      // Loop over all the temperature values associated with the current city key and find the maximum temperature
      for (DoubleWritable temperature : values) {
        maxTemperature = Math.max(maxTemperature, temperature.get());
      }
      // Emit the city as the key and the maximum temperature as the value
      context.write(key, new DoubleWritable(maxTemperature));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "max temperature");

    // Set the main class for the job
    job.setJarByClass(MaxTemperature.class);

    // Set the input and output file formats
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[0]));

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    // Set the mapper and reducer classes
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    // Set the output key and value types for the mapper and reducer
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    // Submit the job and wait for completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
