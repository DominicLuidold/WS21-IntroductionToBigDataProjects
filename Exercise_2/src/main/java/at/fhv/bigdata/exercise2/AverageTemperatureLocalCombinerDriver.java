package at.fhv.bigdata.exercise2;

import at.fhv.bigdata.exercise2.combiner.AverageTemperatureCombiner;
import at.fhv.bigdata.exercise2.combiner.AverageTemperatureMapper;
import at.fhv.bigdata.exercise2.combiner.AverageTemperaturePair;
import at.fhv.bigdata.exercise2.combiner.AverageTemperatureReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperatureLocalCombinerDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //Provides access to configuration parameters.
        Configuration conf = new Configuration();

        //GenericOptionsParser is a utility to parse command line arguments generic to the Hadoop framework.
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);

        String[] remainingArgs = optionsParser.getRemainingArgs();

        if (remainingArgs.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());

            //A utility to help run Tools.
            ToolRunner.printGenericCommandUsage(System.err);

            System.exit(2);
        }

        // Create a new Job
        Job job = Job.getInstance(conf, "Find average temperature for a given year with local combining");
        job.setJarByClass(getClass());

        // Specify various job-specific parameters

        // Set the input file path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the mapper, reducer and combiner classes
        job.setMapperClass(AverageTemperatureMapper.class);
        job.setCombinerClass(AverageTemperatureCombiner.class);
        job.setReducerClass(AverageTemperatureReducer.class);

        // Set the type of the key in the output
        job.setOutputKeyClass(Text.class);

        // Set the type of the value in the output
        job.setOutputValueClass(AverageTemperaturePair.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AverageTemperatureLocalCombinerDriver(), args);
        System.exit(exitCode);
    }
}
