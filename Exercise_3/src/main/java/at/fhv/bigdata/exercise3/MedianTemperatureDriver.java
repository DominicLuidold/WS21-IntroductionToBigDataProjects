package at.fhv.bigdata.exercise3;

import at.fhv.bigdata.exercise3.median.MedianTemperatureGroupingComparator;
import at.fhv.bigdata.exercise3.median.MedianTemperatureMapper;
import at.fhv.bigdata.exercise3.median.MedianTemperaturePartitioner;
import at.fhv.bigdata.exercise3.median.MedianTemperatureReducer;
import at.fhv.bigdata.exercise3.median.YearTemperaturePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MedianTemperatureDriver extends Configured implements Tool {

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
        Job job = Job.getInstance(conf, "Find the median temperature for a given year with secondary sorting");
        job.setJarByClass(getClass());

        // Specify various job-specific parameters

        // Set the input file path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the mapper, reducer, grouping comparator and partitioner classes
        job.setMapperClass(MedianTemperatureMapper.class);
        job.setReducerClass(MedianTemperatureReducer.class);

        job.setPartitionerClass(MedianTemperaturePartitioner.class);
        job.setGroupingComparatorClass(MedianTemperatureGroupingComparator.class);

        // Set the type of the key in the output
        job.setOutputKeyClass(YearTemperaturePair.class);

        // Set the type of the value in the output
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MedianTemperatureDriver(), args);
        System.exit(exitCode);
    }
}
