package at.fhv.bigdata.exercise2;

import at.fhv.bigdata.exercise2.naive.AverageTemperatureMapper;
import at.fhv.bigdata.exercise2.naive.AverageTemperatureReducer;
import at.fhv.bigdata.exercise2.naive.counter.MissingMalformedCounter;
import at.fhv.bigdata.exercise2.naive.counter.QualityCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperatureDriver extends Configured implements Tool {

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
        Job job = Job.getInstance(conf, "Find average temperature for a given year");
        job.setJarByClass(getClass());

        // Specify various job-specific parameters

        // Set the input file path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the mapper, reducer and combiner classes
        job.setMapperClass(AverageTemperatureMapper.class);
        job.setReducerClass(AverageTemperatureReducer.class);

        // Set the type of the key in the output
        job.setOutputKeyClass(Text.class);

        // Set the type of the value in the output
        job.setOutputValueClass(IntWritable.class);

        int completion = job.waitForCompletion(true) ? 0 : 1;

        // Print results from counters
        printCounterResults(job);

        return completion;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AverageTemperatureDriver(), args);
        System.exit(exitCode);
    }

    /**
     * Idea from https://subscription.packtpub.com/book/big-data-and-business-intelligence/9781783285471/4/ch04lvl1sec55/hadoop-counters-to-report-custom-metrics
     */
    private static void printCounterResults(Job job) throws IOException {
        Counters counters = job.getCounters();

        // Missing and malformed values
        Counter missingValue = counters.findCounter(MissingMalformedCounter.MISSING_VALUE);
        System.out.println(missingValue.getDisplayName() + " = " + missingValue.getValue());

        Counter malformedValue = counters.findCounter(MissingMalformedCounter.MALFORMED_VALUE);
        System.out.println(malformedValue.getDisplayName() + " = " + malformedValue.getValue());

        // Quality indices
        Counter q0 = counters.findCounter(QualityCounter.QUALITY_0);
        System.out.println(q0.getDisplayName() + " = " + q0.getValue());

        Counter q1 = counters.findCounter(QualityCounter.QUALITY_1);
        System.out.println(q1.getDisplayName() + " = " + q1.getValue());

        Counter q2 = counters.findCounter(QualityCounter.QUALITY_2);
        System.out.println(q2.getDisplayName() + " = " + q2.getValue());

        Counter q3 = counters.findCounter(QualityCounter.QUALITY_3);
        System.out.println(q3.getDisplayName() + " = " + q3.getValue());

        Counter q4 = counters.findCounter(QualityCounter.QUALITY_4);
        System.out.println(q4.getDisplayName() + " = " + q4.getValue());

        Counter q5 = counters.findCounter(QualityCounter.QUALITY_5);
        System.out.println(q5.getDisplayName() + " = " + q5.getValue());

        Counter q6 = counters.findCounter(QualityCounter.QUALITY_6);
        System.out.println(q6.getDisplayName() + " = " + q6.getValue());

        Counter q7 = counters.findCounter(QualityCounter.QUALITY_7);
        System.out.println(q7.getDisplayName() + " = " + q7.getValue());

        Counter q8 = counters.findCounter(QualityCounter.QUALITY_8);
        System.out.println(q8.getDisplayName() + " = " + q8.getValue());

        Counter q9 = counters.findCounter(QualityCounter.QUALITY_9);
        System.out.println(q9.getDisplayName() + " = " + q9.getValue());

        Counter qA = counters.findCounter(QualityCounter.QUALITY_A);
        System.out.println(qA.getDisplayName() + " = " + qA.getValue());

        Counter qC = counters.findCounter(QualityCounter.QUALITY_C);
        System.out.println(qC.getDisplayName() + " = " + qC.getValue());

        Counter qI = counters.findCounter(QualityCounter.QUALITY_I);
        System.out.println(qI.getDisplayName() + " = " + qI.getValue());

        Counter qM = counters.findCounter(QualityCounter.QUALITY_M);
        System.out.println(qM.getDisplayName() + " = " + qM.getValue());

        Counter qP = counters.findCounter(QualityCounter.QUALITY_P);
        System.out.println(qP.getDisplayName() + " = " + qP.getValue());

        Counter qR = counters.findCounter(QualityCounter.QUALITY_R);
        System.out.println(qR.getDisplayName() + " = " + qR.getValue());

        Counter qU = counters.findCounter(QualityCounter.QUALITY_U);
        System.out.println(qU.getDisplayName() + " = " + qU.getValue());
    }
}
