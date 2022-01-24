package at.fhv.bigdata.exercise5;


import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VSKDriver extends Configured implements Tool {

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
        Job job = Job.getInstance(conf, "Calculates variance, skewness and kurtosis.");
        job.setJarByClass(getClass());

        // Specify various job-specific parameters

        // Set the input file path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the schemas for avro input and output
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, VSKSchema.INSTANCE.vskRecordSchema());
        AvroJob.setOutputKeySchema(job, VSKSchema.INSTANCE.vskRecordSchema());

        // Set the type of the value in the input
        job.setInputFormatClass(TextInputFormat.class);

        // Set the type of the value in the output
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // Set the mapper, reducer and combiner classes
        job.setMapperClass(VSKMapper.class);
        job.setReducerClass(VSKReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new VSKDriver(), args);
        System.exit(exitCode);
    }
}
