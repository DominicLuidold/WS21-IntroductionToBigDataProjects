package at.fhv.bigdata.exercise3.median;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianTemperaturePercentileReducer extends Reducer<YearTemperaturePair, IntWritable, IntWritable, DoubleWritable> {

    @Override
    protected void reduce(YearTemperaturePair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        List<IntWritable> temperatureList = new ArrayList<>();
        for (IntWritable value : values) {
            // Without "net IntWritable()" no sorting happens
            temperatureList.add(new IntWritable(value.get()));
        }

        Configuration conf = context.getConfiguration();
        double percentile = conf.getDouble("Percentile", 50.0); // 50.0 = Median if no value provided

        context.write(key.getYear(), new DoubleWritable(percentile(temperatureList, percentile)));
    }

    /**
     * Method copied and adapted from https://stackoverflow.com/a/41413629
     */
    public static double percentile(List<IntWritable> values, double percentile) {
        int index = (int) Math.ceil((percentile / 100) * values.size());

        return values.get(index - 1).get();
    }
}
