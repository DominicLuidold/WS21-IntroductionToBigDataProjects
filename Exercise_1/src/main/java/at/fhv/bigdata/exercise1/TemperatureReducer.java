package at.fhv.bigdata.exercise1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemperature = Integer.MIN_VALUE;

        for (IntWritable value : values) {
            int currentTemperature = value.get();

            if (maxTemperature < currentTemperature) {
                maxTemperature = currentTemperature;
            }
        }

        context.write(key, new IntWritable(maxTemperature));
    }
}
