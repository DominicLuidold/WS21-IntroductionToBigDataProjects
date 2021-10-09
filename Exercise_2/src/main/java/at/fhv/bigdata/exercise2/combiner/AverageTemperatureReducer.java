package at.fhv.bigdata.exercise2.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperatureReducer extends Reducer<Text, AverageTemperaturePair, Text, IntWritable> {

    /**
     * Reducer is nearly identical to "naive mapper" but receives {@link AverageTemperaturePair} instead of single
     * temperature values and therefore doesn't have to iterate through every single temperature.
     */
    @Override
    protected void reduce(Text key, Iterable<AverageTemperaturePair> values, Context context) throws IOException, InterruptedException {
        int sumOfAllTemperatures = 0;
        int count = 0;

        for (AverageTemperaturePair value : values) {
            sumOfAllTemperatures += value.getTemperature().get();
            count += value.getCount().get();
        }

        int averageTemperature = sumOfAllTemperatures / count;
        context.write(key, new IntWritable(averageTemperature));
    }
}