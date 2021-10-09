package at.fhv.bigdata.exercise2.combiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperatureCombiner extends Reducer<Text, AverageTemperaturePair, Text, AverageTemperaturePair> {

    /**
     * Combines the counts of temperatures and writes year + count of temperatures.
     */
    @Override
    protected void reduce(Text key, Iterable<AverageTemperaturePair> values, Context context) throws IOException, InterruptedException {
        int temperature = 0;
        int count = 0;

        for (AverageTemperaturePair value : values) {
            temperature += value.getTemperature().get();
            count += value.getCount().get();
        }

        context.write(key, new AverageTemperaturePair(temperature, count));
    }
}
