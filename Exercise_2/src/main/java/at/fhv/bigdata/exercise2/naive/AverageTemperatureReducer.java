package at.fhv.bigdata.exercise2.naive;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Naive implementation by simply summing up all temperatures and dividing through count of temperatures.
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sumOfAllTemperatures = 0;
        int count = 0;

        for (IntWritable value : values) {
            sumOfAllTemperatures += value.get();
            count++;
        }

        int averageTemperature = sumOfAllTemperatures / count;
        context.write(key, new IntWritable(averageTemperature));
    }
}
