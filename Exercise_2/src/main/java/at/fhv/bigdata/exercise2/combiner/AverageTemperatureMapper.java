package at.fhv.bigdata.exercise2.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, AverageTemperaturePair> {

    /**
     * Mapper is nearly identical to "naive mapper" but writes {@link AverageTemperaturePair} instead of single
     * temperature values.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        Text year = new Text(line.substring(15, 19));
        int temperature = Integer.parseInt(line.substring(87, 92)); // 87 includes "+" and "-"
        String qualityIndex = line.substring(92, 93);

        if (temperature != 9999 && qualityIndex.matches("[01459]")) {
            // Use AverageTemperaturePair instead of directly writing year & temperature
            // AverageTemperaturePair will be used in Combiner
            context.write(new Text(year), new AverageTemperaturePair(temperature, 1));
        }
    }
}
