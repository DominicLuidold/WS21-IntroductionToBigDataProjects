package at.fhv.bigdata.exercise3.median;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class MedianTemperatureMapper extends Mapper<LongWritable, Text, YearTemperaturePair, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        int year = Integer.parseInt(line.substring(15, 19));
        int temperature = Integer.parseInt(line.substring(87, 92)); // 87 includes "+" and "-"
        String qualityIndex = line.substring(92, 93);

        if (temperature != 9999 && qualityIndex.matches("[01459]")) {
            context.write(new YearTemperaturePair(year, temperature),  new IntWritable(temperature));
        }
    }
}
