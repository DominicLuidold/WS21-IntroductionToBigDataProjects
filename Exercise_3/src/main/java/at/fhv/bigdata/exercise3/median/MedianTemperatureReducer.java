package at.fhv.bigdata.exercise3.median;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianTemperatureReducer extends Reducer<YearTemperaturePair, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(YearTemperaturePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        List<IntWritable> temperatureList = new ArrayList<>();
        for (IntWritable value : values) {
            // Without "net IntWritable()" no sorting happens
            temperatureList.add(new IntWritable(value.get()));
        }

        int count = temperatureList.size();
        if (count % 2 == 0) {
            context.write(
                key.getYear(),
                new IntWritable((temperatureList.get(count / 2).get() + temperatureList.get((count / 2) - 1).get()) / 2)
            );
        } else {
            context.write(key.getYear(), temperatureList.get(count / 2));
        }
    }
}
