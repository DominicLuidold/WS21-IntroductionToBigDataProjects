package at.fhv.bigdata.exercise5;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class VSKMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> {
    private final GenericRecord record = new GenericData.Record(VSKSchema.INSTANCE.vskRecordSchema());
    private final ArrayList<Double> temperatureValues = new ArrayList<>();
    private int yearValue;

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String line = value.toString();

        yearValue = Integer.parseInt(line.substring(15, 19));
        int temperature = Integer.parseInt(line.substring(87, 92)); // 87 includes "+" and "-"
        String qualityIndex = line.substring(92, 93);

        if (temperature != 9999 && qualityIndex.matches("[01459]")) {
            temperatureValues.add((double) temperature);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        record.put("year", yearValue);
        record.put("variance", VSKMathUtils.calcVariance(temperatureValues));
        record.put("skewness", VSKMathUtils.calcSkewness(temperatureValues));
        record.put("kurtosis", VSKMathUtils.calcKurtosis(temperatureValues));

        context.write(new AvroKey<>(yearValue), new AvroValue<>(record));
    }
}
