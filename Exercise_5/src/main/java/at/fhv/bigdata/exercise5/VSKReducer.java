package at.fhv.bigdata.exercise5;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class VSKReducer extends Reducer<IntWritable, CentralMomentData, IntWritable, NullWritable> {
    private final Schema schema = WeatherSchema.INSTANCE.getWeatherSchema();
    private final GenericRecord record = new GenericData.Record(schema);

    @Override
    protected void reduce(IntWritable key, Iterable<CentralMomentData> values, Context context) throws IOException {
        for (CentralMomentData data : values) {
            record.put("year", key.get());
            record.put("variance", data.getVariance());
            record.put("skewness", data.getSkewness());
            record.put("kurtosis", data.getKurtosis());

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter);
            fileWriter.create(schema, new File("/avro-result_" + key.get() + ".avro"));
            fileWriter.append(record);
            fileWriter.close();
        }
    }
}
