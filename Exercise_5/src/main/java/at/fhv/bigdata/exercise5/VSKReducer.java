package at.fhv.bigdata.exercise5;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class VSKReducer extends Reducer<AvroKey<String>, CentralMomentPair, AvroKey<GenericRecord>, NullWritable> {
    private final Schema schema = WeatherSchema.INSTANCE.getWeatherSchema();
    private final GenericRecord record = new GenericData.Record(schema);

    @Override
    protected void reduce(AvroKey<String> key, Iterable<CentralMomentPair> values, Context context) throws IOException {
        for (CentralMomentPair pair : values) {
            record.put("year", pair.getYear());
            record.put("variance", pair.getVariance());
            record.put("skewness", pair.getSkewness());
            record.put("kurtosis", pair.getKurtosis());

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter);
            fileWriter.create(schema, new File("/avro-result_" + key + ".avro"));
            fileWriter.append(record);
            fileWriter.close();
        }
    }
}
