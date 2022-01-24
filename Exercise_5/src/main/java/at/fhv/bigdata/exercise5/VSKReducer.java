package at.fhv.bigdata.exercise5;

import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class VSKReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

    @Override
    protected void reduce(
        AvroKey<Integer> key,
        Iterable<AvroValue<GenericRecord>> values,
        Context context
    ) throws IOException, InterruptedException {
        for (AvroValue<GenericRecord> value : values) {
            context.write(new AvroKey<>(newVSKRecord(value.datum())), NullWritable.get());
        }
    }

    private GenericRecord newVSKRecord(GenericRecord value) {
        GenericRecord record = new GenericData.Record(VSKSchema.INSTANCE.vskRecordSchema());
        record.put("year", value.get("year"));
        record.put("variance", value.get("variance"));
        record.put("skewness", value.get("skewness"));
        record.put("kurtosis", value.get("kurtosis"));

        return record;
    }
}
