package at.fhv.bigdata.exercise5;

import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Disclaimer: The test just doesn't work - we have no idea why..
 *
 * Source: https://bitbucket.org/Repe/simplemapreduceexample/src/master/src/test/java/at/fhv/hadoop/examples/max_temperature_avro/MaxTemperatureAvroTest.java
 */
public class VSKTest {
    private Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> mapper;
    private MapDriver<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> mapDriver;

    private Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> reducer;
    private MapReduceDriver<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> mapReduceDriver;

    private MapDriver<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> configureMapDriver(
        Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> mapper
    ) {
        MapDriver<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> mapDriver = MapDriver.newMapDriver(mapper);

        Configuration configuration = mapDriver.getConfiguration();

        // Add AvroSerialization to the configuration
        // (copy over the default serializations for deserializing the mapper inputs)
        String[] serializations = configuration.getStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY);
        String[] newSerializations = Arrays.copyOf(serializations, serializations.length + 1);
        newSerializations[serializations.length] = AvroSerialization.class.getName();
        configuration.setStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY, newSerializations);

        // Configure AvroSerialization by specifying the key writer and value writer schemas
        AvroSerialization.setKeyWriterSchema(configuration, Schema.create(Schema.Type.INT));
        AvroSerialization.setValueWriterSchema(configuration, VSKSchema.INSTANCE.vskRecordSchema());

        return mapDriver;
    }

    private MapReduceDriver<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> configureMapReduceDriver(
        Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> mapper,
        Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> reducer
    ) {
        MapReduceDriver<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> mapReduceDriver
            = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        Configuration configuration = mapReduceDriver.getConfiguration();

        // Add AvroSerialization to the configuration
        // (copy over the default serializations for deserializing the mapper inputs)
        String[] serializations = configuration.getStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY);
        String[] newSerializations = Arrays.copyOf(serializations, serializations.length + 1);
        newSerializations[serializations.length] = AvroSerialization.class.getName();
        configuration.setStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY, newSerializations);

        // Configure AvroSerialization by specifying the key writer and value writer schemas
        AvroSerialization.setKeyWriterSchema(configuration, Schema.create(Schema.Type.INT));
        AvroSerialization.setValueWriterSchema(configuration, VSKSchema.INSTANCE.vskRecordSchema());

        return mapReduceDriver;
    }

    @Before
    public void setUp() {
        mapper = new VSKMapper();
        mapDriver = configureMapDriver(mapper);

        reducer = new VSKReducer();
        mapReduceDriver = configureMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMap() throws Exception {
        mapDriver.withAll(TestContext.INSTANCE.mapInputLines());
        mapDriver.run();
    }

    @Test
    public void testMapReduce() throws Exception {
        // Add expected input
        mapReduceDriver.withAll(TestContext.INSTANCE.mapInputLines());
        mapReduceDriver.run();
    }
}
