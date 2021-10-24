package at.fhv.bigdata.exercise3;

import at.fhv.bigdata.exercise3.median.MedianTemperatureGroupingComparator;
import at.fhv.bigdata.exercise3.median.MedianTemperatureMapper;
import at.fhv.bigdata.exercise3.median.MedianTemperatureReducer;
import at.fhv.bigdata.exercise3.median.YearTemperaturePair;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MedianTemperatureTest {
    private TestContext tc = TestContext.INSTANCE;

    private Mapper mapper;
    private MapDriver<LongWritable, Text, YearTemperaturePair, IntWritable> mapDriver;

    private Reducer reducer;
    private ReduceDriver<YearTemperaturePair, IntWritable, IntWritable, IntWritable> reduceDriver;

    private MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, IntWritable, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        mapper = new MedianTemperatureMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        reducer = new MedianTemperatureReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setKeyGroupingComparator(new MedianTemperatureGroupingComparator());
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.addAll(tc.mapInput());
        mapReduceDriver.addAllOutput(tc.expectedReduceOutput());
        mapReduceDriver.runTest();
    }
}
