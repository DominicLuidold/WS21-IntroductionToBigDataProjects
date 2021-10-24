package at.fhv.bigdata.exercise3.median;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Code inspired by https://www.javacodegeeks.com/2013/01/mapreduce-algorithms-secondary-sorting.html
 * and https://www.oreilly.com/library/view/data-algorithms/9781491906170/ch01.html
 *
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class MedianTemperaturePartitioner extends Partitioner<YearTemperaturePair, IntWritable> {

    @Override
    public int getPartition(
        YearTemperaturePair pair,
        IntWritable temperature,
        int numberOfPartitions
    ) {
        // Make sure that partitions are non-negative
        return Math.abs(pair.getYear().hashCode() % numberOfPartitions);
    }
}
