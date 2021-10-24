package at.fhv.bigdata.exercise3.median;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Code inspired by https://www.javacodegeeks.com/2013/01/mapreduce-algorithms-secondary-sorting.html
 * and https://www.oreilly.com/library/view/data-algorithms/9781491906170/ch01.html
 *
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class MedianTemperatureGroupingComparator extends WritableComparator {

    public MedianTemperatureGroupingComparator() {
        super(YearTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearTemperaturePair pair1 = (YearTemperaturePair) a;
        YearTemperaturePair pair2 = (YearTemperaturePair) b;

        return Integer.compare(pair1.getYear().get(), pair2.getYear().get());
    }
}
