package at.fhv.bigdata.exercise2.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores a temperature and how often said temperature has occurred.
 * Code inspired by/copied from "word_co_occurrence".
 *
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperaturePair implements WritableComparable<AverageTemperaturePair> {
    private IntWritable temperature;
    private IntWritable count;

    public AverageTemperaturePair() {
        set(new IntWritable(), new IntWritable());
    }

    public AverageTemperaturePair(int temperature, int count) {
        set(new IntWritable(temperature), new IntWritable(count));
    }

    public AverageTemperaturePair(IntWritable temperature, IntWritable count) {
        set(temperature, count);
    }

    private void set(IntWritable temperature, IntWritable count) {
        this.temperature = temperature;
        this.count = count;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.temperature.readFields(dataInput);
        this.count.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.temperature.write(dataOutput);
        this.count.write(dataOutput);
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    public int compareTo(AverageTemperaturePair o) {
        AverageTemperaturePair other = (AverageTemperaturePair) o;

        int cmp = temperature.compareTo(other.temperature);
        if (cmp != 0) {
            return cmp;
        }
        return count.compareTo(other.count);
    }

    @Override
    public String toString() {
        return "AverageTemperaturePair{" +
                "temperature=" + temperature +
                ", count=" + count +
                '}';
    }
}
