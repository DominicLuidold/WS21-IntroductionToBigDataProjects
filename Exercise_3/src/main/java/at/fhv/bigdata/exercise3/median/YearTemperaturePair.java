package at.fhv.bigdata.exercise3.median;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Stores year and temperature.
 * Code inspired by/copied from "word_co_occurrence".
 *
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class YearTemperaturePair implements WritableComparable<YearTemperaturePair> {
    private IntWritable temperature;
    private IntWritable year;

    public YearTemperaturePair() {
        set(new IntWritable(), new IntWritable());
    }

    public YearTemperaturePair(int year, int temperature) {
        set(new IntWritable(year), new IntWritable(temperature));
    }

    public YearTemperaturePair(IntWritable year, IntWritable temperature) {
        set(year, temperature);
    }

    private void set(IntWritable year, IntWritable temperature) {
        this.year = year;
        this.temperature = temperature;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.temperature.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.temperature.write(dataOutput);
    }

    public IntWritable getYear() {
        return year;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    @Override
    public int compareTo(YearTemperaturePair pair) {
        int compareValue = this.year.compareTo(pair.getYear());
        // Used for sorting keys in an ascending way
        if (compareValue == 0) {
            compareValue = this.temperature.compareTo(pair.getTemperature());
        }

        return compareValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof YearTemperaturePair) {
            YearTemperaturePair pair = (YearTemperaturePair) obj;
            return this.year.get() == pair.year.get() &&
                this.temperature.get() == pair.temperature.get();
        }

        return false;
    }

    @Override
    public int hashCode() {
        return year.get() + temperature.get();
    }

    @Override
    public String toString() {
        return "YearTemperaturePair{" +
            "temperature=" + temperature +
            ", year=" + year +
            '}';
    }
}
