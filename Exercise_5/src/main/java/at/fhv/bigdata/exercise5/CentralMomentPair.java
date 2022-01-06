package at.fhv.bigdata.exercise5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public final class CentralMomentPair implements Writable {
    private String year;
    private double variance;
    private double skewness;
    private double kurtosis;

    public CentralMomentPair(String year, double variance, double skewness, double kurtosis) {
        this.year = year;
        this.variance = variance;
        this.skewness = skewness;
        this.kurtosis = kurtosis;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readLine();
        variance = dataInput.readDouble();
        skewness = dataInput.readDouble();
        kurtosis = dataInput.readDouble();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeDouble(variance);
        dataOutput.writeDouble(skewness);
        dataOutput.writeDouble(kurtosis);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CentralMomentPair) {
            CentralMomentPair pair = (CentralMomentPair) obj;

            return this.year.equals(pair.year) &&
                this.variance == pair.variance &&
                this.skewness == pair.skewness &&
                this.kurtosis == pair.kurtosis;
        }

        return false;
    }

    public String getYear() {
        return year;
    }

    public double getVariance() {
        return variance;
    }

    public double getSkewness() {
        return skewness;
    }

    public double getKurtosis() {
        return kurtosis;
    }
}
