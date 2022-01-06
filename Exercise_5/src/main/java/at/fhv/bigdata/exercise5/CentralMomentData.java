package at.fhv.bigdata.exercise5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public final class CentralMomentData implements Writable {
    private double variance;
    private double skewness;
    private double kurtosis;

    public CentralMomentData() {
    }

    public CentralMomentData(double variance, double skewness, double kurtosis) {
        this.variance = variance;
        this.skewness = skewness;
        this.kurtosis = kurtosis;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        variance = dataInput.readDouble();
        skewness = dataInput.readDouble();
        kurtosis = dataInput.readDouble();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(variance);
        dataOutput.writeDouble(skewness);
        dataOutput.writeDouble(kurtosis);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CentralMomentData) {
            CentralMomentData pair = (CentralMomentData) obj;

            return this.variance == pair.variance &&
                this.skewness == pair.skewness &&
                this.kurtosis == pair.kurtosis;
        }

        return false;
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
