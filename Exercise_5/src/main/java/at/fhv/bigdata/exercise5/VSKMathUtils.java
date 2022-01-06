package at.fhv.bigdata.exercise5;

import java.util.ArrayList;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public final class VSKMathUtils {

    public static double calcVariance(ArrayList<Double> values) {
        double sum = 0.0;
        double sum2 = 0.0;

        for (int i = 0; i < values.size(); i++) {
            values.set(i, Math.pow(values.get(i), 2));
        }

        for (Double value : values) {
            sum = sum + value;
        }
        sum = sum * (1.0 / values.size());

        for (Double value : values) {
            sum2 = sum2 + value;
        }
        sum2 = sum * (1.0 / values.size());
        sum2 = Math.pow(sum2, 2);

        return sum - sum2;
    }

    public static double calcSkewness(ArrayList<Double> values) {
        double[] convertedValues = ArrayUtils.toPrimitive((Double[]) values.toArray());

        return new Skewness().evaluate(convertedValues, 0, convertedValues.length);
    }

    public static double calcKurtosis(ArrayList<Double> values) {
        double[] convertedValues = ArrayUtils.toPrimitive((Double[]) values.toArray());

        return new Kurtosis().evaluate(convertedValues, 0, convertedValues.length);
    }
}
