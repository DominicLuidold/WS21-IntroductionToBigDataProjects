package at.fhv.bigdata.exercise2.naive;

import at.fhv.bigdata.exercise2.naive.counter.MissingMalformedCounter;
import at.fhv.bigdata.exercise2.naive.counter.QualityCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String year = line.substring(15, 19);
        int temperature = Integer.parseInt(line.substring(87, 92)); // 87 includes "+" and "-"
        String qualityIndex = line.substring(92, 93);

        if (temperature != 9999 && qualityIndex.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(temperature));
        }

        // Call counter methods to increase counters, if necessary
        countMissingMalformedValues(temperature, qualityIndex, context);
        countQualityIndex(qualityIndex, context);
    }

    /**
     * Counter of missing values is increased if temperature equals "9999".
     * Counter of malformed values is getting increased if quality index isn't either {0,1,4,5,9}.
     */
    private static void countMissingMalformedValues(int temperature, String qualityIndex, Context context) {
        if (temperature == 9999) {
            context.getCounter(MissingMalformedCounter.MISSING_VALUE).increment(1);
        }
        if (!qualityIndex.matches("[01459]")) {
            context.getCounter(MissingMalformedCounter.MALFORMED_VALUE).increment(1);
        }
    }

    /**
     * Counter of quality index is increased if quality index matches a case defined below.
     * <p>
     * Cases are defined in Exercise_1 and documented in the {@link QualityCounter} class.
     */
    private static void countQualityIndex(String qualityIndex, Context context) {
        switch (qualityIndex) {
            case "0":
                context.getCounter(QualityCounter.QUALITY_0).increment(1);
                break;
            case "1":
                context.getCounter(QualityCounter.QUALITY_1).increment(1);
                break;
            case "2":
                context.getCounter(QualityCounter.QUALITY_2).increment(1);
                break;
            case "3":
                context.getCounter(QualityCounter.QUALITY_3).increment(1);
                break;
            case "4":
                context.getCounter(QualityCounter.QUALITY_4).increment(1);
                break;
            case "5":
                context.getCounter(QualityCounter.QUALITY_5).increment(1);
                break;
            case "6":
                context.getCounter(QualityCounter.QUALITY_6).increment(1);
                break;
            case "7":
                context.getCounter(QualityCounter.QUALITY_7).increment(1);
                break;
            case "8":
                context.getCounter(QualityCounter.QUALITY_8).increment(1);
                break;
            case "9":
                context.getCounter(QualityCounter.QUALITY_9).increment(1);
                break;
            case "A":
                context.getCounter(QualityCounter.QUALITY_A).increment(1);
                break;
            case "C":
                context.getCounter(QualityCounter.QUALITY_C).increment(1);
                break;
            case "I":
                context.getCounter(QualityCounter.QUALITY_I).increment(1);
                break;
            case "M":
                context.getCounter(QualityCounter.QUALITY_M).increment(1);
                break;
            case "P":
                context.getCounter(QualityCounter.QUALITY_P).increment(1);
                break;
            case "R":
                context.getCounter(QualityCounter.QUALITY_R).increment(1);
                break;
            case "U":
                context.getCounter(QualityCounter.QUALITY_U).increment(1);
                break;
        }
    }
}
