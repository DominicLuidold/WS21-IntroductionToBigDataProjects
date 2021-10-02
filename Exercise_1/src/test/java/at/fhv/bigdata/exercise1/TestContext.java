package at.fhv.bigdata.exercise1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Code copied from WordCount example from project used during lecture.
 *
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public enum TestContext {
    INSTANCE;

    private String mapInputFileName() {
        return "/max-temperature/temperature-input.txt";
    }

    private String reduceInputFileName() {
        return "/max-temperature/temperature-reducer-input.txt";
    }

    private String mapOutputFileName() {
        return "/max-temperature/temperature-mapper-output.txt";
    }

    private String reduceOutputFileName() {
        return "/max-temperature/temperature-reducer-output.txt";
    }

    private List<Pair<Text, IntWritable>> getOutput(String fileName) {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(fileName));
        List<Pair<Text, IntWritable>> output = new ArrayList<>();
        while (scanner.hasNext()) {
            String[] keyValue = scanner.nextLine().split(",");
            String word = keyValue[0];
            String count = keyValue[1];
            output.add(new Pair<>(new Text(word), new IntWritable(Integer.parseInt(count))));
        }
        return output;
    }

    public List<Pair<LongWritable, Text>> mapInput() {

        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(mapInputFileName()));
        List<Pair<LongWritable, Text>> input = new ArrayList<>();

        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            input.add(new Pair<>(new LongWritable(0), new Text(line)));
        }
        return input;
    }

    public List<Pair<Text, IntWritable>> expectedMapOutput() {
        return getOutput(mapOutputFileName());
    }

    public List<Pair<Text, List<IntWritable>>> reduceInput() {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(reduceInputFileName()));

        List<Pair<Text, List<IntWritable>>> result = new ArrayList<>();

        while (scanner.hasNext()) {
            String[] keyValue = scanner.nextLine().split(",");

            String word = keyValue[0];

            List<IntWritable> values = new ArrayList<>();
            for (int i = 1; i < keyValue.length; i++) {
                String count = keyValue[i];
                values.add(new IntWritable(Integer.parseInt(count)));
            }

            result.add(new Pair<>(new Text(word), values));
        }
        return result;
    }

    public List<Pair<Text, IntWritable>> expectedReduceOutput() {
        return getOutput(reduceOutputFileName());
    }
}
