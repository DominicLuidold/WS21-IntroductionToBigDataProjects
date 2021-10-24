package at.fhv.bigdata.exercise3;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

public enum TestContext {
    INSTANCE;

    private String mapInputFileName() {
        return "/median-temperature/temperature-input.txt";
    }

    private String reduceOutputFileName() {
        return "/median-temperature/temperature-reducer-output.txt";
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

    public List<Pair<IntWritable, IntWritable>> expectedReduceOutput() {
        return getOutput(reduceOutputFileName());
    }

    private List<Pair<IntWritable, IntWritable>> getOutput(String fileName) {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(fileName));
        List<Pair<IntWritable, IntWritable>> output = new ArrayList<>();
        while (scanner.hasNext()) {
            String[] keyValue = scanner.nextLine().split(",");
            String year = keyValue[0];
            String temperature = keyValue[1];
            output.add(new Pair<>(new IntWritable(Integer.parseInt(year)), new IntWritable(Integer.parseInt(temperature))));
        }
        return output;
    }
}
