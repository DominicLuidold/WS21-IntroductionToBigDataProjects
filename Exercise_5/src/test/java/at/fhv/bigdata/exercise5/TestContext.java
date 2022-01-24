package at.fhv.bigdata.exercise5;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Source: https://bitbucket.org/Repe/simplemapreduceexample/src/master/src/test/java/at/fhv/hadoop/examples/max_temperature_avro/TestContext.java
 */
public enum TestContext {
    INSTANCE;

    private String[] mapInputFileNames() {
        String[] filenames = {
            "/1901.txt",
        };
        return filenames;
    }

    public List<Pair<LongWritable, Text>> mapInputLines() throws URISyntaxException, IOException {

        List<Pair<LongWritable, Text>> result = new ArrayList<>();

        int lineCounter = 0;
        for (String fileName : mapInputFileNames()) {
            //System.out.println(fileName);

            URL test = TestContext.class.getResource(fileName);
            for (String line : Files.readAllLines(Paths.get(test.toURI()))) {
                result.add(new Pair<>(new LongWritable(lineCounter++), new Text(line)));
            }
        }

        return result;
    }
}
