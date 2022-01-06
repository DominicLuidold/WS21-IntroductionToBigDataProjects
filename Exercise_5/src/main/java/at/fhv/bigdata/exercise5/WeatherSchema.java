package at.fhv.bigdata.exercise5;

import java.io.IOException;
import org.apache.avro.Schema;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public enum WeatherSchema {
    INSTANCE;

    private static final String AVRO_SCHEMA_FILE = "/avro/VSK.avsc";
    private Schema weatherSchema;

    WeatherSchema() {
        Schema.Parser parser = new Schema.Parser();

        try {
            weatherSchema = parser.parse(getClass().getResourceAsStream(AVRO_SCHEMA_FILE));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Schema getWeatherSchema() {
        return weatherSchema;
    }
}
