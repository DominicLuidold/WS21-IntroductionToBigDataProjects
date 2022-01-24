package at.fhv.bigdata.exercise5;

import java.io.IOException;
import org.apache.avro.Schema;

/**
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public enum VSKSchema {
    INSTANCE;

    private final String vskRecordSchemaFile = "/avro/VSKRecord.avsc";
    private Schema vskRecordSchema;

    VSKSchema() {
        //Create the parser for the schema
        Schema.Parser parser = new Schema.Parser();

        //Create schema from .avsc file
        try {
            vskRecordSchema = parser.parse(getClass().getResourceAsStream(vskRecordSchemaFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Schema vskRecordSchema() {
        return vskRecordSchema;
    }
}
