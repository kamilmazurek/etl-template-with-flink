package template.table;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PartsTable extends Table {

    private static final String PARTS_TABLE = """
            CREATE TABLE parts (
                id STRING,
                item_id STRING,
                name STRING
            ) WITH (
                %s
            )
            """;

    private PartsTable() {
        super("parts", PARTS_TABLE);
    }

    public static void init(StreamTableEnvironment tableEnv) {
        new PartsTable().createTable(tableEnv);
    }

}
