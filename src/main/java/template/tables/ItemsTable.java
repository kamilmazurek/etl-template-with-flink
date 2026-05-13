package template.tables;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class ItemsTable extends Table {

    private static final String ITEMS_TABLE = """
            CREATE TABLE items (
                id STRING,
                name STRING,
                description STRING
            ) WITH (
                %s
            )
            """;

    private ItemsTable() {
        super("items", ITEMS_TABLE);
    }

    public static void create(StreamTableEnvironment tableEnv) {
        new ItemsTable().createTable(tableEnv);
    }

}
