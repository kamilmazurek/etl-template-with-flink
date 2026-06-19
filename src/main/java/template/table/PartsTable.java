package template.table;

public final class PartsTable extends Table {

    private static final String PARTS_TABLE = """
            CREATE TABLE parts (
                id STRING,
                item_id STRING,
                name STRING
            ) WITH (
                %s
            )
            """;

    public PartsTable() {
        super("parts", PARTS_TABLE);
    }

}
