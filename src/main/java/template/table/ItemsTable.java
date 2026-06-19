package template.table;

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

    public ItemsTable() {
        super("items", ITEMS_TABLE);
    }

}
