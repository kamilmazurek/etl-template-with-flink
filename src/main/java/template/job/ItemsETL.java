package template.job;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bson.BsonDocument;
import org.bson.Document;
import template.connection.JdbcConnectionParameters;
import template.connection.MongodbConnectionParameters;
import template.mapper.ItemToDocumentMapper;
import template.mapper.RowToItemMapper;

import static com.mongodb.client.model.Filters.eq;
import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static template.connection.JdbcConnectorOptions.forTable;

public class ItemsETL {

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(BATCH);
        var tableEnv = StreamTableEnvironment.create(env);

        try (var jdbcParams = new JdbcConnectionParameters()) {

            tableEnv.executeSql("""
                    CREATE TABLE items (
                        id STRING,
                        name STRING,
                        description STRING
                    ) WITH (
                        %s
                    )
                    """.formatted(forTable(jdbcParams, "items"))
            );

            tableEnv.executeSql("""
                    CREATE TABLE parts (
                        part_id STRING,
                        item_id STRING,
                        name STRING
                    ) WITH (
                        %s
                    )
                    """.formatted(forTable(jdbcParams, "parts"))
            );
        }

        var resultTable = tableEnv.sqlQuery("""
                    SELECT
                        i.id,
                        i.name,
                        i.description,
                        (
                            SELECT COLLECT(
                                CAST(ROW(p.part_id, p.name) AS ROW<part_id STRING, name STRING>)
                            )
                            FROM parts p
                            WHERE p.item_id = i.id
                        ) AS parts
                    FROM items i
                """);

        var mongoSink = createMongoSink();

        tableEnv.toDataStream(resultTable)
                .map(new RowToItemMapper())
                .map(new ItemToDocumentMapper())
                .sinkTo(mongoSink)
                .setParallelism(1)
                .name("ItemsETL output");

        env.execute("ItemsETL");
    }

    private static MongoSink<Document> createMongoSink() {
        try (var mongodbParams = new MongodbConnectionParameters()) {
            return MongoSink.<Document>builder()
                    .setUri(mongodbParams.getUri())
                    .setDatabase(mongodbParams.getDatabase())
                    .setCollection("documents")
                    .setSerializationSchema((document, context) -> upsertById(document))
                    .build();
        }
    }

    private static ReplaceOneModel<BsonDocument> upsertById(Document doc) {
        return new ReplaceOneModel<>(eq("_id", doc.get("_id")), doc.toBsonDocument(), new ReplaceOptions().upsert(true));
    }

}