package template.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import template.mapper.ItemMapper;
import template.table.ItemsTable;
import template.table.PartsTable;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static template.sink.Sink.createMongoSink;

public class ItemsETL {

    public static void main(String[] args) throws Exception {
        var streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setRuntimeMode(BATCH);
        var streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);

        ItemsTable.create(streamTableEnvironment);
        PartsTable.create(streamTableEnvironment);

        var resultTable = streamTableEnvironment.sqlQuery("""
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

        var mongoSink = createMongoSink("items");

        streamTableEnvironment.toDataStream(resultTable)
                .map(new ItemMapper())
                .sinkTo(mongoSink)
                .setParallelism(1)
                .name("ItemsETL output");

        streamExecutionEnvironment.execute("ItemsETL");
    }

}