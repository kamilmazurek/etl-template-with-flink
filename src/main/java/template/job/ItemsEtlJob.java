package template.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import template.mapper.ItemMapper;
import template.model.Item;
import template.sink.MongoSinkProvider;
import template.table.ItemsTable;
import template.table.PartsTable;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;

public class ItemsEtlJob {

    public static void main(String[] args) throws Exception {
        var streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setRuntimeMode(BATCH);
        var streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);

        new ItemsTable().init(streamTableEnvironment);
        new PartsTable().init(streamTableEnvironment);

        var resultTable = streamTableEnvironment.sqlQuery("""
                SELECT
                    i.id,
                    i.name,
                    i.description,
                    (
                        SELECT COLLECT(
                            CAST(ROW(p.id, p.name) AS ROW<id STRING, name STRING>)
                        )
                        FROM parts p
                        WHERE p.item_id = i.id
                    ) AS parts
                FROM items i
                """);

        streamTableEnvironment.toDataStream(resultTable)
                .map(new ItemMapper())
                .sinkTo(new MongoSinkProvider<Item>().create("items"))
                .name("ItemsETL output");

        streamExecutionEnvironment.execute("ItemsETL");
    }

}