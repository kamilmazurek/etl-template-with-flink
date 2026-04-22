package template.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import template.mapper.ItemMapper;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;

public class ItemsETL {

    public static void main(String[] args) throws Exception {
        var dbUrl = System.getenv("DB_URL");
        var dbUser = System.getenv("DB_USER");
        var dbPassword = System.getenv("DB_PASSWORD");

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(BATCH);
        var tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(String.format("""
                    CREATE TABLE items (
                        id STRING,
                        name STRING,
                        description STRING
                    ) WITH (
                        'connector' = 'jdbc',
                        'url' = '%s',
                        'table-name' = 'items',
                        'driver' = 'org.postgresql.Driver',
                        'username' = '%s',
                        'password' = '%s'
                    )
                """, dbUrl, dbUser, dbPassword));

        var resultTable = tableEnv.sqlQuery("SELECT * FROM items");

        tableEnv.toDataStream(resultTable)
                .map(new ItemMapper())
                .print();

        env.execute();
    }

}