package template.table;

import lombok.AllArgsConstructor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import template.connection.JdbcConnectionParameters;

import static template.connection.JdbcConnectorOptions.toWithClause;

@AllArgsConstructor
abstract class Table {

    private final String tableName;

    private final String tableCreateQuery;

    void createTable(StreamTableEnvironment tableEnv) {
        try (var connectionParameters = new JdbcConnectionParameters()) {
            tableEnv.executeSql(withConnectorOptions(connectionParameters));
        }
    }

    private String withConnectorOptions(JdbcConnectionParameters connectionParameters) {
        return tableCreateQuery.formatted(toWithClause(connectionParameters, tableName));
    }

}
