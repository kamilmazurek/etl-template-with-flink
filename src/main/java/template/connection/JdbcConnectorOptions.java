package template.connection;

import lombok.NoArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class JdbcConnectorOptions {

    private static final String JDBC_CONNECTOR_OPTIONS = """
            'connector' = 'jdbc',
            'driver' = 'org.postgresql.Driver',
            'url' = '%s',
            'username' = '%s',
            'password' = '%s',
            'table-name' = '%s'
            """;

    public static String toWithClause(JdbcConnectionParameters params, String tableName) {
        return JDBC_CONNECTOR_OPTIONS.formatted(
                escape(params.getUrl()),
                escape(params.getUsername()),
                escape(new String(params.getPassword())),
                escape(tableName)
        );
    }

    private static String escape(String value) {
        return value.replace("'", "''");
    }

}
