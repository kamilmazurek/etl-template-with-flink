package template.connection;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.System.getenv;

public class JdbcConnectionParameters implements AutoCloseable {

    private final String url;

    private final String username;

    private final char[] password;

    public JdbcConnectionParameters() {
        this.url = getenv("POSTGRESQL_URL");
        this.username = getenv("POSTGRESQL_USER");
        var rawPass = getenv("POSTGRESQL_PASSWORD");
        this.password = (rawPass != null) ? rawPass.toCharArray() : new char[0];
    }

    public String toConnectionWithClause() {
        var map = Map.of(
                "connector", "jdbc",
                "driver", "org.postgresql.Driver",
                "url", this.url != null ? this.url : "",
                "username", this.username != null ? this.username : "",
                "password", new String(password)
        );

        return map.entrySet()
                .stream()
                .map(entry -> "'%s' = '%s'".formatted(escape(entry.getKey()), escape(entry.getValue())))
                .collect(Collectors.joining(",\n"));
    }

    private String escape(String value) {
        return value.replace("'", "''");
    }

    @Override
    public void close() {
        Arrays.fill(this.password, '\0');
    }

}

