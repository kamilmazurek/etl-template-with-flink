package template.connection;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

import static java.lang.System.getenv;

@Getter
public class JdbcConnectionParameters implements AutoCloseable {

    private final String url;

    private final String username;

    private final char[] password;

    public JdbcConnectionParameters() {
        this.url = getenv("POSTGRESQL_URL");
        this.username = getenv("POSTGRESQL_USER");
        this.password = Optional.ofNullable(getenv("POSTGRESQL_PASSWORD")).map(String::toCharArray).orElse(null);
    }

    @Override
    public void close() {
        if (this.password == null) {
            return;
        }

        Arrays.fill(this.password, '\0');
    }

}

