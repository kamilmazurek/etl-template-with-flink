package template.connection;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

import static java.lang.System.getenv;

public class MongodbConnectionParameters implements AutoCloseable {

    private final char[] uri;

    @Getter
    private final String database;

    public MongodbConnectionParameters() {
        this.uri = Optional.ofNullable(getenv("MONGODB_URI")).map(String::toCharArray).orElse(null);
        this.database = getenv("MONGODB_DATABASE");
    }

    public String getUri() {
        return new String(uri);
    }

    @Override
    public void close() {
        if (this.uri == null) {
            return;
        }

        Arrays.fill(this.uri, '\0');
    }

}