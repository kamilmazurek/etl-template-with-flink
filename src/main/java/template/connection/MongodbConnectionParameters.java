package template.connection;

import lombok.Getter;

import java.util.Arrays;

import static java.lang.System.getenv;

public class MongodbConnectionParameters implements AutoCloseable {

    private final char[] uri;

    @Getter
    private final String database;

    public MongodbConnectionParameters() {
        var rawUri = getenv("MONGODB_URI");
        this.uri = (rawUri != null) ? rawUri.toCharArray() : new char[0];
        this.database = getenv("MONGODB_DATABASE");
    }

    public String getUri() {
        return new String(uri);
    }

    @Override
    public void close() {
        Arrays.fill(this.uri, '\0');
    }
}