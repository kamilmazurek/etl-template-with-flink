package template.sink;

import org.apache.flink.connector.mongodb.sink.MongoSink;
import template.connection.MongodbConnectionParameters;

public class MongoSinkProvider<T> {

    public MongoSink<T> create(String collectionName) {
        try (var connectionParameters = new MongodbConnectionParameters()) {
            return MongoSink.<T>builder()
                    .setUri(connectionParameters.getUri())
                    .setDatabase(connectionParameters.getDatabase())
                    .setCollection(collectionName)
                    .setSerializationSchema(new UpsertMongoSerializationSchema<>())
                    .build();
        }
    }
}