package template.sink;


import lombok.SneakyThrows;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.ReplaceOneModel;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.ReplaceOptions;
import org.apache.flink.mongodb.shaded.org.bson.BsonDocument;
import org.apache.flink.mongodb.shaded.org.bson.Document;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import template.connection.MongodbConnectionParameters;

import static org.apache.flink.mongodb.shaded.com.mongodb.client.model.Filters.eq;


public class Sink {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> MongoSink<T> createMongoSink(String collectionName) {
        try (var connectionParameters = new MongodbConnectionParameters()) {
            return MongoSink.<T>builder()
                    .setUri(connectionParameters.getUri())
                    .setDatabase(connectionParameters.getDatabase())
                    .setCollection(collectionName)
                    .setSerializationSchema((item, context) -> upsertById(toDocument(item)))
                    .build();
        }
    }

    @SneakyThrows
    private static Document toDocument(Object object) {
        return Document.parse(objectMapper.writeValueAsString(object));
    }

    private static ReplaceOneModel<BsonDocument> upsertById(Document doc) {
        return new ReplaceOneModel<>(eq("_id", doc.get("_id")), doc.toBsonDocument(), new ReplaceOptions().upsert(true));
    }

}
