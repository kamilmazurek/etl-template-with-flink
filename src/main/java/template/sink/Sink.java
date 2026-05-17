package template.sink;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import lombok.SneakyThrows;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.bson.BsonDocument;
import org.bson.Document;
import template.connection.MongodbConnectionParameters;
import template.model.Item;

import static com.mongodb.client.model.Filters.eq;

public class Sink {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public static MongoSink<Item> createMongoSink(String collectionName) {
        try (var connectionParameters = new MongodbConnectionParameters()) {
            return MongoSink.<Item>builder()
                    .setUri(connectionParameters.getUri())
                    .setDatabase(connectionParameters.getDatabase())
                    .setCollection(collectionName)
                    .setSerializationSchema((item, context) -> upsertById(toDocument(item)))
                    .build();
        }
    }

    @SneakyThrows
    private static Document toDocument(Item item) {
        return Document.parse(objectMapper.writeValueAsString(item));
    }

    private static ReplaceOneModel<BsonDocument> upsertById(Document doc) {
        return new ReplaceOneModel<>(eq("_id", doc.get("_id")), doc.toBsonDocument(), new ReplaceOptions().upsert(true));
    }

}
