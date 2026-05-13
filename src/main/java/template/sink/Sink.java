package template.sink;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.bson.BsonDocument;
import org.bson.Document;
import template.connection.MongodbConnectionParameters;

import static com.mongodb.client.model.Filters.eq;

public class Sink {

    public static MongoSink<Document> createMongoSink(String collectionName) {
        try (var connectionParameters = new MongodbConnectionParameters()) {
            return MongoSink.<Document>builder()
                    .setUri(connectionParameters.getUri())
                    .setDatabase(connectionParameters.getDatabase())
                    .setCollection(collectionName)
                    .setSerializationSchema((document, context) -> upsertById(document))
                    .build();
        }
    }

    private static ReplaceOneModel<BsonDocument> upsertById(Document doc) {
        return new ReplaceOneModel<>(eq("_id", doc.get("_id")), doc.toBsonDocument(), new ReplaceOptions().upsert(true));
    }

}
