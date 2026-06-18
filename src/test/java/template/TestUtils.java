package template;

import lombok.SneakyThrows;
import org.apache.flink.mongodb.shaded.com.mongodb.client.MongoClients;
import org.apache.flink.mongodb.shaded.org.bson.Document;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> List<T> getMongoCollection(String replicaSetUrl, String dbName, String collectionName, Class<T> clazz) {
        try (var mongoClient = MongoClients.create(replicaSetUrl)) {
            var database = mongoClient.getDatabase(dbName);
            var collection = database.getCollection(collectionName);
            return collection.find().into(new ArrayList<>()).stream()
                    .map(document -> mapTo(clazz, document))
                    .toList();
        }
    }

    @SneakyThrows
    private static <T> T mapTo(Class<T> clazz, Document document) {
        return OBJECT_MAPPER.readValue(document.toJson(), clazz);
    }

}
