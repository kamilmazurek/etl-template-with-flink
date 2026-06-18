package template.sink;

import lombok.SneakyThrows;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.ReplaceOneModel;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.ReplaceOptions;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.WriteModel;
import org.apache.flink.mongodb.shaded.org.bson.BsonDocument;
import org.apache.flink.mongodb.shaded.org.bson.Document;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.flink.mongodb.shaded.com.mongodb.client.model.Filters.eq;

public class UpsertMongoSerializationSchema<T> implements MongoSerializationSchema<T> {

    private transient ObjectMapper objectMapper;

    @Override
    @SneakyThrows
    public WriteModel<BsonDocument> serialize(T item, MongoSinkContext context) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        var doc = Document.parse(objectMapper.writeValueAsString(item));

        return new ReplaceOneModel<>(
                eq("_id", doc.get("_id")),
                doc.toBsonDocument(),
                new ReplaceOptions().upsert(true)
        );
    }
}