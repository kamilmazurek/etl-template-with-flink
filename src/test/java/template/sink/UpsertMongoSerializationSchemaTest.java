package template.sink;

import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.ReplaceOneModel;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.WriteModel;
import org.apache.flink.mongodb.shaded.org.bson.BsonArray;
import org.apache.flink.mongodb.shaded.org.bson.BsonDocument;
import org.apache.flink.mongodb.shaded.org.bson.BsonString;
import org.junit.jupiter.api.Test;
import template.TestData;
import template.model.Item;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class UpsertMongoSerializationSchemaTest {

    @Test
    void shouldSerializeItemToReplaceOneModel() {
        //given serialization schema
        var schema = new UpsertMongoSerializationSchema<Item>();

        //and item
        var item = TestData.createTestItemA();

        //when item is serialized
        var result = schema.serialize(item, mock(MongoSinkContext.class));

        //then ReplaceOneModel is returned
        var replaceModel = (ReplaceOneModel<BsonDocument>) result;

        //and upsert is set to true
        assertTrue(replaceModel.getReplaceOptions().isUpsert());

        //and fields are mapped
        var replacementDocument = replaceModel.getReplacement();
        assertEquals(new BsonString("item-a"), replacementDocument.get("_id"));
        assertEquals(new BsonString("Item A"), replacementDocument.get("name"));
        assertEquals(new BsonString("Test item A"), replacementDocument.get("description"));
        assertInstanceOf(BsonArray.class, replacementDocument.get("parts"));
        assertEquals(3, replacementDocument.getArray("parts").size());
    }

}