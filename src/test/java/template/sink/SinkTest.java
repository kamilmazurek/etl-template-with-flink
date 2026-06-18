package template.sink;

import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SystemStubsExtension.class)
class SinkTest {

    @SystemStub
    private EnvironmentVariables env;

    @Test
    void shouldCreateMongoSink() {
        //given environment variables
        env.set("MONGODB_URI", "mongodb://localhost:27017");
        env.set("MONGODB_DATABASE", "testdb");

        //and collection name
        var collectionName = "test-collection";

        //when sink is created
        MongoSink<Object> sink = Sink.createMongoSink(collectionName);

        //then the sink object should be returned
        assertNotNull(sink);
    }
}