package template.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SystemStubsExtension.class)
class MongoSinkProviderTest {

    @SystemStub
    private EnvironmentVariables env;

    @Test
    void shouldCreateMongoSink() {
        //given environment variables
        env.set("MONGODB_URI", "mongodb://localhost:27017");
        env.set("MONGODB_DATABASE", "testdb");

        //and provider
        var sinkProvider = new MongoSinkProvider<>();

        //when sink is created
        var sink = sinkProvider.create("test-collection");

        //then sink is returned
        assertNotNull(sink);
    }
}