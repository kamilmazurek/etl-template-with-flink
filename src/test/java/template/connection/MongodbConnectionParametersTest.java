package template.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SystemStubsExtension.class)
class MongodbConnectionParametersTest {

    @SystemStub
    private EnvironmentVariables env;

    @Test
    void shouldCreateConnectionParameters() {
        //given environment variables
        env.set("MONGODB_URI", "mongodb://localhost:27017");
        env.set("MONGODB_DATABASE", "testdb");

        //when connection parameters are created
        var parameters = new MongodbConnectionParameters();

        //then parameters should match environment values
        assertEquals("mongodb://localhost:27017", parameters.getUri());
        assertEquals("testdb", parameters.getDatabase());
    }

    @Test
    void shouldClearUri() {
        //given uri
        var uri = "mongodb://localhost:27017";

        //and environment variables
        env.set("MONGODB_URI", uri);
        var parameters = new MongodbConnectionParameters();

        //when connection parameters are closed
        parameters.close();

        //then the uri array should be cleared
        var expectedClearedUri = new String(new char[uri.length()]);
        assertEquals(expectedClearedUri, parameters.getUri());
    }

}