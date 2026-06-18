package template.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SystemStubsExtension.class)
class JdbcConnectionParametersTest {

    @SystemStub
    private EnvironmentVariables env;

    @Test
    void shouldCreateConnectionParameters() {
        //given environment variables
        env.set("POSTGRESQL_URL", "jdbc:postgresql://localhost:5432/mydb");
        env.set("POSTGRESQL_USER", "testuser");
        env.set("POSTGRESQL_PASSWORD", "testpassword");

        //when new connection parameters are created
        var parameters = new JdbcConnectionParameters();

        //then parameters should match expected environment values
        assertEquals("jdbc:postgresql://localhost:5432/mydb", parameters.getUrl());
        assertEquals("testuser", parameters.getUsername());
        assertNotNull(parameters.getPassword());
        assertEquals("testpassword", new String(parameters.getPassword()));
    }

    @Test
    void shouldClearPassword() {
        //given password
        var password = "secret";

        //and environment variables
        env.set("POSTGRESQL_PASSWORD", password);
        var parameters = new JdbcConnectionParameters();
        char[] passwordArray = parameters.getPassword();

        //when connection parameters are closed
        parameters.close();

        // then the password array should be cleared
        assertArrayEquals(new char[password.length()], passwordArray);
    }

}